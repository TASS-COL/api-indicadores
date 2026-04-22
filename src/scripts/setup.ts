/**
 * Script de inicialización completa (ejecutar UNA SOLA VEZ por ambiente).
 *
 * Pasos:
 *   1. Verifica conexión a PostgreSQL
 *   2. Crea todas las tablas
 *   3. Carga el catálogo de indicadores (seed)
 *   4. Carga el historial completo de todos los indicadores
 *
 * Uso:
 *   npm run setup
 */
import { pool, pingDB } from "../db/db";
import { ensureIndicatorTables } from "../db/indicator-historic.repo";
import { ensureIndicatorRecordTables, ensureIndicatorUniqueIndex } from "../db/indicator.repo";
import { ensureIndicatorGroupTables } from "../db/indicator-group.repo";
import { ensureAppTables } from "../db/app.repo";
import { ensureDamodaranTables } from "../db/damodaran.repo";
import { DAMODARAN_DATASETS, runDamodaranJob } from "../indicators/damodaran";
import { runSeed } from "./seed-indicators";
import { runLoadHistory } from "./load-indicators-history";
import { logger } from "../utils/logger";

async function setup() {
  // ── 1. Conexión ─────────────────────────────────────────────────────────────
  logger.info("[setup] Verificando conexión a PostgreSQL...");
  const ok = await pingDB();
  if (!ok) throw new Error("No se pudo conectar a PostgreSQL");
  logger.info("[setup] Conexión OK");

  // ── 2. Tablas ────────────────────────────────────────────────────────────────
  logger.info("[setup] Creando tablas...");
  await ensureIndicatorTables();
  await ensureIndicatorRecordTables();
  await ensureIndicatorGroupTables();
  await ensureAppTables();
  await ensureIndicatorUniqueIndex();
  await ensureDamodaranTables();
  logger.info("[setup] Tablas OK");

  // ── 3. Seed ──────────────────────────────────────────────────────────────────
  logger.info("[setup] Cargando catálogo de indicadores...");
  await runSeed();
  logger.info("[setup] Seed OK");

  // ── 4. Historial ─────────────────────────────────────────────────────────────
  logger.info("[setup] Cargando historial de indicadores...");
  await runLoadHistory();
  logger.info("[setup] Historial OK");

  // ── 5. Damodaran ─────────────────────────────────────────────────────────────
  logger.info("[setup] Cargando datasets Damodaran...");
  const damodaranErrors: string[] = [];
  for (const ds of DAMODARAN_DATASETS) {
    try {
      logger.info(`[setup]   → ${ds.dataset}`);
      const r = await runDamodaranJob(ds.dataset);
      logger.info(`[setup]   ${ds.dataset}: ${r.stored} industrias (${r.updateDate})`);
    } catch (e: any) {
      logger.error(`[setup]   ${ds.dataset} ERROR: ${e.message}`);
      damodaranErrors.push(`${ds.dataset}: ${e.message}`);
    }
  }
  if (damodaranErrors.length) {
    logger.warn(`[setup] Damodaran con errores: ${damodaranErrors.join(", ")}`);
  } else {
    logger.info("[setup] Damodaran OK");
  }

  logger.info("[setup] ✔ Inicialización completa");
}

setup()
  .catch((e) => {
    logger.error("[setup] Error fatal:");
    logger.error(e);
    process.exitCode = 1;
  })
  .finally(() => pool.end());
