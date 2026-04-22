/**
 * Carga el historial completo disponible en cada fuente de datos.
 *
 * Uso:
 *   npm run backfill              → historial desde 2019
 *   npm run backfill -- 2016      → historial desde 2016
 */
import { pool } from "../db/db";
import { INDICATOR_JOBS, runIndicatorJob } from "../indicators/jobs";
import { fetchUsuraAndIbcHistory, fetchIppDaneHistory } from "../indicators/fetchers";
import { logger } from "../utils/logger";

const fromYear = Number(process.argv[2]) || 2019;

async function backfill() {
  logger.info(`[backfill] Iniciando carga de historial (IPP desde ${fromYear})...`);
  const results = [];
  const errors: { key: string; error: string }[] = [];

  // ── 1. Todos los jobs regulares ──────────────────────────────────────────
  for (const job of INDICATOR_JOBS) {
    try {
      logger.info(`[backfill] → ${job.key}`);
      const r = await runIndicatorJob(job.key);
      logger.info(`[backfill]   ${job.key}: ${r.stored} registros almacenados`);
      results.push(r);
    } catch (e: any) {
      logger.error(`[backfill]   ${job.key} ERROR: ${e.message}`);
      errors.push({ key: job.key, error: e.message });
    }
  }

  // ── 2. Historial completo Superfinanciera (USURA / IBC) ──────────────────
  try {
    logger.info("[backfill] → USURA_IBC_HISTORY (Excel acumulado Superfinanciera)");
    const r = await runIndicatorJob("USURA_IBC", fetchUsuraAndIbcHistory);
    logger.info(`[backfill]   USURA_IBC_HISTORY: ${r.stored} registros almacenados`);
    results.push(r);
  } catch (e: any) {
    logger.error(`[backfill]   USURA_IBC_HISTORY ERROR: ${e.message}`);
    errors.push({ key: "USURA_IBC_HISTORY", error: e.message });
  }

  // ── 3. Historial IPP-DANE mes a mes ──────────────────────────────────────
  try {
    logger.info(`[backfill] → IPP_HISTORY (DANE desde ${fromYear}/01)`);
    const r = await runIndicatorJob("IPP", () => fetchIppDaneHistory(fromYear, 1));
    logger.info(`[backfill]   IPP_HISTORY: ${r.stored} registros almacenados`);
    results.push(r);
  } catch (e: any) {
    logger.error(`[backfill]   IPP_HISTORY ERROR: ${e.message}`);
    errors.push({ key: "IPP_HISTORY", error: e.message });
  }

  // ── Resumen ──────────────────────────────────────────────────────────────
  const totalStored = results.reduce((s, r) => s + r.stored, 0);
  logger.info(`[backfill] Completado: ${totalStored} registros totales, ${errors.length} errores`);
  if (errors.length) {
    logger.warn("[backfill] Errores:");
    for (const e of errors) logger.warn(`  - ${e.key}: ${e.error}`);
  }
}

backfill()
  .catch((e) => { logger.error(e); process.exit(1); })
  .finally(() => pool.end());
