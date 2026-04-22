import { ensureAppTables } from "../db/app.repo";
import { ensureDamodaranTables } from "../db/damodaran.repo";
import { ensureIndicatorTables } from "../db/indicator-historic.repo";
import { ensureIndicatorRecordTables } from "../db/indicator.repo";
import { ensureIndicatorGroupTables } from "../db/indicator-group.repo";
import { pingDB } from "../db/db";
import { logger } from "../utils/logger";

async function main() {
  const ok = await pingDB();
  if (!ok) throw new Error("No conecta a PostgreSQL");

  logger.info("Creando tablas de indicadores (catálogo + histórico)...");
  await ensureIndicatorTables();

  logger.info("Creando tablas de indicator_record...");
  await ensureIndicatorRecordTables();

  logger.info("Creando tablas de grupos de indicadores...");
  await ensureIndicatorGroupTables();

  logger.info("Creando tablas de aplicaciones (auth)...");
  await ensureAppTables();

  logger.info("Creando tablas Damodaran...");
  await ensureDamodaranTables();

  logger.info("Todas las tablas han sido creadas exitosamente.");
  process.exit(0);
}

main().catch((e) => {
  logger.error(e);
  process.exit(1);
});
