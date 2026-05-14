import { createApp } from "./app";
import { env } from "./config/env";
import { pingDB } from "./db/db";
import { ensureAppTables } from "./db/app.repo";
import { ensureIndicatorTables } from "./db/indicator-historic.repo";
import { ensureIndicatorRecordTables } from "./db/indicator.repo";
import { ensureIndicatorGroupTables } from "./db/indicator-group.repo";
import { ensureDamodaranTables } from "./db/damodaran.repo";
import "./indicators/worker";
import { logger } from "./utils/logger";
import { scheduleIndicatorJobs } from "./indicators/scheduler";
import { runSeed } from "./scripts/seed-indicators";
import { INDICATOR_JOBS, runIndicatorJob } from "./indicators/jobs";
import { indicatorQueue } from "./queue/queue";

async function triggerAllJobsNow() {
  logger.info("[startup] Disparando todos los jobs de indicadores al arrancar...");
  for (const job of INDICATOR_JOBS) {
    await indicatorQueue.add(job.key, { jobKey: job.key }, {
      removeOnComplete: { count: 50 },
      removeOnFail: { count: 50 },
    });
  }
  logger.info(`[startup] ${INDICATOR_JOBS.length} jobs encolados`);
}

async function main() {
  const ok = await pingDB();
  if (!ok) throw new Error("No conecta a PostgreSQL");

  await ensureIndicatorTables();
  await ensureIndicatorRecordTables();
  await ensureIndicatorGroupTables();
  await ensureAppTables();
  await ensureDamodaranTables();
  await runSeed();
  await scheduleIndicatorJobs();
  await triggerAllJobsNow();

  const app = await createApp();
  app.listen(env.port, () => {
    logger.info(`API-INDICATORS http://localhost:${env.port}`);
    logger.info(
      `Dashboard ${env.dashPath} -> http://localhost:${env.port}${env.dashPath}`
    );
  });
}

main().catch((e) => {
  logger.error(e);
  process.exit(1);
});
