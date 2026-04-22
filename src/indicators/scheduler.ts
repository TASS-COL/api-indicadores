import { INDICATOR_JOBS } from "./jobs";
import { DAMODARAN_DATASETS } from "./damodaran";
import { indicatorQueue } from "../queue/queue";
import { env } from "../config/env";
import { logger } from "../utils/logger";

export async function scheduleIndicatorJobs() {
  const repeatables = await indicatorQueue.getRepeatableJobs();
  const validIds = new Set(INDICATOR_JOBS.map((j) => `indicator:${j.key}`));

  for (const rep of repeatables) {
    const repId = rep.id ?? "";
    if (!repId || !validIds.has(repId)) {
      await indicatorQueue.removeRepeatableByKey(rep.key);
    }
  }

  for (const job of INDICATOR_JOBS) {
    const jobId = `indicator:${job.key}`;
    const cron = job.cron ?? env.indicatorCron ?? "0 * * * *";
    const existing = repeatables.find((r) => r.id === jobId);

    if (existing && (existing.pattern === cron || (existing as any).cron === cron)) continue;
    if (existing) {
      await indicatorQueue.removeRepeatableByKey(existing.key);
    }

    await indicatorQueue.add(
      job.key,
      { jobKey: job.key },
      {
        jobId,
        removeOnComplete: { count: 50 },
        removeOnFail: { count: 50 },
        repeat: { pattern: cron, tz: env.tz },
      }
    );
  }

  // Damodaran datasets (annual update, run on same cron)
  for (const ds of DAMODARAN_DATASETS) {
    const jobId = `damodaran:${ds.dataset}`;
    const cron = env.indicatorCron ?? "0 * * * *";
    const existing = repeatables.find((r) => r.id === jobId);
    if (existing && (existing.pattern === cron || (existing as any).cron === cron)) continue;
    if (existing) await indicatorQueue.removeRepeatableByKey(existing.key);
    await indicatorQueue.add(
      ds.dataset,
      { jobType: "damodaran", dataset: ds.dataset },
      {
        jobId,
        removeOnComplete: { count: 10 },
        removeOnFail: { count: 10 },
        repeat: { pattern: cron, tz: env.tz },
      },
    );
  }

  logger.info(
    `[indicator] Programados ${INDICATOR_JOBS.length} jobs (cron: ${env.indicatorCron})`
  );
}
