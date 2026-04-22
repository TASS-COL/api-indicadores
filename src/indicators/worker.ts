import { Worker } from "bullmq";
import { runIndicatorJob } from "./jobs";
import { runDamodaranJob } from "./damodaran";
import { connection, indicatorQueue } from "../queue/queue";
import { logger } from "../utils/logger";

export const indicatorWorker = new Worker(
  indicatorQueue.name,
  async (job) => {
    if (job.data.jobType === "damodaran") {
      const { dataset } = job.data as { dataset: string };
      await job.log(`Iniciando job Damodaran: ${dataset}`);
      const result = await runDamodaranJob(dataset);
      await job.log(`Almacenados ${result.stored} registros`);
      return result;
    }
    const { jobKey } = job.data as { jobKey: string };
    await job.log(`Iniciando job de indicador ${jobKey}`);
    const result = await runIndicatorJob(jobKey);
    await job.log(`Guardados ${result.stored} registros`);
    return result;
  },
  {
    connection,
    concurrency: 1,
    removeOnComplete: { count: 50 },
    removeOnFail: { count: 50 },
  }
);

indicatorWorker.on("failed", (job, err) => {
  logger.error(`[indicator] Job ${job?.id} falló`, err?.message || err);
});

indicatorWorker.on("completed", (job) => {
  logger.info(`[indicator] Job ${job?.id} completado`);
});
