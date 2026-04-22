import { Queue } from "bullmq";
import IORedis from "ioredis";
import { env } from "../config/env";

export const connection = new IORedis(env.redisUrl, {
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
});

export const indicatorQueue = new Queue(env.indicatorQueueName, {
  connection,
  defaultJobOptions: {
    removeOnComplete: { count: 50 },
    removeOnFail: { count: 50 },
  },
});
