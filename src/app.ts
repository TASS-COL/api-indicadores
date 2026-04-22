import express from "express";
import { env } from "./config/env";

import { createBullBoard } from "@bull-board/api";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";
import { ExpressAdapter } from "@bull-board/express";

import { authRouter, adminAuthRouter } from "./routes/auth.routes";
import { indicatorsRouter } from "./routes/indicators.routes";
import { indicatorJobsRouter } from "./routes/indicator-jobs.routes";
import { indicatorQueue } from "./queue/queue";
import { jwtAuthMiddleware } from "./middleware/jwt-auth";
import { panelRouter } from "./panel/panel.router";
import { damodaranRouter } from "./routes/damodaran.routes";
import { logger } from "./utils/logger";

export async function createApp() {
  const app = express();
  app.use(express.json());

  // Health (exempt from auth)
  app.get("/health", (_req, res) => res.json({ ok: true }));

  // Bull Board
  const serverAdapter = new ExpressAdapter();
  serverAdapter.setBasePath(env.dashPath);

  createBullBoard({
    queues: [new BullMQAdapter(indicatorQueue) as any],
    serverAdapter,
  });

  app.use(env.dashPath, serverAdapter.getRouter());

  // Admin panel (public — own auth)
  app.use("/panel", panelRouter);

  // Auth token endpoint (public)
  app.post("/auth/token", (req, res, next) => {
    authRouter(req, res, next);
  });

  // JWT + API Key middleware
  app.use(jwtAuthMiddleware);

  // Routes
  app.use("/auth", authRouter);
  app.use("/indicators", indicatorsRouter);
  app.use("/indicator-jobs", indicatorJobsRouter);
  app.use("/damodaran", damodaranRouter);

  // Admin routes (requires master key per-request — no global enableAdminApi flag)
  app.use("/auth", adminAuthRouter);
  logger.info("[admin] Endpoints administrativos habilitados (protegidos por master key)");

  return app;
}
