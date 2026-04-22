import { Router, Request, Response } from "express";
import { indicatorQueue } from "../queue/queue";
import { INDICATOR_JOBS, runIndicatorJob } from "../indicators/jobs";
import { fetchUsuraAndIbcHistory, fetchIppDaneHistory } from "../indicators/fetchers";

export const indicatorJobsRouter = Router();

// GET /indicator-jobs — list all configured jobs with status
indicatorJobsRouter.get("/", async (_req: Request, res: Response) => {
  try {
    const repeatables = await indicatorQueue.getRepeatableJobs();
    const jobs = INDICATOR_JOBS.map((job) => {
      const rep = repeatables.find((r) => r.id === `indicator:${job.key}`);
      return {
        key: job.key,
        description: job.description,
        cron: job.cron,
        skipUpsert: job.skipUpsert ?? false,
        scheduled: !!rep,
        nextRun: rep?.next ? new Date(rep.next).toISOString() : null,
      };
    });
    res.json({ ok: true, count: jobs.length, jobs });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// POST /indicator-jobs/:key/run — trigger a job immediately
indicatorJobsRouter.post("/:key/run", async (req: Request, res: Response) => {
  try {
    const { key } = req.params;
    const job = INDICATOR_JOBS.find((j) => j.key === key.toUpperCase());
    if (!job) {
      return res.status(404).json({
        ok: false,
        error: `Job '${key}' no encontrado. Claves válidas: ${INDICATOR_JOBS.map((j) => j.key).join(", ")}`,
      });
    }

    const result = await runIndicatorJob(job.key);
    res.json({ ok: true, result });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// POST /indicator-jobs/run-all — trigger all jobs sequentially
indicatorJobsRouter.post("/run-all", async (_req: Request, res: Response) => {
  const results = [];
  const errors = [];

  for (const job of INDICATOR_JOBS) {
    try {
      const result = await runIndicatorJob(job.key);
      results.push(result);
    } catch (e: any) {
      errors.push({ key: job.key, error: e.message });
    }
  }

  res.json({
    ok: errors.length === 0,
    results,
    errors,
  });
});

// POST /indicator-jobs/backfill — carga historial completo disponible en cada fuente
indicatorJobsRouter.post("/backfill", async (req: Request, res: Response) => {
  const fromYear: number = Number(req.body?.from_year) || 2019;
  const results = [];
  const errors  = [];

  // 1. Todos los jobs regulares (datos recientes/actuales)
  for (const job of INDICATOR_JOBS) {
    try {
      results.push(await runIndicatorJob(job.key));
    } catch (e: any) {
      errors.push({ key: job.key, error: e.message });
    }
  }

  // 2. Historial completo Superfinanciera (USURA / IBC) desde el Excel acumulado
  try {
    results.push(await runIndicatorJob("USURA_IBC", fetchUsuraAndIbcHistory));
  } catch (e: any) {
    errors.push({ key: "USURA_IBC_HISTORY", error: e.message });
  }

  // 3. Historial IPP-DANE mes a mes desde from_year
  try {
    results.push(await runIndicatorJob("IPP", () => fetchIppDaneHistory(fromYear, 1)));
  } catch (e: any) {
    errors.push({ key: "IPP_HISTORY", error: e.message });
  }

  res.json({ ok: errors.length === 0, from_year: fromYear, results, errors });
});

// GET /indicator-jobs/queue/stats — queue statistics
indicatorJobsRouter.get("/queue/stats", async (_req: Request, res: Response) => {
  try {
    const [waiting, active, completed, failed, delayed] = await Promise.all([
      indicatorQueue.getWaitingCount(),
      indicatorQueue.getActiveCount(),
      indicatorQueue.getCompletedCount(),
      indicatorQueue.getFailedCount(),
      indicatorQueue.getDelayedCount(),
    ]);
    res.json({ ok: true, stats: { waiting, active, completed, failed, delayed } });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e.message });
  }
});
