import { Router, Request, Response } from "express";
import {
  listDamodaranDatasets,
  getDamodaranSnapshot,
  getDamodaranIndustryHistory,
} from "../db/damodaran.repo";
import { DAMODARAN_DATASETS, runDamodaranJob } from "../indicators/damodaran";

export const damodaranRouter = Router();

// GET /damodaran  → list available datasets with last update date
damodaranRouter.get("/", async (_req: Request, res: Response) => {
  try {
    const datasets = await listDamodaranDatasets();
    const available = DAMODARAN_DATASETS.map((d) => {
      const found = datasets.find((r) => r.dataset === d.dataset);
      return {
        dataset: d.dataset,
        name: d.name,
        last_update: found?.last_update ?? null,
        industries: found?.industries ?? 0,
        endpoint: `/damodaran/${d.dataset}`,
      };
    });
    res.json({ ok: true, datasets: available });
  } catch (e: any) { res.status(500).json({ ok: false, error: e.message }); }
});

// GET /damodaran/:dataset?industry=Banking  → latest snapshot, optional filter
damodaranRouter.get("/:dataset", async (req: Request, res: Response) => {
  try {
    const { dataset } = req.params;
    const industry = req.query.industry ? String(req.query.industry) : undefined;
    const rows = await getDamodaranSnapshot(dataset, industry);
    if (!rows.length && !industry) {
      return res.status(404).json({ ok: false, error: `Dataset '${dataset}' sin datos. Ejecuta el job primero.` });
    }
    res.json({ ok: true, dataset, count: rows.length, rows });
  } catch (e: any) { res.status(500).json({ ok: false, error: e.message }); }
});

// GET /damodaran/:dataset/industry/:name/history  → historical for one industry
damodaranRouter.get("/:dataset/industry/:name/history", async (req: Request, res: Response) => {
  try {
    const rows = await getDamodaranIndustryHistory(req.params.dataset, req.params.name);
    res.json({ ok: true, dataset: req.params.dataset, industry: req.params.name, count: rows.length, rows });
  } catch (e: any) { res.status(500).json({ ok: false, error: e.message }); }
});

// POST /damodaran/:dataset/refresh  → trigger manual refresh
damodaranRouter.post("/:dataset/refresh", async (req: Request, res: Response) => {
  try {
    const { dataset } = req.params;
    const result = await runDamodaranJob(dataset);
    res.json({ ok: true, ...result });
  } catch (e: any) { res.status(500).json({ ok: false, error: e.message }); }
});
