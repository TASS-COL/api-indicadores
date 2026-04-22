import { Router, Request, Response } from "express";
import { IndicatorsService } from "../services/indicators.service";

export const indicatorsRouter = Router();

// GET /indicators/catalog
indicatorsRouter.get("/catalog", async (_req: Request, res: Response) => {
  try {
    const catalog = await IndicatorsService.listCatalog();
    const baseUrl = `${_req.protocol}://${_req.get("host")}`;
    res.json({
      ok: true,
      count: catalog.length,
      catalog: catalog.map((item) => ({
        ...item,
        endpoint: `${baseUrl}/indicators/${item.alias}/historic`,
        endpoint_latest: `${baseUrl}/indicators/${item.alias}/latest`,
      })),
    });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// GET /indicators/groups
indicatorsRouter.get("/groups", async (_req: Request, res: Response) => {
  try {
    const groups = await IndicatorsService.listGroups();
    res.json({ ok: true, count: groups.length, groups });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// GET /indicators/snapshot
indicatorsRouter.get("/snapshot", async (_req: Request, res: Response) => {
  try {
    const snapshot = await IndicatorsService.snapshot();
    res.json({ ok: true, count: snapshot.length, snapshot });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// GET /indicators/latest
indicatorsRouter.get("/latest", async (_req: Request, res: Response) => {
  try {
    const records = await IndicatorsService.getLatestRecords();
    res.json({ ok: true, count: records.length, records });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// GET /indicators/:alias/latest
indicatorsRouter.get("/:alias/latest", async (req: Request, res: Response) => {
  try {
    const entry = await IndicatorsService.getLatestForAlias(req.params.alias);
    if (!entry) {
      return res.status(404).json({ ok: false, error: "Indicador no encontrado" });
    }
    res.json({ ok: true, indicator: entry });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// GET /indicators/:alias/records
indicatorsRouter.get("/:alias/records", async (req: Request, res: Response) => {
  try {
    const records = await IndicatorsService.getLatestRecordsByIndicator(req.params.alias);
    res.json({ ok: true, count: records.length, records });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// GET /indicators/historic?alias=&from=&to=&segmentation=&page=  → historial plano con join a indicador
indicatorsRouter.get("/historic", async (req: Request, res: Response) => {
  try {
    const alias        = req.query.alias        ? String(req.query.alias).toUpperCase()   : null;
    const from         = req.query.from         ? String(req.query.from)                  : null;
    const to           = req.query.to           ? String(req.query.to)                    : null;
    const segmentation = req.query.segmentation ? String(req.query.segmentation)          : null;
    const page         = Math.max(1, Number(req.query.page) || 1);
    const limit        = 200;
    const offset       = (page - 1) * limit;

    const conditions: string[] = [];
    const params: any[]        = [];

    if (alias)        { params.push(alias);        conditions.push(`i.alias = $${params.length}`); }
    if (from)         { params.push(from);          conditions.push(`ih.from_date >= $${params.length}`); }
    if (to)           { params.push(to);            conditions.push(`ih.from_date <= $${params.length}`); }
    if (segmentation) { params.push(segmentation);  conditions.push(`i.segmentation = $${params.length}`); }

    const where = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";

    params.push(limit);  const limitIdx  = params.length;
    params.push(offset); const offsetIdx = params.length;

    const { pool: db } = await import("../db/db");
    const [dataRes, countRes] = await Promise.all([
      db.query(
        `SELECT
           i.alias, i.name, i.segmentation, i.term_value, i.term_unit,
           i.base, i.unit, i.source,
           ih.from_date, ih.to_date, ih.value, ih.periodicity, ih.status
         FROM indicator_historic ih
         JOIN indicator i ON i.id = ih.indicator_id
         ${where}
         ORDER BY ih.from_date DESC, i.alias
         LIMIT $${limitIdx} OFFSET $${offsetIdx}`,
        params,
      ),
      db.query(
        `SELECT COUNT(*) FROM indicator_historic ih JOIN indicator i ON i.id = ih.indicator_id ${where}`,
        params.slice(0, params.length - 2),
      ),
    ]);

    const total = Number(countRes.rows[0].count);
    res.json({ ok: true, total, page, limit, pages: Math.ceil(total / limit) || 1, rows: dataRes.rows });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// GET /indicators/:alias/historic?from=YYYY-MM-DD&to=YYYY-MM-DD[&segmentation=...&term_value=...&term_unit=...]
indicatorsRouter.get("/:alias/historic", async (req: Request, res: Response) => {
  try {
    const { alias } = req.params;
    const { from, to, segmentation, term_value, term_unit } = req.query;

    if (!from || !to) {
      return res.status(400).json({
        ok: false,
        error: "Los parámetros 'from' y 'to' son requeridos (YYYY-MM-DD)",
      });
    }

    const result = await IndicatorsService.getHistoric(
      alias,
      String(from),
      String(to),
      segmentation ? String(segmentation) : null,
      term_value ? Number(term_value) : null,
      term_unit ? String(term_unit) : null,
    );

    if (!result) {
      return res.status(404).json({ ok: false, error: "Indicador no encontrado" });
    }

    res.json({
      ok: true,
      indicator: result.indicator,
      count: result.rows.length,
      rows: result.rows,
    });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e.message });
  }
});
