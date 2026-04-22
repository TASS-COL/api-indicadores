import { pool } from "./db";

export interface DamodaranRow {
  id: number;
  dataset: string;
  industry_name: string;
  update_date: string;
  metrics: Record<string, number | null>;
}

export async function ensureDamodaranTables(): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS damodaran_industry (
      id            SERIAL PRIMARY KEY,
      dataset       TEXT NOT NULL,
      industry_name TEXT NOT NULL,
      update_date   DATE NOT NULL,
      metrics       JSONB NOT NULL DEFAULT '{}'::jsonb,
      UNIQUE (dataset, industry_name, update_date)
    );
    CREATE INDEX IF NOT EXISTS idx_damodaran_dataset_date
      ON damodaran_industry (dataset, update_date DESC);
  `);
}

export async function upsertDamodaranBatch(
  dataset: string,
  updateDate: string,
  rows: { industry_name: string; metrics: Record<string, number | null> }[],
): Promise<number> {
  if (!rows.length) return 0;
  let count = 0;
  for (const row of rows) {
    await pool.query(
      `INSERT INTO damodaran_industry (dataset, industry_name, update_date, metrics)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (dataset, industry_name, update_date)
       DO UPDATE SET metrics = EXCLUDED.metrics`,
      [dataset, row.industry_name, updateDate, JSON.stringify(row.metrics)],
    );
    count++;
  }
  return count;
}

export async function listDamodaranDatasets() {
  const { rows } = await pool.query(`
    SELECT dataset,
           MAX(update_date)::text AS last_update,
           COUNT(DISTINCT industry_name)::int AS industries
    FROM damodaran_industry
    GROUP BY dataset
    ORDER BY dataset
  `);
  return rows as { dataset: string; last_update: string; industries: number }[];
}

export async function getDamodaranLatestDate(dataset: string): Promise<string | null> {
  const { rows } = await pool.query(
    `SELECT MAX(update_date)::text AS date FROM damodaran_industry WHERE dataset = $1`,
    [dataset],
  );
  return rows[0]?.date ?? null;
}

export async function getDamodaranSnapshot(dataset: string, industry?: string): Promise<DamodaranRow[]> {
  const latest = await getDamodaranLatestDate(dataset);
  if (!latest) return [];
  if (industry) {
    const { rows } = await pool.query<DamodaranRow>(
      `SELECT * FROM damodaran_industry
       WHERE dataset = $1 AND update_date = $2 AND industry_name ILIKE $3
       ORDER BY industry_name`,
      [dataset, latest, `%${industry}%`],
    );
    return rows;
  }
  const { rows } = await pool.query<DamodaranRow>(
    `SELECT * FROM damodaran_industry
     WHERE dataset = $1 AND update_date = $2
     ORDER BY industry_name`,
    [dataset, latest],
  );
  return rows;
}

export async function getDamodaranIndustryHistory(dataset: string, industry: string): Promise<DamodaranRow[]> {
  const { rows } = await pool.query<DamodaranRow>(
    `SELECT * FROM damodaran_industry
     WHERE dataset = $1 AND industry_name ILIKE $2
     ORDER BY update_date DESC`,
    [dataset, industry],
  );
  return rows;
}
