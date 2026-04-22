import { pool } from "./db";
import { IndicatorCatalog, IndicatorCatalogInsert } from "../models/indicator-catalog.model";
import { IndicatorHistoric, IndicatorHistoricUpsert } from "../models/indicator-historic.model";

// ─── Schema ──────────────────────────────────────────────────────────────────

export async function ensureIndicatorTables(): Promise<void> {
  // Create ENUM types (idempotent)
  await pool.query(`
    DO $$ BEGIN
      CREATE TYPE indicator_term_unit_enum AS ENUM (
        'diaria', 'semanal', 'mensual', 'bimestral', 'trimestral', 'semestral', 'anual'
      );
    EXCEPTION WHEN duplicate_object THEN NULL;
    END $$
  `);

  await pool.query(`
    DO $$ BEGIN
      CREATE TYPE indicator_unit_of_measure_enum AS ENUM (
        'percentage', 'cop', 'usd', 'eur', 'basis_points', 'index'
      );
    EXCEPTION WHEN duplicate_object THEN NULL;
    END $$
  `);

  await pool.query(`
    DO $$ BEGIN
      CREATE TYPE indicator_record_status_enum AS ENUM (
        'definitive', 'provisional', 'revised'
      );
    EXCEPTION WHEN duplicate_object THEN NULL;
    END $$
  `);

  // Indicator catalog table
  await pool.query(`
    CREATE TABLE IF NOT EXISTS indicator (
      id           SERIAL PRIMARY KEY,
      name         TEXT NOT NULL,
      alias        TEXT NOT NULL,
      segmentation TEXT,
      term_value   NUMERIC,
      term_unit    indicator_term_unit_enum,
      base         NUMERIC,
      unit         indicator_unit_of_measure_enum NOT NULL,
      source       TEXT,
      is_active    BOOLEAN NOT NULL DEFAULT TRUE,
      created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  // Partial unique index to support ON CONFLICT in upsert
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_indicator_catalog_key
      ON indicator (alias, COALESCE(segmentation, ''), COALESCE(term_value, -1), term_unit)
  `);

  // Indicator historic table
  await pool.query(`
    CREATE TABLE IF NOT EXISTS indicator_historic (
      id           SERIAL PRIMARY KEY,
      indicator_id INTEGER NOT NULL REFERENCES indicator(id) ON DELETE CASCADE,
      from_date    DATE NOT NULL,
      to_date      DATE,
      value        TEXT NOT NULL,
      periodicity  indicator_term_unit_enum,
      status       indicator_record_status_enum NOT NULL DEFAULT 'definitive',
      created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (indicator_id, from_date)
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_indicator_historic_indicator_from
      ON indicator_historic (indicator_id, from_date DESC)
  `);
}

// ─── Indicator catalog ────────────────────────────────────────────────────────

export async function getIndicatorByAlias(alias: string): Promise<IndicatorCatalog | null> {
  const { rows } = await pool.query<IndicatorCatalog>(
    `SELECT * FROM indicator WHERE alias = $1 AND is_active = true LIMIT 1`,
    [alias]
  );
  return rows[0] ?? null;
}

export async function getIndicatorByCatalogKey(
  alias: string,
  segmentation?: string | null,
  termValue?: number | null,
  termUnit?: string | null
): Promise<IndicatorCatalog | null> {
  const { rows } = await pool.query<IndicatorCatalog>(
    `SELECT * FROM indicator
     WHERE alias = $1
       AND COALESCE(segmentation, '') = COALESCE($2, '')
       AND COALESCE(term_value, -1) = COALESCE($3, -1)
       AND term_unit IS NOT DISTINCT FROM $4::indicator_term_unit_enum
       AND is_active = true
     LIMIT 1`,
    [alias, segmentation ?? null, termValue ?? null, termUnit ?? null]
  );
  return rows[0] ?? null;
}

export async function upsertIndicatorCatalog(data: IndicatorCatalogInsert): Promise<IndicatorCatalog> {
  const { rows } = await pool.query<IndicatorCatalog>(
    `INSERT INTO indicator (name, alias, segmentation, term_value, term_unit, base, unit, source, is_active)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
     ON CONFLICT (alias, COALESCE(segmentation, ''), COALESCE(term_value, -1), term_unit) DO UPDATE SET
       name         = EXCLUDED.name,
       segmentation = EXCLUDED.segmentation,
       term_value   = EXCLUDED.term_value,
       term_unit    = EXCLUDED.term_unit,
       base         = EXCLUDED.base,
       unit         = EXCLUDED.unit,
       source       = EXCLUDED.source,
       is_active    = EXCLUDED.is_active,
       updated_at   = NOW()
     RETURNING *`,
    [
      data.name,
      data.alias,
      data.segmentation ?? null,
      data.term_value ?? null,
      data.term_unit ?? null,
      data.base ?? null,
      data.unit,
      data.source ?? null,
      data.is_active ?? true,
    ]
  );
  return rows[0];
}

export async function listIndicators(onlyActive = true): Promise<IndicatorCatalog[]> {
  const { rows } = await pool.query<IndicatorCatalog>(
    `SELECT * FROM indicator ${onlyActive ? "WHERE is_active = true" : ""} ORDER BY alias, name`
  );
  return rows;
}

// ─── Indicator historic ───────────────────────────────────────────────────────

export async function upsertIndicatorHistoric(data: IndicatorHistoricUpsert): Promise<IndicatorHistoric> {
  const fromDate = data.from_date instanceof Date
    ? data.from_date.toISOString().slice(0, 10)
    : String(data.from_date).slice(0, 10);

  const toDate = data.to_date
    ? (data.to_date instanceof Date ? data.to_date.toISOString().slice(0, 10) : String(data.to_date).slice(0, 10))
    : null;

  const { rows } = await pool.query<IndicatorHistoric>(
    `INSERT INTO indicator_historic (indicator_id, from_date, to_date, value, periodicity, status)
     VALUES ($1, $2, $3, $4, $5, $6)
     ON CONFLICT (indicator_id, from_date) DO UPDATE SET
       to_date     = EXCLUDED.to_date,
       value       = EXCLUDED.value,
       periodicity = EXCLUDED.periodicity,
       status      = EXCLUDED.status,
       updated_at  = NOW()
     RETURNING *`,
    [
      data.indicator_id,
      fromDate,
      toDate,
      data.value,
      data.periodicity ?? null,
      data.status ?? 'definitive',
    ]
  );
  return rows[0];
}

export async function getIndicatorHistoricRange(
  indicatorId: number,
  fromDate: string,
  toDate: string
): Promise<IndicatorHistoric[]> {
  const { rows } = await pool.query<IndicatorHistoric>(
    `SELECT * FROM indicator_historic
     WHERE indicator_id = $1
       AND from_date >= $2
       AND from_date <= $3
       AND status = 'definitive'
     ORDER BY from_date ASC`,
    [indicatorId, fromDate, toDate]
  );
  return rows;
}

export async function getLatestIndicatorHistoric(indicatorId: number): Promise<IndicatorHistoric | null> {
  const { rows } = await pool.query<IndicatorHistoric>(
    `SELECT * FROM indicator_historic
     WHERE indicator_id = $1 AND status = 'definitive'
     ORDER BY from_date DESC
     LIMIT 1`,
    [indicatorId]
  );
  return rows[0] ?? null;
}
