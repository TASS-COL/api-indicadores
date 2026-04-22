import { pool } from "./db";
import { IndicatorInsert, IndicatorRecord } from "../models/indicator.model";

// ─── Schema ──────────────────────────────────────────────────────────────────

export async function ensureIndicatorRecordTables(): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS indicator_record (
      id           SERIAL PRIMARY KEY,
      tenant_id    INTEGER,
      indicator    TEXT NOT NULL,
      variant      TEXT,
      value_number TEXT,
      value_text   TEXT,
      unit         TEXT,
      observed_at  TIMESTAMPTZ NOT NULL,
      fetched_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      source       TEXT,
      metadata     JSONB NOT NULL DEFAULT '{}',
      is_active    BOOLEAN NOT NULL DEFAULT TRUE
    )
  `);

  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_indicator_record_unique
    ON indicator_record (indicator, variant, observed_at)
    WHERE variant IS NOT NULL
  `);

  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_indicator_record_unique_no_variant
    ON indicator_record (indicator, observed_at)
    WHERE variant IS NULL
  `);
}

// ─── Data access ─────────────────────────────────────────────────────────────

const toIso = (value: string | Date | undefined): string => {
  if (!value) return new Date().toISOString();
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return new Date().toISOString();
  return date.toISOString();
};

export async function ensureIndicatorUniqueIndex(): Promise<void> {
  await ensureIndicatorRecordTables();
}

export async function deleteIndicatorsByName(indicator: string): Promise<void> {
  await pool.query(`DELETE FROM indicator_record WHERE indicator = $1`, [indicator]);
}

export async function insertIndicatorRecordSimple(
  entry: IndicatorInsert
): Promise<IndicatorRecord> {
  const { rows } = await pool.query<IndicatorRecord>(
    `INSERT INTO indicator_record (
      tenant_id, indicator, variant, value_number, value_text,
      unit, observed_at, fetched_at, source, metadata, is_active
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
    RETURNING *`,
    [
      entry.tenant_id ?? null,
      entry.indicator,
      entry.variant ?? null,
      entry.value_number ?? null,
      entry.value_text ?? null,
      entry.unit ?? null,
      toIso(entry.observed_at),
      toIso(entry.fetched_at),
      entry.source ?? null,
      entry.metadata ?? {},
      entry.is_active ?? true,
    ]
  );
  return rows[0];
}

export async function insertIndicatorRecord(
  entry: IndicatorInsert
): Promise<IndicatorRecord> {
  const { rows } = await pool.query<IndicatorRecord>(
    `INSERT INTO indicator_record (
      tenant_id, indicator, variant, value_number, value_text,
      unit, observed_at, fetched_at, source, metadata, is_active
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
    ON CONFLICT (indicator, variant, observed_at)
      WHERE variant IS NOT NULL
    DO UPDATE SET
      value_number = EXCLUDED.value_number,
      value_text   = EXCLUDED.value_text,
      unit         = EXCLUDED.unit,
      fetched_at   = EXCLUDED.fetched_at,
      source       = EXCLUDED.source,
      metadata     = EXCLUDED.metadata,
      is_active    = EXCLUDED.is_active
    RETURNING *`,
    [
      entry.tenant_id ?? null,
      entry.indicator,
      entry.variant ?? null,
      entry.value_number ?? null,
      entry.value_text ?? null,
      entry.unit ?? null,
      toIso(entry.observed_at),
      toIso(entry.fetched_at),
      entry.source ?? null,
      entry.metadata ?? {},
      entry.is_active ?? true,
    ]
  );
  return rows[0];
}

export async function getLatestIndicatorRecords(): Promise<IndicatorRecord[]> {
  const { rows } = await pool.query<IndicatorRecord>(
    `SELECT DISTINCT ON (indicator, variant)
       id, tenant_id, indicator, variant, value_number, value_text,
       unit, observed_at, fetched_at, source, metadata, is_active
     FROM indicator_record
     WHERE is_active = TRUE
     ORDER BY indicator, variant, observed_at DESC`
  );
  return rows;
}

export async function getLatestByIndicator(
  indicator: string
): Promise<IndicatorRecord[]> {
  const { rows } = await pool.query<IndicatorRecord>(
    `SELECT DISTINCT ON (indicator, variant)
       id, tenant_id, indicator, variant, value_number, value_text,
       unit, observed_at, fetched_at, source, metadata, is_active
     FROM indicator_record
     WHERE indicator = $1 AND is_active = TRUE
     ORDER BY indicator, variant, observed_at DESC`,
    [indicator]
  );
  return rows;
}
