import { pool } from "./db";

export interface IndicatorGroup {
  id: number;
  name: string;
  description: string | null;
  is_active: boolean;
}

export interface IndicatorGroupWithMembers extends IndicatorGroup {
  members: { id: number; alias: string; name: string; segmentation: string | null }[];
}

// ─── Schema ──────────────────────────────────────────────────────────────────

export async function ensureIndicatorGroupTables(): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS indicator_group (
      id          SERIAL PRIMARY KEY,
      name        TEXT NOT NULL UNIQUE,
      description TEXT,
      is_active   BOOLEAN NOT NULL DEFAULT TRUE,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS indicator_group_member (
      id           SERIAL PRIMARY KEY,
      group_id     INTEGER NOT NULL REFERENCES indicator_group(id) ON DELETE CASCADE,
      indicator_id INTEGER NOT NULL REFERENCES indicator(id) ON DELETE CASCADE,
      UNIQUE (group_id, indicator_id)
    )
  `);
}

// ─── Data access ─────────────────────────────────────────────────────────────

export async function upsertIndicatorGroup(
  name: string,
  description?: string | null
): Promise<IndicatorGroup> {
  const { rows } = await pool.query<IndicatorGroup>(
    `INSERT INTO indicator_group (name, description)
     VALUES ($1, $2)
     ON CONFLICT (name) DO UPDATE SET
       description = EXCLUDED.description,
       updated_at  = NOW()
     RETURNING *`,
    [name, description ?? null]
  );
  return rows[0];
}

export async function addGroupMember(groupId: number, indicatorId: number): Promise<void> {
  await pool.query(
    `INSERT INTO indicator_group_member (group_id, indicator_id)
     VALUES ($1, $2)
     ON CONFLICT DO NOTHING`,
    [groupId, indicatorId]
  );
}

export async function listIndicatorGroups(): Promise<IndicatorGroupWithMembers[]> {
  const { rows: groups } = await pool.query<IndicatorGroup>(
    `SELECT * FROM indicator_group WHERE is_active = TRUE ORDER BY name`
  );

  const result: IndicatorGroupWithMembers[] = [];
  for (const group of groups) {
    const { rows: members } = await pool.query<{ id: number; alias: string; name: string; segmentation: string | null }>(
      `SELECT i.id, i.alias, i.name, i.segmentation
       FROM indicator_group_member m
       JOIN indicator i ON i.id = m.indicator_id
       WHERE m.group_id = $1
       ORDER BY i.alias, i.name`,
      [group.id]
    );
    result.push({ ...group, members });
  }
  return result;
}
