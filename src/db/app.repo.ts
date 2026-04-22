import { pool } from "./db";
import {
  ErpApplication,
  ErpApplicationPublic,
  ErpAppCompany,
} from "../models/app.model";

// ─── Schema ──────────────────────────────────────────────────────────────────

export async function ensureAppTables(): Promise<void> {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS erp_application (
      id          SERIAL PRIMARY KEY,
      code        TEXT NOT NULL UNIQUE,
      name        TEXT NOT NULL,
      secret_hash TEXT NOT NULL,
      is_active   BOOLEAN NOT NULL DEFAULT TRUE,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS erp_app_company (
      id                  SERIAL PRIMARY KEY,
      app_id              INTEGER NOT NULL REFERENCES erp_application(id) ON DELETE CASCADE,
      doc_type            TEXT NOT NULL DEFAULT 'NIT',
      company_doc_number  TEXT NOT NULL,
      company_name        TEXT NOT NULL,
      is_active           BOOLEAN NOT NULL DEFAULT TRUE,
      created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CHECK (doc_type IN ('NIT', 'CC', 'CE', 'PP'))
    )
  `);

  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_erp_app_company_app_doctype_docnumber
      ON erp_app_company(app_id, doc_type, company_doc_number)
  `);
}

// ─── Application CRUD ────────────────────────────────────────────────────────

export async function createApplication(
  code: string,
  name: string,
  secretHash: string,
): Promise<ErpApplication> {
  const { rows } = await pool.query<ErpApplication>(
    `INSERT INTO erp_application (code, name, secret_hash)
     VALUES ($1, $2, $3)
     RETURNING *`,
    [code.trim().toLowerCase(), name.trim(), secretHash],
  );
  return rows[0];
}

export async function getApplicationByCode(
  code: string,
): Promise<ErpApplication | null> {
  const { rows } = await pool.query<ErpApplication>(
    `SELECT * FROM erp_application WHERE code = $1 LIMIT 1`,
    [code.trim().toLowerCase()],
  );
  return rows[0] || null;
}

export async function getApplicationById(
  id: number,
): Promise<ErpApplication | null> {
  const { rows } = await pool.query<ErpApplication>(
    `SELECT * FROM erp_application WHERE id = $1 LIMIT 1`,
    [id],
  );
  return rows[0] || null;
}

export async function listApplications(): Promise<ErpApplicationPublic[]> {
  const { rows } = await pool.query<ErpApplicationPublic>(
    `SELECT id, code, name, is_active, created_at, updated_at
       FROM erp_application
      ORDER BY id ASC`,
  );
  return rows;
}

export async function setApplicationActive(
  id: number,
  isActive: boolean,
): Promise<boolean> {
  const result = await pool.query(
    `UPDATE erp_application
        SET is_active = $2, updated_at = NOW()
      WHERE id = $1`,
    [id, isActive],
  );
  return (result.rowCount || 0) > 0;
}

// ─── App-Company association ─────────────────────────────────────────────────

export async function addAppCompany(
  appId: number,
  docType: string,
  docNumber: string,
  companyName: string,
): Promise<ErpAppCompany> {
  const { rows } = await pool.query<ErpAppCompany>(
    `INSERT INTO erp_app_company (app_id, doc_type, company_doc_number, company_name)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (app_id, doc_type, company_doc_number)
     DO UPDATE SET company_name = EXCLUDED.company_name, is_active = TRUE
     RETURNING *`,
    [appId, docType.trim().toUpperCase(), docNumber.trim(), companyName.trim()],
  );
  return rows[0];
}

export async function listAppCompanies(
  appId: number,
  onlyActive = true,
): Promise<ErpAppCompany[]> {
  const { rows } = await pool.query<ErpAppCompany>(
    `SELECT *
       FROM erp_app_company
      WHERE app_id = $1 ${onlyActive ? "AND is_active = TRUE" : ""}
      ORDER BY id ASC`,
    [appId],
  );
  return rows;
}

export async function removeAppCompany(
  appId: number,
  docType: string,
  docNumber: string,
): Promise<boolean> {
  const result = await pool.query(
    `UPDATE erp_app_company
        SET is_active = FALSE
      WHERE app_id = $1 AND doc_type = $2 AND company_doc_number = $3`,
    [appId, docType.trim().toUpperCase(), docNumber.trim()],
  );
  return (result.rowCount || 0) > 0;
}

export async function listAllCompaniesWithApp(): Promise<
  (ErpAppCompany & { app_code: string })[]
> {
  const { rows } = await pool.query<ErpAppCompany & { app_code: string }>(
    `SELECT c.*, a.code AS app_code
       FROM erp_app_company c
       JOIN erp_application a ON a.id = c.app_id
      ORDER BY a.code ASC, c.id ASC`,
  );
  return rows;
}

export async function getActiveCompanyDocNumbers(
  appId: number,
): Promise<string[]> {
  const { rows } = await pool.query<{ company_doc_number: string }>(
    `SELECT company_doc_number
       FROM erp_app_company
      WHERE app_id = $1 AND is_active = TRUE`,
    [appId],
  );
  return rows.map((r) => r.company_doc_number);
}
