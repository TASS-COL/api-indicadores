// ─── Application registry & JWT types ────────────────────────────────────────

export interface ErpApplication {
  id: number;
  code: string;
  name: string;
  secret_hash: string;
  is_active: boolean;
  created_at?: string;
  updated_at?: string;
}

export interface ErpApplicationPublic {
  id: number;
  code: string;
  name: string;
  is_active: boolean;
  created_at?: string;
  updated_at?: string;
}

export interface ErpAppCompany {
  id: number;
  app_id: number;
  doc_type: string;
  company_doc_number: string;
  company_name: string;
  is_active: boolean;
  created_at?: string;
}

export interface ErpAppRegisterInput {
  code: string;
  name: string;
}

export interface ErpAppCompanyInput {
  doc_type: string;
  doc_number: string;
  name: string;
}

export interface JwtPayload {
  sub: string;        // app code
  app_id: number;     // app numeric id
  companies: string[]; // allowed NITs
  iat?: number;
  exp?: number;
}

export interface AuthenticatedApp {
  appId: number;
  appCode: string;
  companies: string[];
}

// ─── Document type helpers ────────────────────────────────────────────────────

export type DocType = "NIT" | "CC" | "CE" | "PP";
export const VALID_DOC_TYPES: DocType[] = ["NIT", "CC", "CE", "PP"];
