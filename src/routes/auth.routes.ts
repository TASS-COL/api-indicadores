import { Router, Request, Response } from "express";
import { env } from "../config/env";
import crypto from "crypto";
import { AuthService } from "../services/auth.service";
import {
  addAppCompany,
  listAppCompanies,
  listApplications,
  removeAppCompany,
  setApplicationActive,
} from "../db/app.repo";
import { VALID_DOC_TYPES, DocType } from "../models/app.model";

function sanitizeCompany(c: any) {
  return {
    doc_type: c.doc_type,
    company_doc_number: c.company_doc_number,
    company_name: c.company_name,
    is_active: c.is_active,
  };
}

function requireMasterKey(req: Request, res: Response): boolean {
  if (!env.apiKey) {
    res.status(503).json({
      ok: false,
      error: "API_KEY (master key) no configurada.",
    });
    return false;
  }

  const key = (req.header("x-api-key") || (req.query.api_key as string)) ?? "";
  if (!key) {
    res.status(401).json({ ok: false, error: "Master key requerida (X-Api-Key)" });
    return false;
  }

  const expected = Buffer.from(env.apiKey);
  const provided = Buffer.from(key);
  if (expected.length !== provided.length || !crypto.timingSafeEqual(expected, provided)) {
    res.status(403).json({ ok: false, error: "Master key invalida" });
    return false;
  }

  return true;
}

export const authRouter = Router();

authRouter.post("/token", async (req: Request, res: Response) => {
  try {
    const { app_id, app_secret } = req.body || {};
    if (!app_id || !app_secret) {
      return res.status(400).json({
        ok: false,
        error: "Se requieren 'app_id' (code) y 'app_secret'",
      });
    }

    const result = await AuthService.login(
      String(app_id),
      String(app_secret),
    );

    res.json({
      ok: true,
      token: result.token,
      expires_in: result.expiresIn,
    });
  } catch (e: any) {
    const isAuthError =
      e.message?.includes("invalidas") || e.message?.includes("desactivada");
    res
      .status(isAuthError ? 401 : 500)
      .json({ ok: false, error: e.message });
  }
});

authRouter.get("/companies", async (req: Request, res: Response) => {
  try {
    const auth = req.app_auth;
    if (!auth) {
      return res.status(401).json({
        ok: false,
        error: "Autenticacion JWT requerida",
      });
    }

    const companies = await listAppCompanies(auth.appId);
    res.json({
      ok: true,
      count: companies.length,
      companies: companies.map(sanitizeCompany),
    });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

export const adminAuthRouter = Router();

adminAuthRouter.post("/register", async (req: Request, res: Response) => {
  try {
    if (!requireMasterKey(req, res)) return;

    const { code, name } = req.body || {};
    if (!code || !name) {
      return res.status(400).json({
        ok: false,
        error: "Se requieren los campos 'code' y 'name'",
      });
    }

    const result = await AuthService.register(String(code), String(name));

    res.status(201).json({
      ok: true,
      message:
        "Aplicacion registrada. Guarda el app_secret, no se puede recuperar.",
      app: {
        code: result.app.code,
        name: result.app.name,
      },
      app_secret: result.appSecret,
    });
  } catch (e: any) {
    const status = e.message?.includes("ya existe") ? 409 : 500;
    res.status(status).json({ ok: false, error: e.message });
  }
});

adminAuthRouter.post("/companies", async (req: Request, res: Response) => {
  try {
    if (!requireMasterKey(req, res)) return;

    const { app_code, doc_type, doc_number, name } = req.body || {};
    if (!app_code || !doc_number || !name) {
      return res.status(400).json({
        ok: false,
        error: "Se requieren 'app_code', 'doc_number' y 'name'",
      });
    }

    const docType = String(doc_type || "NIT").trim().toUpperCase();
    if (!VALID_DOC_TYPES.includes(docType as DocType)) {
      return res.status(400).json({
        ok: false,
        error: `doc_type invalido: '${docType}'. Valores permitidos: ${VALID_DOC_TYPES.join(", ")}`,
      });
    }

    const { getApplicationByCode } = await import("../db/app.repo");
    const app = await getApplicationByCode(String(app_code));
    if (!app) {
      return res
        .status(404)
        .json({ ok: false, error: `Aplicacion '${app_code}' no encontrada` });
    }

    const company = await addAppCompany(
      app.id,
      docType,
      String(doc_number),
      String(name),
    );

    res.status(201).json({ ok: true, company: sanitizeCompany(company as any) });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

adminAuthRouter.delete("/companies/:doc_number", async (req: Request, res: Response) => {
  try {
    if (!requireMasterKey(req, res)) return;

    const { app_code, doc_type } = req.body || {};
    const { doc_number } = req.params;

    if (!app_code) {
      return res.status(400).json({
        ok: false,
        error: "Se requiere 'app_code' en el body",
      });
    }

    const docType = String(doc_type || "NIT").trim().toUpperCase();
    if (!VALID_DOC_TYPES.includes(docType as DocType)) {
      return res.status(400).json({
        ok: false,
        error: `doc_type invalido: '${docType}'. Valores permitidos: ${VALID_DOC_TYPES.join(", ")}`,
      });
    }

    const { getApplicationByCode } = await import("../db/app.repo");
    const app = await getApplicationByCode(String(app_code));
    if (!app) {
      return res
        .status(404)
        .json({ ok: false, error: `Aplicacion '${app_code}' no encontrada` });
    }

    const removed = await removeAppCompany(app.id, docType, doc_number);
    if (!removed) {
      return res
        .status(404)
        .json({ ok: false, error: "Documento no encontrado para este aplicativo" });
    }

    res.json({ ok: true, message: `Documento ${docType}:${doc_number} desasociado de ${app_code}` });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

adminAuthRouter.get("/apps", async (req: Request, res: Response) => {
  try {
    if (!requireMasterKey(req, res)) return;

    const apps = await listApplications();
    res.json({
      ok: true,
      count: apps.length,
      apps: apps.map((a) => ({
        code: a.code,
        name: a.name,
        is_active: a.is_active,
      })),
    });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

adminAuthRouter.patch("/apps/:code/status", async (req: Request, res: Response) => {
  try {
    if (!requireMasterKey(req, res)) return;

    const { is_active } = req.body || {};
    if (typeof is_active !== "boolean") {
      return res.status(400).json({
        ok: false,
        error: "Se requiere 'is_active' (boolean)",
      });
    }

    const { getApplicationByCode } = await import("../db/app.repo");
    const app = await getApplicationByCode(req.params.code);
    if (!app) {
      return res.status(404).json({ ok: false, error: "Aplicacion no encontrada" });
    }

    await setApplicationActive(app.id, is_active);
    res.json({
      ok: true,
      message: `Aplicacion '${app.code}' ${is_active ? "activada" : "desactivada"}`,
    });
  } catch (e: any) {
    res.status(500).json({ ok: false, error: e.message });
  }
});
