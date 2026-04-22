import { Router, Request, Response, NextFunction } from "express";
import express from "express";
import crypto from "crypto";
import { env } from "../config/env";
import { AuthService } from "../services/auth.service";
import {
  listApplications,
  getApplicationByCode,
  setApplicationActive,
  listAllCompaniesWithApp,
  addAppCompany,
  removeAppCompany,
} from "../db/app.repo";
import { pool } from "../db/db";
import { listDamodaranDatasets } from "../db/damodaran.repo";
import { DAMODARAN_DATASETS } from "../indicators/damodaran";
import { logger } from "../utils/logger";

export const panelRouter = Router();

const PANEL_PATH = "/panel";
const COOKIE_NAME = "_apiind_admin";

// ─── Session (HMAC stateless) ────────────────────────────────────────────────

const SESSION_SECRET = crypto.randomBytes(32).toString("hex");
const SESSION_MAX_AGE = 4 * 60 * 60 * 1000;

function signSession(user: string): string {
  const ts = Date.now().toString();
  const payload = `${user}:${ts}`;
  const hmac = crypto.createHmac("sha256", SESSION_SECRET).update(payload).digest("hex");
  return `${payload}:${hmac}`;
}

function verifySession(cookie: string): boolean {
  if (!cookie) return false;
  const parts = cookie.split(":");
  if (parts.length !== 3) return false;
  const [user, ts, hmac] = parts;
  const age = Date.now() - Number(ts);
  if (!Number.isFinite(age) || age < 0 || age > SESSION_MAX_AGE) return false;
  const expected = crypto.createHmac("sha256", SESSION_SECRET).update(`${user}:${ts}`).digest("hex");
  if (expected.length !== hmac.length) return false;
  return crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(hmac));
}

// ─── Cookies ────────────────────────────────────────────────────────────────

function parseCookies(header: string | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  if (!header) return out;
  for (const pair of header.split(";")) {
    const eq = pair.indexOf("=");
    if (eq < 0) continue;
    out[pair.substring(0, eq).trim()] = decodeURIComponent(pair.substring(eq + 1).trim());
  }
  return out;
}

// ─── Rate limiting ───────────────────────────────────────────────────────────

const loginAttempts = new Map<string, { count: number; resetAt: number }>();
const MAX_ATTEMPTS = 5;
const WINDOW_MS = 15 * 60 * 1000;

function isRateLimited(ip: string): boolean {
  const now = Date.now();
  const entry = loginAttempts.get(ip);
  if (!entry || now > entry.resetAt) {
    loginAttempts.set(ip, { count: 1, resetAt: now + WINDOW_MS });
    return false;
  }
  entry.count++;
  return entry.count > MAX_ATTEMPTS;
}

function clearRateLimit(ip: string): void {
  loginAttempts.delete(ip);
}

setInterval(() => {
  const now = Date.now();
  for (const [ip, entry] of loginAttempts) {
    if (now > entry.resetAt) loginAttempts.delete(ip);
  }
}, 30 * 60 * 1000).unref();

// ─── CSRF (stateless HMAC) ───────────────────────────────────────────────────

const CSRF_SECRET = crypto.randomBytes(32).toString("hex");

function generateCsrfToken(): string {
  const ts = Date.now().toString();
  const hmac = crypto.createHmac("sha256", CSRF_SECRET).update(ts).digest("hex");
  return `${ts}.${hmac}`;
}

function validateCsrfToken(token: string): boolean {
  if (!token) return false;
  const dot = token.indexOf(".");
  if (dot === -1) return false;
  const ts = token.substring(0, dot);
  const hmac = token.substring(dot + 1);
  const age = Date.now() - Number(ts);
  if (!Number.isFinite(age) || age > 30 * 60 * 1000 || age < 0) return false;
  const expected = crypto.createHmac("sha256", CSRF_SECRET).update(ts).digest("hex");
  if (expected.length !== hmac.length) return false;
  return crypto.timingSafeEqual(Buffer.from(hmac), Buffer.from(expected));
}

// ─── Security headers ────────────────────────────────────────────────────────

panelRouter.use((_req: Request, res: Response, next: NextFunction) => {
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-XSS-Protection", "1; mode=block");
  res.setHeader("Referrer-Policy", "no-referrer");
  res.setHeader(
    "Content-Security-Policy",
    "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; img-src data:; font-src data:;",
  );
  res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, private");
  res.setHeader("Pragma", "no-cache");
  next();
});

// ─── Login / Logout ──────────────────────────────────────────────────────────

panelRouter.use(express.urlencoded({ extended: false }));

panelRouter.get("/login", (req: Request, res: Response) => {
  const cookies = parseCookies(req.headers.cookie);
  if (verifySession(cookies[COOKIE_NAME])) return res.redirect(PANEL_PATH);
  const error =
    req.query.error === "1" ? "Credenciales invalidas" :
    req.query.error === "rate" ? "Demasiados intentos. Espera 15 minutos." :
    undefined;
  res.type("html").send(getLoginHtml(error));
});

panelRouter.post("/login", (req: Request, res: Response) => {
  const ip = req.ip || req.socket.remoteAddress || "unknown";
  if (isRateLimited(ip)) {
    logger.warn(`[panel] Rate limited: ${ip}`);
    return res.redirect(`${PANEL_PATH}/login?error=rate`);
  }

  const u = String(req.body?.user || "");
  const p = String(req.body?.pass || "");

  const validUser = Buffer.from(env.dashUser);
  const givenUser = Buffer.from(u);
  const validPass = Buffer.from(env.dashPass);
  const givenPass = Buffer.from(p);

  const uLen = Math.max(validUser.length, givenUser.length) || 1;
  const pLen = Math.max(validPass.length, givenPass.length) || 1;
  const pu1 = Buffer.alloc(uLen); validUser.copy(pu1);
  const pu2 = Buffer.alloc(uLen); givenUser.copy(pu2);
  const pp1 = Buffer.alloc(pLen); validPass.copy(pp1);
  const pp2 = Buffer.alloc(pLen); givenPass.copy(pp2);

  const ok =
    validUser.length === givenUser.length && crypto.timingSafeEqual(pu1, pu2) &&
    validPass.length === givenPass.length && crypto.timingSafeEqual(pp1, pp2);

  if (!ok) {
    logger.warn(`[panel] Login fallido desde ${ip}`);
    return res.redirect(`${PANEL_PATH}/login?error=1`);
  }

  clearRateLimit(ip);
  res.setHeader(
    "Set-Cookie",
    `${COOKIE_NAME}=${encodeURIComponent(signSession(u))}; Path=${PANEL_PATH}; HttpOnly; SameSite=Strict; Max-Age=${SESSION_MAX_AGE / 1000}`,
  );
  logger.info(`[panel] Login exitoso desde ${ip}`);
  res.redirect(PANEL_PATH);
});

panelRouter.get("/logout", (_req: Request, res: Response) => {
  res.setHeader(
    "Set-Cookie",
    `${COOKIE_NAME}=; Path=${PANEL_PATH}; HttpOnly; SameSite=Strict; Max-Age=0`,
  );
  res.redirect(`${PANEL_PATH}/login`);
});

// ─── Session middleware ──────────────────────────────────────────────────────

panelRouter.use((req: Request, res: Response, next: NextFunction) => {
  const cookies = parseCookies(req.headers.cookie);
  if (!verifySession(cookies[COOKIE_NAME])) return res.redirect(`${PANEL_PATH}/login`);
  next();
});

function requireCsrf(req: Request, res: Response, next: NextFunction) {
  if (!validateCsrfToken(req.header("X-CSRF-Token") || ""))
    return res.status(403).json({ ok: false, error: "Token CSRF invalido o expirado. Recarga la pagina." });
  next();
}

// ─── HTML page ───────────────────────────────────────────────────────────────

panelRouter.get("/", (_req: Request, res: Response) => {
  res.type("html").send(getAdminHtml(generateCsrfToken()));
});

// ─── JSON API ────────────────────────────────────────────────────────────────

panelRouter.get("/api/apps", async (_req: Request, res: Response) => {
  try {
    const apps = await listApplications();
    res.json({
      ok: true,
      apps: apps.map((a) => ({
        code: a.code, name: a.name, is_active: a.is_active, created_at: (a as any).created_at,
      })),
    });
  } catch (e: any) { res.status(500).json({ ok: false, error: e.message }); }
});

panelRouter.get("/api/companies", async (_req: Request, res: Response) => {
  try {
    const companies = await listAllCompaniesWithApp();
    res.json({ ok: true, companies });
  } catch (e: any) { res.status(500).json({ ok: false, error: e.message }); }
});

panelRouter.post("/api/apps", requireCsrf, async (req: Request, res: Response) => {
  try {
    const { code, name } = req.body || {};
    if (!code || !name)
      return res.status(400).json({ ok: false, error: "Se requieren 'code' y 'name'" });
    const result = await AuthService.register(String(code), String(name));
    logger.info(`[panel] App registrada: ${result.app.code}`);
    res.status(201).json({ ok: true, app: { code: result.app.code, name: result.app.name }, app_secret: result.appSecret });
  } catch (e: any) {
    res.status(e.message?.includes("ya existe") ? 409 : 500).json({ ok: false, error: e.message });
  }
});

panelRouter.patch("/api/apps/:code/status", requireCsrf, async (req: Request, res: Response) => {
  try {
    const { is_active } = req.body || {};
    if (typeof is_active !== "boolean")
      return res.status(400).json({ ok: false, error: "Se requiere 'is_active' (boolean)" });
    const app = await getApplicationByCode(req.params.code);
    if (!app) return res.status(404).json({ ok: false, error: "Aplicacion no encontrada" });
    await setApplicationActive(app.id, is_active);
    logger.info(`[panel] App ${app.code} ${is_active ? "activada" : "desactivada"}`);
    res.json({ ok: true });
  } catch (e: any) { res.status(500).json({ ok: false, error: e.message }); }
});

panelRouter.delete(
  "/api/apps/:code/companies/:docType/:docNumber",
  requireCsrf,
  async (req: Request, res: Response) => {
    try {
      const app = await getApplicationByCode(req.params.code);
      if (!app) return res.status(404).json({ ok: false, error: "Aplicacion no encontrada" });
      const removed = await removeAppCompany(app.id, req.params.docType, req.params.docNumber);
      if (!removed) return res.status(404).json({ ok: false, error: "Documento no encontrado" });
      logger.info(`[panel] Empresa ${req.params.docType}:${req.params.docNumber} desasociada de ${app.code}`);
      res.json({ ok: true, message: "Documento desasociado" });
    } catch (e: any) { res.status(500).json({ ok: false, error: e.message }); }
  },
);

panelRouter.get("/api/catalog", async (req: Request, res: Response) => {
  try {
    const search = req.query.search ? String(req.query.search) : "";
    const page = Math.max(1, Number(req.query.page) || 1);
    const limit = 50;
    const offset = (page - 1) * limit;
    const where = search ? `WHERE alias ILIKE $1 OR name ILIKE $1` : "";
    const params = search ? [`%${search}%`, limit, offset] : [limit, offset];
    const dataRes = await pool.query(
      `SELECT id, alias, name, segmentation, term_value, term_unit, base, unit, source, is_active
       FROM indicator ${where} ORDER BY alias, name LIMIT $${search ? 2 : 1} OFFSET $${search ? 3 : 2}`,
      params,
    );
    const countRes = await pool.query(
      `SELECT COUNT(*) FROM indicator ${where}`,
      search ? [`%${search}%`] : [],
    );
    res.json({ ok: true, rows: dataRes.rows, total: Number(countRes.rows[0].count), page, limit });
  } catch (e: any) { res.status(500).json({ ok: false, error: e.message }); }
});

panelRouter.get("/api/historic", async (req: Request, res: Response) => {
  try {
    const alias = req.query.alias ? String(req.query.alias).toUpperCase() : "";
    const page = Math.max(1, Number(req.query.page) || 1);
    const limit = 100;
    const offset = (page - 1) * limit;
    const where = alias ? `WHERE i.alias = $1` : "";
    const params = alias ? [alias, limit, offset] : [limit, offset];
    const dataRes = await pool.query(
      `SELECT ih.id, i.alias, i.name, i.segmentation, ih.from_date, ih.to_date, ih.value, ih.periodicity, ih.status
       FROM indicator_historic ih
       JOIN indicator i ON i.id = ih.indicator_id
       ${where}
       ORDER BY ih.from_date DESC, i.alias
       LIMIT $${alias ? 2 : 1} OFFSET $${alias ? 3 : 2}`,
      params,
    );
    const countRes = await pool.query(
      `SELECT COUNT(*) FROM indicator_historic ih JOIN indicator i ON i.id = ih.indicator_id ${where}`,
      alias ? [alias] : [],
    );
    res.json({ ok: true, rows: dataRes.rows, total: Number(countRes.rows[0].count), page, limit });
  } catch (e: any) { res.status(500).json({ ok: false, error: e.message }); }
});

panelRouter.get("/api/damodaran", async (_req: Request, res: Response) => {
  try {
    const dbRows = await listDamodaranDatasets();
    const datasets = DAMODARAN_DATASETS.map((d) => {
      const found = dbRows.find((r) => r.dataset === d.dataset);
      return { dataset: d.dataset, name: d.name, last_update: found?.last_update ?? null, industries: found?.industries ?? 0 };
    });
    res.json({ ok: true, datasets });
  } catch (e: any) { res.status(500).json({ ok: false, error: e.message }); }
});

panelRouter.get("/api/damodaran/:dataset", async (req: Request, res: Response) => {
  try {
    const { dataset } = req.params;
    const industry = req.query.industry ? String(req.query.industry) : undefined;
    const page = Math.max(1, Number(req.query.page) || 1);
    const limit = 100;
    const offset = (page - 1) * limit;
    const latest = await pool.query(
      `SELECT MAX(update_date)::text AS date FROM damodaran_industry WHERE dataset = $1`,
      [dataset],
    );
    const latestDate = latest.rows[0]?.date;
    if (!latestDate) return res.json({ ok: true, rows: [], total: 0, page, limit, dataset, update_date: null });

    const where = industry ? `AND industry_name ILIKE $3` : "";
    const params = industry ? [dataset, latestDate, `%${industry}%`, limit, offset] : [dataset, latestDate, limit, offset];
    const dataRes = await pool.query(
      `SELECT industry_name, metrics FROM damodaran_industry
       WHERE dataset = $1 AND update_date = $2 ${where}
       ORDER BY industry_name
       LIMIT $${industry ? 4 : 3} OFFSET $${industry ? 5 : 4}`,
      params,
    );
    const countRes = await pool.query(
      `SELECT COUNT(*) FROM damodaran_industry WHERE dataset = $1 AND update_date = $2 ${where}`,
      industry ? [dataset, latestDate, `%${industry}%`] : [dataset, latestDate],
    );
    res.json({ ok: true, dataset, update_date: latestDate, rows: dataRes.rows, total: Number(countRes.rows[0].count), page, limit });
  } catch (e: any) { res.status(500).json({ ok: false, error: e.message }); }
});

// ─── HTML templates ──────────────────────────────────────────────────────────

function getLoginHtml(error?: string): string {
  const errorHtml = error
    ? `<div class="login-error">${error.replace(/</g, "&lt;")}</div>`
    : "";
  return `<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow">
<title>Login &mdash; Admin Panel</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0f1117;--surface:#1a1d27;--border:#2e3348;
  --text:#e4e6f0;--text2:#9498b0;--accent:#6c72cb;--accent-h:#8187e0;
  --red:#f87171;--radius:8px;
  --font:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
}
body{font-family:var(--font);background:var(--bg);color:var(--text);
  min-height:100vh;display:flex;align-items:center;justify-content:center}
.login-card{
  background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);
  padding:32px;width:100%;max-width:380px;box-shadow:0 4px 24px rgba(0,0,0,.35);
}
.login-card h1{font-size:1.1rem;font-weight:600;margin-bottom:4px;text-align:center}
.login-card .sub{font-size:.8rem;color:var(--text2);text-align:center;margin-bottom:24px}
.login-error{
  background:rgba(248,113,113,.12);border:1px solid var(--red);color:var(--red);
  padding:8px 12px;border-radius:6px;font-size:.82rem;margin-bottom:16px;text-align:center;
}
.field{margin-bottom:16px}
.field label{display:block;font-size:.8rem;color:var(--text2);margin-bottom:5px;font-weight:500}
.field input{
  width:100%;padding:10px 12px;border-radius:6px;
  border:1px solid var(--border);background:var(--bg);color:var(--text);
  font-size:.9rem;font-family:var(--font);transition:border-color .15s;
}
.field input:focus{outline:none;border-color:var(--accent)}
.btn-login{
  width:100%;padding:10px;border-radius:6px;border:none;
  background:var(--accent);color:#fff;font-size:.9rem;font-weight:600;
  cursor:pointer;font-family:var(--font);transition:background .15s;
}
.btn-login:hover{background:var(--accent-h)}
.lock{text-align:center;font-size:2rem;margin-bottom:12px;opacity:.6}
</style>
</head>
<body>
<form class="login-card" method="POST" action="/panel/login" autocomplete="off">
  <div class="lock">&#x1f512;</div>
  <h1>Admin Panel</h1>
  <div class="sub">API Indicators</div>
  ${errorHtml}
  <div class="field">
    <label for="user">Usuario</label>
    <input id="user" name="user" type="text" required autofocus autocomplete="off">
  </div>
  <div class="field">
    <label for="pass">Contrasena</label>
    <input id="pass" name="pass" type="password" required autocomplete="off">
  </div>
  <button type="submit" class="btn-login">Ingresar</button>
</form>
</body>
</html>`;
}

function getAdminHtml(csrfToken: string): string {
  const safeCsrf = csrfToken.replace(/'/g, "\\'");
  return `<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow">
<title>Admin Panel &mdash; API Indicators</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0f1117;--surface:#1a1d27;--surface2:#242836;--border:#2e3348;
  --text:#e4e6f0;--text2:#9498b0;--accent:#6c72cb;--accent-h:#8187e0;
  --green:#34d399;--green-bg:rgba(52,211,153,.12);
  --red:#f87171;--red-bg:rgba(248,113,113,.12);
  --yellow:#fbbf24;--yellow-bg:rgba(251,191,36,.12);
  --radius:8px;--radius-sm:6px;
  --shadow:0 4px 24px rgba(0,0,0,.35);
  --font:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Oxygen,Ubuntu,sans-serif;
  --mono:'SF Mono',SFMono-Regular,Consolas,'Liberation Mono',Menlo,monospace;
}
html{font-size:14px}
body{font-family:var(--font);background:var(--bg);color:var(--text);min-height:100vh}

.header{
  background:var(--surface);border-bottom:1px solid var(--border);
  padding:0 24px;height:56px;display:flex;align-items:center;gap:16px;
  position:sticky;top:0;z-index:100;
}
.header h1{font-size:1.1rem;font-weight:600;letter-spacing:-.3px}
.header .lock{opacity:.5;font-size:1.2rem}
.header .env-badge{
  margin-left:auto;font-size:.75rem;padding:3px 10px;border-radius:12px;
  background:var(--accent);color:#fff;font-weight:600;letter-spacing:.5px;
}

.container{max-width:1100px;margin:0 auto;padding:24px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:24px}
@media(max-width:860px){.grid{grid-template-columns:1fr}}

.card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius)}
.card-head{padding:16px 20px;display:flex;align-items:center;gap:12px;border-bottom:1px solid var(--border)}
.card-head h2{font-size:.95rem;font-weight:600}
.card-head .count{font-size:.75rem;background:var(--surface2);padding:2px 8px;border-radius:10px;color:var(--text2)}
.card-body{padding:16px 20px}
.card-body.no-pad{padding:0;overflow-x:auto}

.btn{
  display:inline-flex;align-items:center;gap:6px;padding:7px 14px;
  border-radius:var(--radius-sm);border:1px solid var(--border);
  background:var(--surface2);color:var(--text);font-size:.8rem;
  cursor:pointer;transition:all .15s;font-family:var(--font);
}
.btn:hover{border-color:var(--accent);color:var(--accent-h)}
.btn-primary{background:var(--accent);border-color:var(--accent);color:#fff}
.btn-primary:hover{background:var(--accent-h)}
.btn-sm{padding:4px 10px;font-size:.75rem}
.btn-danger{color:var(--red);border-color:transparent}
.btn-danger:hover{background:var(--red-bg);border-color:var(--red)}
.btn-success{color:var(--green);border-color:transparent}
.btn-success:hover{background:var(--green-bg);border-color:var(--green)}

table{width:100%;border-collapse:collapse;font-size:.85rem}
th{text-align:left;padding:10px 16px;color:var(--text2);font-weight:500;
   font-size:.75rem;text-transform:uppercase;letter-spacing:.5px;white-space:nowrap;
   background:var(--surface2);border-bottom:1px solid var(--border)}
td{padding:10px 16px;border-bottom:1px solid var(--border)}
tr:last-child td{border-bottom:none}
tr:hover td{background:rgba(108,114,203,.04)}
.empty-row td{text-align:center;color:var(--text2);padding:32px 16px}

.badge{display:inline-flex;align-items:center;gap:4px;padding:2px 10px;border-radius:10px;font-size:.75rem;font-weight:600}
.badge-active{background:var(--green-bg);color:var(--green)}
.badge-inactive{background:var(--red-bg);color:var(--red)}

.modal-overlay{
  position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:200;
  display:none;align-items:center;justify-content:center;backdrop-filter:blur(4px);
}
.modal-overlay.open{display:flex}
.modal{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);width:90%;max-width:460px;box-shadow:var(--shadow)}
.modal-head{padding:16px 20px;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
.modal-head h3{font-size:.95rem;font-weight:600}
.modal-close{background:none;border:none;color:var(--text2);font-size:1.2rem;cursor:pointer;padding:4px;line-height:1}
.modal-close:hover{color:var(--text)}
.modal-body{padding:20px}
.modal-footer{padding:12px 20px;border-top:1px solid var(--border);display:flex;justify-content:flex-end;gap:8px}

.field{margin-bottom:14px}
.field label{display:block;font-size:.8rem;color:var(--text2);margin-bottom:5px;font-weight:500}
.field input,.field select{
  width:100%;padding:8px 12px;border-radius:var(--radius-sm);
  border:1px solid var(--border);background:var(--bg);color:var(--text);
  font-size:.85rem;font-family:var(--font);transition:border-color .15s;
}
.field input:focus,.field select:focus{outline:none;border-color:var(--accent)}
.field .hint{font-size:.72rem;color:var(--text2);margin-top:3px}

.toast-container{position:fixed;top:68px;right:20px;z-index:300;display:flex;flex-direction:column;gap:8px}
.toast{padding:10px 16px;border-radius:var(--radius-sm);font-size:.82rem;box-shadow:var(--shadow);animation:slideIn .25s ease;max-width:360px;border:1px solid var(--border)}
.toast-success{background:#0d3326;border-color:var(--green);color:var(--green)}
.toast-error{background:#3b1c1c;border-color:var(--red);color:var(--red)}
@keyframes slideIn{from{opacity:0;transform:translateX(30px)}to{opacity:1;transform:none}}

.secret-box{background:var(--bg);border:1px solid var(--yellow);border-radius:var(--radius-sm);padding:12px;margin-top:12px}
.secret-box .warn{color:var(--yellow);font-size:.8rem;font-weight:600;margin-bottom:8px}
.secret-box code{display:block;font-family:var(--mono);font-size:.82rem;word-break:break-all;color:var(--text);background:var(--surface2);padding:8px;border-radius:4px;cursor:pointer}
.secret-box .copy-hint{font-size:.7rem;color:var(--text2);margin-top:6px}

.spinner{display:inline-block;width:16px;height:16px;border:2px solid var(--border);border-top-color:var(--accent);border-radius:50%;animation:spin .6s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}

.app-row{cursor:pointer}
.app-row.active{background:rgba(108,114,203,.08)}

.tabs-nav{
  background:var(--surface);border-bottom:1px solid var(--border);
  padding:0 24px;display:flex;gap:4px;
}
.tab-btn{
  padding:12px 16px;border:none;background:none;color:var(--text2);
  font-size:.85rem;font-family:var(--font);cursor:pointer;
  border-bottom:2px solid transparent;margin-bottom:-1px;transition:all .15s;
}
.tab-btn:hover{color:var(--text)}
.tab-btn.active{color:var(--accent);border-bottom-color:var(--accent);font-weight:600}

.tab-content{display:none}
.tab-content.active{display:block}

.tbl-toolbar{display:flex;align-items:center;gap:10px;padding:12px 20px;border-bottom:1px solid var(--border)}
.tbl-toolbar input{
  flex:1;max-width:280px;padding:7px 12px;border-radius:var(--radius-sm);
  border:1px solid var(--border);background:var(--bg);color:var(--text);
  font-size:.82rem;font-family:var(--font);
}
.tbl-toolbar input:focus{outline:none;border-color:var(--accent)}
.tbl-toolbar .total{font-size:.78rem;color:var(--text2);margin-left:auto}
.pagination{display:flex;align-items:center;gap:8px;padding:12px 20px;border-top:1px solid var(--border)}
.pagination button{padding:4px 10px;font-size:.78rem;border-radius:4px;border:1px solid var(--border);background:var(--surface2);color:var(--text);cursor:pointer}
.pagination button:disabled{opacity:.4;cursor:default}
.pagination .page-info{font-size:.78rem;color:var(--text2)}
.endpoint-cell{font-family:var(--mono);font-size:.75rem;color:var(--text2)}
</style>
</head>
<body>

<div class="header">
  <span class="lock">&#x1f512;</span>
  <h1>Admin Panel &mdash; API Indicators</h1>
  <span class="env-badge">ADMIN</span>
  <a href="/panel/logout" class="btn btn-sm" style="margin-left:8px;text-decoration:none">Cerrar sesion</a>
</div>

<div class="tabs-nav">
  <button class="tab-btn active" onclick="switchTab('apps')">Aplicativos</button>
  <button class="tab-btn" onclick="switchTab('catalog')">Catalogo</button>
  <button class="tab-btn" onclick="switchTab('historic')">Historico</button>
  <button class="tab-btn" onclick="switchTab('damodaran')">Damodaran</button>
</div>

<div class="container">

<div class="tab-content active" id="tab-apps">
  <div class="grid">

    <div class="card">
      <div class="card-head">
        <h2>Aplicativos</h2>
        <span class="count" id="apps-count">0</span>
        <span style="flex:1"></span>
        <button class="btn btn-primary btn-sm" onclick="openModal('register')">+ Registrar</button>
      </div>
      <div class="card-body no-pad">
        <table>
          <thead>
            <tr><th>Code</th><th>Nombre</th><th>Estado</th><th style="width:90px">Acciones</th></tr>
          </thead>
          <tbody id="apps-table">
            <tr class="empty-row"><td colspan="4"><span class="spinner"></span> Cargando...</td></tr>
          </tbody>
        </table>
      </div>
    </div>

    <div class="card">
      <div class="card-head">
        <h2>Empresas asociadas</h2>
        <span class="count" id="companies-count">0</span>
        <span style="flex:1"></span>
        <span style="font-size:.72rem;color:var(--text2)">Se asocian automaticamente al consumir el API</span>
      </div>
      <div class="card-body no-pad">
        <table>
          <thead>
            <tr><th>Aplicativo</th><th>Tipo</th><th>Documento</th><th>Nombre</th><th>Estado</th><th style="width:50px"></th></tr>
          </thead>
          <tbody id="companies-table">
            <tr class="empty-row"><td colspan="6"><span class="spinner"></span> Cargando...</td></tr>
          </tbody>
        </table>
      </div>
    </div>

  </div>
</div><!-- /tab-apps -->

<div class="tab-content" id="tab-catalog">
  <div class="card" style="margin-top:0;border-radius:0;border-left:none;border-right:none;border-top:none">
    <div class="card-head">
      <h2>Catalogo de Indicadores</h2>
      <span class="count" id="catalog-count">0</span>
    </div>
    <div class="tbl-toolbar">
      <input id="catalog-search" placeholder="Buscar por alias o nombre..." oninput="searchCatalog()" autocomplete="off">
      <span class="total" id="catalog-total"></span>
    </div>
    <div class="card-body no-pad">
      <table>
        <thead>
          <tr>
            <th>Alias</th><th>Nombre</th><th>Segmentacion</th>
            <th>Plazo</th><th>Unidad</th><th>Fuente</th><th>Estado</th><th>Endpoint</th>
          </tr>
        </thead>
        <tbody id="catalog-table">
          <tr class="empty-row"><td colspan="8"><span class="spinner"></span> Cargando...</td></tr>
        </tbody>
      </table>
    </div>
    <div class="pagination" id="catalog-pagination" style="display:none">
      <button onclick="catalogPage(-1)" id="catalog-prev">&#8592; Anterior</button>
      <span class="page-info" id="catalog-page-info"></span>
      <button onclick="catalogPage(1)" id="catalog-next">Siguiente &#8594;</button>
    </div>
  </div>
</div><!-- /tab-catalog -->

<div class="tab-content" id="tab-historic">
  <div class="card" style="margin-top:0;border-radius:0;border-left:none;border-right:none;border-top:none">
    <div class="card-head">
      <h2>Historico de Indicadores</h2>
      <span class="count" id="historic-count">0</span>
    </div>
    <div class="tbl-toolbar">
      <input id="historic-alias" placeholder="Filtrar por alias (TRM, IBR...)" oninput="filterHistoric()" autocomplete="off" spellcheck="false">
      <span class="total" id="historic-total"></span>
    </div>
    <div class="card-body no-pad">
      <table>
        <thead>
          <tr>
            <th>Alias</th><th>Nombre</th><th>Segmentacion</th>
            <th>Fecha</th><th>Valor</th><th>Periodicidad</th><th>Estado</th>
          </tr>
        </thead>
        <tbody id="historic-table">
          <tr class="empty-row"><td colspan="7">Ingresa un alias para filtrar o deja en blanco para ver todo</td></tr>
        </tbody>
      </table>
    </div>
    <div class="pagination" id="historic-pagination" style="display:none">
      <button onclick="historicPage(-1)" id="historic-prev">&#8592; Anterior</button>
      <span class="page-info" id="historic-page-info"></span>
      <button onclick="historicPage(1)" id="historic-next">Siguiente &#8594;</button>
    </div>
  </div>
</div><!-- /tab-historic -->

<div class="tab-content" id="tab-damodaran">
  <div class="card" style="margin-top:0;border-radius:0;border-left:none;border-right:none;border-top:none">
    <div class="card-head">
      <h2>Damodaran</h2>
      <span class="count" id="damodaran-count">0</span>
      <span style="flex:1"></span>
      <span style="font-size:.72rem;color:var(--text2)" id="damodaran-date"></span>
    </div>
    <div class="tbl-toolbar">
      <select id="damodaran-dataset" onchange="loadDamodaran()" style="max-width:220px;padding:7px 12px;border-radius:var(--radius-sm);border:1px solid var(--border);background:var(--bg);color:var(--text);font-size:.82rem;font-family:var(--font)">
        <option value="">Selecciona dataset...</option>
        <option value="betas-global">betas-global</option>
        <option value="ev-ebitda">ev-ebitda</option>
        <option value="ev-sales">ev-sales</option>
      </select>
      <input id="damodaran-search" placeholder="Filtrar industria..." oninput="searchDamodaran()" autocomplete="off" style="max-width:240px">
      <span class="total" id="damodaran-total"></span>
    </div>
    <div class="card-body no-pad">
      <table id="damodaran-table-wrap">
        <thead id="damodaran-thead"><tr><th colspan="2">Selecciona un dataset</th></tr></thead>
        <tbody id="damodaran-tbody">
          <tr class="empty-row"><td colspan="2">Selecciona un dataset para ver los datos</td></tr>
        </tbody>
      </table>
    </div>
    <div class="pagination" id="damodaran-pagination" style="display:none">
      <button onclick="damodaranPage(-1)" id="damodaran-prev">&#8592; Anterior</button>
      <span class="page-info" id="damodaran-page-info"></span>
      <button onclick="damodaranPage(1)" id="damodaran-next">Siguiente &#8594;</button>
    </div>
  </div>
</div><!-- /tab-damodaran -->

</div><!-- /container -->

<div class="toast-container" id="toasts"></div>

<div class="modal-overlay" id="modal-register">
  <div class="modal">
    <div class="modal-head">
      <h3>Registrar Aplicativo</h3>
      <button class="modal-close" onclick="closeModal('register')">&times;</button>
    </div>
    <div class="modal-body">
      <div id="register-form">
        <div class="field">
          <label>Codigo (identificador unico)</label>
          <input id="reg-code" placeholder="mi-aplicativo" autocomplete="off" spellcheck="false">
          <div class="hint">Minusculas, sin espacios. Ej: contabilidad-app</div>
        </div>
        <div class="field">
          <label>Nombre descriptivo</label>
          <input id="reg-name" placeholder="Mi Aplicativo de Contabilidad" autocomplete="off">
        </div>
      </div>
      <div id="register-result" style="display:none"></div>
    </div>
    <div class="modal-footer" id="register-footer">
      <button class="btn" onclick="closeModal('register')">Cancelar</button>
      <button class="btn btn-primary" id="btn-register" onclick="registerApp()">Registrar</button>
    </div>
  </div>
</div>

<div class="modal-overlay" id="modal-confirm">
  <div class="modal">
    <div class="modal-head">
      <h3 id="confirm-title">Confirmar</h3>
      <button class="modal-close" onclick="closeModal('confirm')">&times;</button>
    </div>
    <div class="modal-body">
      <p id="confirm-msg" style="font-size:.9rem"></p>
    </div>
    <div class="modal-footer">
      <button class="btn" onclick="closeModal('confirm')">Cancelar</button>
      <button class="btn btn-danger" id="btn-confirm" onclick="confirmAction()">Confirmar</button>
    </div>
  </div>
</div>

<script>
(function(){
  'use strict';
  const CSRF = '${safeCsrf}';
  let selectedApp = null;
  let pendingConfirm = null;

  async function api(method, path, body) {
    const opts = { method, headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': CSRF } };
    if (body) opts.body = JSON.stringify(body);
    const res = await fetch('/panel' + path, opts);
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || 'Error desconocido');
    return data;
  }

  function toast(msg, type) {
    const el = document.createElement('div');
    el.className = 'toast toast-' + (type || 'success');
    el.textContent = msg;
    document.getElementById('toasts').appendChild(el);
    setTimeout(function(){ el.remove(); }, 4000);
  }

  function esc(str) {
    var d = document.createElement('div');
    d.appendChild(document.createTextNode(str || ''));
    return d.innerHTML;
  }

  window.openModal = function(name) { document.getElementById('modal-' + name).classList.add('open'); };
  window.closeModal = function(name) {
    document.getElementById('modal-' + name).classList.remove('open');
    if (name === 'register') resetRegisterModal();
  };

  function resetRegisterModal() {
    document.getElementById('register-form').style.display = '';
    document.getElementById('register-result').style.display = 'none';
    document.getElementById('register-result').innerHTML = '';
    document.getElementById('register-footer').style.display = '';
    document.getElementById('reg-code').value = '';
    document.getElementById('reg-name').value = '';
  }

  async function loadApps() {
    try {
      var data = await api('GET', '/api/apps');
      var apps = data.apps || [];
      document.getElementById('apps-count').textContent = apps.length;
      var tb = document.getElementById('apps-table');
      if (!apps.length) {
        tb.innerHTML = '<tr class="empty-row"><td colspan="4">No hay aplicativos registrados</td></tr>';
        return;
      }
      tb.innerHTML = apps.map(function(a) {
        var badge = a.is_active
          ? '<span class="badge badge-active">Activo</span>'
          : '<span class="badge badge-inactive">Inactivo</span>';
        var toggleBtn = a.is_active
          ? '<button class="btn btn-sm btn-danger" onclick="event.stopPropagation();toggleApp(\\''+esc(a.code)+'\\',false)" title="Desactivar">&#x23F8;</button>'
          : '<button class="btn btn-sm btn-success" onclick="event.stopPropagation();toggleApp(\\''+esc(a.code)+'\\',true)" title="Activar">&#x25B6;</button>';
        var cls = selectedApp === a.code ? 'app-row active' : 'app-row';
        return '<tr class="'+cls+'" onclick="selectApp(\\''+esc(a.code)+'\\')">'
          + '<td><code style="font-family:var(--mono);font-size:.82rem">'+esc(a.code)+'</code></td>'
          + '<td>'+esc(a.name)+'</td>'
          + '<td>'+badge+'</td>'
          + '<td>'+toggleBtn+'</td>'
          + '</tr>';
      }).join('');
    } catch(e) { toast(e.message, 'error'); }
  }

  window.selectApp = function(code) {
    selectedApp = (selectedApp === code) ? null : code;
    loadApps();
    loadCompanies();
  };

  async function loadCompanies() {
    var tb = document.getElementById('companies-table');
    tb.innerHTML = '<tr class="empty-row"><td colspan="6"><span class="spinner"></span> Cargando...</td></tr>';
    try {
      var data = await api('GET', '/api/companies');
      var comps = data.companies || [];
      var filtered = selectedApp ? comps.filter(function(c){ return c.app_code === selectedApp; }) : comps;
      document.getElementById('companies-count').textContent = filtered.length;
      if (!filtered.length) {
        var msg = selectedApp ? 'Sin empresas para ' + esc(selectedApp) : 'No hay empresas asociadas aun';
        tb.innerHTML = '<tr class="empty-row"><td colspan="6">' + msg + '</td></tr>';
        return;
      }
      tb.innerHTML = filtered.map(function(c) {
        var badge = c.is_active
          ? '<span class="badge badge-active">Activa</span>'
          : '<span class="badge badge-inactive">Inactiva</span>';
        var rmBtn = c.is_active
          ? '<button class="btn btn-sm btn-danger" onclick="confirmRemove(\\''+esc(c.app_code)+'\\',\\''+esc(c.doc_type)+'\\',\\''+esc(c.company_doc_number)+'\\',\\''+esc(c.company_name)+'\\')" title="Desasociar">&#x1D5EB;</button>'
          : '';
        return '<tr>'
          + '<td><code style="font-family:var(--mono);font-size:.82rem">'+esc(c.app_code)+'</code></td>'
          + '<td><code>'+esc(c.doc_type)+'</code></td>'
          + '<td><code style="font-family:var(--mono)">'+esc(c.company_doc_number)+'</code></td>'
          + '<td>'+esc(c.company_name)+'</td>'
          + '<td>'+badge+'</td>'
          + '<td>'+rmBtn+'</td>'
          + '</tr>';
      }).join('');
    } catch(e) { toast(e.message, 'error'); }
  }

  window.registerApp = async function() {
    var code = document.getElementById('reg-code').value.trim();
    var name = document.getElementById('reg-name').value.trim();
    if (!code || !name) { toast('Completa todos los campos', 'error'); return; }
    var btn = document.getElementById('btn-register');
    btn.disabled = true; btn.textContent = 'Registrando...';
    try {
      var data = await api('POST', '/api/apps', { code: code, name: name });
      document.getElementById('register-form').style.display = 'none';
      document.getElementById('register-footer').style.display = 'none';
      var result = document.getElementById('register-result');
      result.style.display = '';
      result.innerHTML =
        '<p style="color:var(--green);font-weight:600;margin-bottom:8px">'
        + 'Aplicativo <code>' + esc(data.app.code) + '</code> registrado exitosamente.'
        + '</p>'
        + '<div class="secret-box">'
        + '<div class="warn">&#x26A0; IMPORTANTE: Copia el app_secret ahora. No se puede recuperar.</div>'
        + '<code onclick="copySecret(this)" title="Click para copiar">' + esc(data.app_secret) + '</code>'
        + '<div class="copy-hint">Click en el secret para copiarlo al portapapeles</div>'
        + '</div>'
        + '<div style="margin-top:16px;text-align:right">'
        + '<button class="btn btn-primary" onclick="closeModal(\\'register\\')">Cerrar</button>'
        + '</div>';
      loadApps();
      toast('Aplicativo registrado', 'success');
    } catch(e) {
      toast(e.message, 'error');
    } finally {
      btn.disabled = false; btn.textContent = 'Registrar';
    }
  };

  window.copySecret = function(el) {
    navigator.clipboard.writeText(el.textContent).then(function(){
      toast('Secret copiado al portapapeles', 'success');
    });
  };

  window.toggleApp = async function(code, active) {
    try {
      await api('PATCH', '/api/apps/' + encodeURIComponent(code) + '/status', { is_active: active });
      toast('Aplicativo ' + code + (active ? ' activado' : ' desactivado'), 'success');
      loadApps();
    } catch(e) { toast(e.message, 'error'); }
  };

  window.confirmRemove = function(appCode, docType, docNum, compName) {
    document.getElementById('confirm-title').textContent = 'Desasociar Empresa';
    document.getElementById('confirm-msg').innerHTML =
      'Desasociar <strong>' + esc(compName) + '</strong> (' + esc(docType) + ':' + esc(docNum) + ') de <strong>' + esc(appCode) + '</strong>?';
    pendingConfirm = function(){ removeCompany(appCode, docType, docNum); };
    openModal('confirm');
  };

  window.confirmAction = function() {
    closeModal('confirm');
    if (pendingConfirm) { pendingConfirm(); pendingConfirm = null; }
  };

  async function removeCompany(appCode, docType, docNum) {
    try {
      await api('DELETE', '/api/apps/' + encodeURIComponent(appCode) + '/companies/'
        + encodeURIComponent(docType) + '/' + encodeURIComponent(docNum));
      toast('Empresa desasociada', 'success');
      loadCompanies();
    } catch(e) { toast(e.message, 'error'); }
  }

  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') document.querySelectorAll('.modal-overlay.open').forEach(function(m){ m.classList.remove('open'); });
  });
  document.querySelectorAll('.modal-overlay').forEach(function(overlay) {
    overlay.addEventListener('click', function(e) { if (e.target === overlay) overlay.classList.remove('open'); });
  });

  loadApps();
  loadCompanies();

  // ── Tabs ────────────────────────────────────────────────────────────────────
  window.switchTab = function(name) {
    document.querySelectorAll('.tab-content').forEach(function(el){ el.classList.remove('active'); });
    document.querySelectorAll('.tab-btn').forEach(function(el){ el.classList.remove('active'); });
    document.getElementById('tab-' + name).classList.add('active');
    document.querySelector('.tab-btn[onclick*="\\'' + name + '\\'"]').classList.add('active');
    if (name === 'catalog' && !catalogLoaded) loadCatalog();
    if (name === 'historic' && !historicLoaded) loadHistoric();
    if (name === 'damodaran' && !damodaranLoaded) initDamodaran();
  };

  // ── Catalogo ────────────────────────────────────────────────────────────────
  var catalogLoaded = false;
  var catalogCurrentPage = 1;
  var catalogSearchTimer = null;

  window.searchCatalog = function() {
    clearTimeout(catalogSearchTimer);
    catalogSearchTimer = setTimeout(function(){ catalogCurrentPage = 1; loadCatalog(); }, 300);
  };

  window.catalogPage = function(dir) {
    catalogCurrentPage += dir;
    loadCatalog();
  };

  async function loadCatalog() {
    catalogLoaded = true;
    var search = document.getElementById('catalog-search').value.trim();
    var tb = document.getElementById('catalog-table');
    tb.innerHTML = '<tr class="empty-row"><td colspan="8"><span class="spinner"></span></td></tr>';
    try {
      var url = '/panel/api/catalog?page=' + catalogCurrentPage + (search ? '&search=' + encodeURIComponent(search) : '');
      var data = await fetch(url).then(function(r){ return r.json(); });
      var rows = data.rows || [];
      var total = data.total || 0;
      var pages = Math.ceil(total / data.limit) || 1;
      document.getElementById('catalog-count').textContent = rows.length;
      document.getElementById('catalog-total').textContent = total + ' indicadores';
      document.getElementById('catalog-page-info').textContent = 'Pag ' + data.page + ' / ' + pages;
      document.getElementById('catalog-prev').disabled = data.page <= 1;
      document.getElementById('catalog-next').disabled = data.page >= pages;
      document.getElementById('catalog-pagination').style.display = total > data.limit ? '' : 'none';
      if (!rows.length) {
        tb.innerHTML = '<tr class="empty-row"><td colspan="8">Sin resultados</td></tr>';
        return;
      }
      tb.innerHTML = rows.map(function(r) {
        var badge = r.is_active
          ? '<span class="badge badge-active">Activo</span>'
          : '<span class="badge badge-inactive">Inactivo</span>';
        var plazo = r.term_value ? (r.term_value + ' ' + (r.term_unit || '')) : (r.term_unit || '—');
        var ep = '/indicators/' + esc(r.alias) + '/historic';
        return '<tr>'
          + '<td><code style="font-family:var(--mono);font-size:.82rem;font-weight:600">' + esc(r.alias) + '</code></td>'
          + '<td>' + esc(r.name) + '</td>'
          + '<td>' + esc(r.segmentation || '—') + '</td>'
          + '<td>' + esc(plazo) + '</td>'
          + '<td>' + esc(r.unit || '—') + '</td>'
          + '<td style="font-size:.78rem">' + esc(r.source || '—') + '</td>'
          + '<td>' + badge + '</td>'
          + '<td class="endpoint-cell" title="' + ep + '">' + ep + '</td>'
          + '</tr>';
      }).join('');
    } catch(e) { toast(e.message, 'error'); }
  }

  // ── Historico ───────────────────────────────────────────────────────────────
  var historicLoaded = false;
  var historicCurrentPage = 1;
  var historicTimer = null;

  window.filterHistoric = function() {
    clearTimeout(historicTimer);
    historicTimer = setTimeout(function(){ historicCurrentPage = 1; loadHistoric(); }, 400);
  };

  window.historicPage = function(dir) {
    historicCurrentPage += dir;
    loadHistoric();
  };

  async function loadHistoric() {
    historicLoaded = true;
    var alias = document.getElementById('historic-alias').value.trim();
    var tb = document.getElementById('historic-table');
    tb.innerHTML = '<tr class="empty-row"><td colspan="7"><span class="spinner"></span></td></tr>';
    try {
      var url = '/panel/api/historic?page=' + historicCurrentPage + (alias ? '&alias=' + encodeURIComponent(alias) : '');
      var data = await fetch(url).then(function(r){ return r.json(); });
      var rows = data.rows || [];
      var total = data.total || 0;
      var pages = Math.ceil(total / data.limit) || 1;
      document.getElementById('historic-count').textContent = rows.length;
      document.getElementById('historic-total').textContent = total.toLocaleString() + ' registros';
      document.getElementById('historic-page-info').textContent = 'Pag ' + data.page + ' / ' + pages;
      document.getElementById('historic-prev').disabled = data.page <= 1;
      document.getElementById('historic-next').disabled = data.page >= pages;
      document.getElementById('historic-pagination').style.display = total > 0 ? '' : 'none';
      if (!rows.length) {
        tb.innerHTML = '<tr class="empty-row"><td colspan="7">' + (alias ? 'Sin registros para ' + esc(alias) : 'Sin datos') + '</td></tr>';
        return;
      }
      tb.innerHTML = rows.map(function(r) {
        var fecha = r.from_date ? String(r.from_date).slice(0, 10) : '—';
        return '<tr>'
          + '<td><code style="font-family:var(--mono);font-size:.82rem;font-weight:600">' + esc(r.alias) + '</code></td>'
          + '<td style="font-size:.8rem">' + esc(r.name) + '</td>'
          + '<td style="font-size:.8rem">' + esc(r.segmentation || '—') + '</td>'
          + '<td style="font-family:var(--mono);font-size:.82rem">' + esc(fecha) + '</td>'
          + '<td style="font-family:var(--mono);font-size:.82rem;text-align:right">' + esc(r.value || '—') + '</td>'
          + '<td>' + esc(r.periodicity || '—') + '</td>'
          + '<td><span class="badge ' + (r.status === 'definitive' ? 'badge-active' : 'badge-inactive') + '">' + esc(r.status || '') + '</span></td>'
          + '</tr>';
      }).join('');
    } catch(e) { toast(e.message, 'error'); }
  }

  // ── Damodaran ────────────────────────────────────────────────────────────────
  var damodaranLoaded = false;
  var damodaranCurrentPage = 1;
  var damodaranSearchTimer = null;
  var damodaranMetricKeys = [];

  function initDamodaran() {
    damodaranLoaded = true;
    // Populate dataset selector with actual data from API
    fetch('/panel/api/damodaran').then(function(r){ return r.json(); }).then(function(data) {
      var sel = document.getElementById('damodaran-dataset');
      sel.innerHTML = '<option value="">Selecciona dataset...</option>';
      (data.datasets || []).forEach(function(ds) {
        var opt = document.createElement('option');
        opt.value = ds.dataset;
        var info = ds.industries ? ' — ' + ds.industries + ' industrias' : ' — sin datos';
        opt.textContent = (ds.name || ds.dataset) + info;
        sel.appendChild(opt);
      });
    }).catch(function(){});
  }

  window.loadDamodaran = function() {
    damodaranCurrentPage = 1;
    document.getElementById('damodaran-search').value = '';
    fetchDamodaran();
  };

  window.searchDamodaran = function() {
    clearTimeout(damodaranSearchTimer);
    damodaranSearchTimer = setTimeout(function(){ damodaranCurrentPage = 1; fetchDamodaran(); }, 300);
  };

  window.damodaranPage = function(dir) {
    damodaranCurrentPage += dir;
    fetchDamodaran();
  };

  async function fetchDamodaran() {
    var dataset = document.getElementById('damodaran-dataset').value;
    if (!dataset) return;
    var search = document.getElementById('damodaran-search').value.trim();
    var tbody = document.getElementById('damodaran-tbody');
    tbody.innerHTML = '<tr class="empty-row"><td colspan="10"><span class="spinner"></span></td></tr>';
    try {
      var url = '/panel/api/damodaran/' + encodeURIComponent(dataset) + '?page=' + damodaranCurrentPage;
      if (search) url += '&industry=' + encodeURIComponent(search);
      var data = await fetch(url).then(function(r){ return r.json(); });
      var rows = data.rows || [];
      var total = data.total || 0;
      var pages = Math.ceil(total / data.limit) || 1;

      document.getElementById('damodaran-count').textContent = rows.length;
      document.getElementById('damodaran-total').textContent = total + ' industrias';
      document.getElementById('damodaran-date').textContent = data.update_date ? 'Actualizado: ' + data.update_date : '';
      document.getElementById('damodaran-page-info').textContent = 'Pag ' + data.page + ' / ' + pages;
      document.getElementById('damodaran-prev').disabled = data.page <= 1;
      document.getElementById('damodaran-next').disabled = data.page >= pages;
      document.getElementById('damodaran-pagination').style.display = total > data.limit ? '' : 'none';

      if (!rows.length) {
        document.getElementById('damodaran-thead').innerHTML = '<tr><th>Industria</th></tr>';
        tbody.innerHTML = '<tr class="empty-row"><td>' + (search ? 'Sin resultados para "' + esc(search) + '"' : 'Sin datos — ejecuta el job primero') + '</td></tr>';
        return;
      }

      // Build dynamic columns from first row metrics
      var metricKeys = Object.keys(rows[0].metrics || {});
      document.getElementById('damodaran-thead').innerHTML =
        '<tr><th>Industria</th>' + metricKeys.map(function(k){ return '<th style="text-align:right">' + esc(k.replace(/_/g,' ').toUpperCase()) + '</th>'; }).join('') + '</tr>';

      tbody.innerHTML = rows.map(function(r) {
        var metrics = r.metrics || {};
        return '<tr>'
          + '<td style="font-size:.82rem;min-width:180px">' + esc(r.industry_name) + '</td>'
          + metricKeys.map(function(k){
              var v = metrics[k];
              return '<td style="font-family:var(--mono);font-size:.8rem;text-align:right;min-width:120px">'
                + (v !== null && v !== undefined ? Number(v).toFixed(4) : '—') + '</td>';
            }).join('')
          + '</tr>';
      }).join('');
    } catch(e) { toast(e.message, 'error'); }
  }

})();
</script>
</body>
</html>`;
}
