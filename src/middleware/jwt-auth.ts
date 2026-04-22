import type { Request, Response, NextFunction } from "express";
import { env } from "../config/env";
import crypto from "crypto";
import { AuthService } from "../services/auth.service";
import { getApplicationById } from "../db/app.repo";
import type { AuthenticatedApp } from "../models/app.model";

declare global {
  namespace Express {
    interface Request {
      app_auth?: AuthenticatedApp;
    }
  }
}

const PUBLIC_PATHS = ["/health", "/auth/token", "/token"];

function isLegacyApiKeyValid(key: string): boolean {
  if (!env.apiKey) return false;
  const expected = Buffer.from(env.apiKey);
  const provided = Buffer.from(key);
  return (
    expected.length === provided.length &&
    crypto.timingSafeEqual(expected, provided)
  );
}

export function jwtAuthMiddleware(
  req: Request,
  res: Response,
  next: NextFunction,
) {
  if (PUBLIC_PATHS.includes(req.path)) {
    return next();
  }

  const authHeader = req.headers.authorization || "";
  const bearerToken = authHeader.startsWith("Bearer ")
    ? authHeader.slice(7).trim()
    : "";

  // ── Path 1: JWT Bearer token ───────────────────────────────────────────
  if (bearerToken) {
    let payload;
    try {
      payload = AuthService.verifyToken(bearerToken);
    } catch (err: unknown) {
      const message =
        (err as any)?.name === "TokenExpiredError"
          ? "Token expirado"
          : "Token invalido";
      return res.status(401).json({ ok: false, error: message });
    }

    getApplicationById(payload.app_id)
      .then((app) => {
        if (!app || !app.is_active) {
          return res
            .status(403)
            .json({ ok: false, error: "Aplicacion desactivada" });
        }

        req.app_auth = {
          appId: payload.app_id,
          appCode: payload.sub,
          companies: payload.companies,
        };
        next();
      })
      .catch(() =>
        res
          .status(500)
          .json({ ok: false, error: "Error verificando aplicacion" }),
      );

    return;
  }

  // ── Path 2: Legacy API Key ─────────────────────────────────────────────
  const legacyKey =
    (req.header("x-api-key") || (req.query.api_key as string)) ?? "";

  if (!legacyKey && !bearerToken) {
    return res
      .status(401)
      .json({ ok: false, error: "Autenticacion requerida (Bearer token o X-Api-Key)" });
  }

  if (legacyKey && isLegacyApiKeyValid(legacyKey)) {
    return next();
  }

  return res.status(403).json({ ok: false, error: "Credenciales invalidas" });
}
