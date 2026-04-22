import bcrypt from "bcrypt";
import jwt from "jsonwebtoken";
import crypto from "crypto";
import { env } from "../config/env";
import {
  createApplication,
  getApplicationByCode,
  getActiveCompanyDocNumbers,
} from "../db/app.repo";
import type { JwtPayload, ErpApplicationPublic } from "../models/app.model";

const BCRYPT_ROUNDS = 12;

const TIME_UNITS: Record<string, number> = {
  s: 1, m: 60, h: 3600, d: 86400,
};

function parseExpiresIn(value: string): number {
  const match = value.match(/^(\d+)\s*([smhd])$/i);
  if (match) {
    return Number(match[1]) * (TIME_UNITS[match[2].toLowerCase()] || 1);
  }
  const num = Number(value);
  return Number.isFinite(num) ? num : 3600;
}

function ensureJwtSecret(): string {
  if (!env.jwt.secret) {
    throw new Error(
      "JWT_SECRET no esta configurado. Define la variable de entorno JWT_SECRET.",
    );
  }
  return env.jwt.secret;
}

function generateSecret(): string {
  return crypto.randomBytes(32).toString("base64url");
}

export const AuthService = {
  async register(
    code: string,
    name: string,
  ): Promise<{ app: ErpApplicationPublic; appSecret: string }> {
    const normalized = code.trim().toLowerCase();
    if (!normalized || !name.trim()) {
      throw new Error("code y name son requeridos");
    }

    const existing = await getApplicationByCode(normalized);
    if (existing) {
      throw new Error(`La aplicacion '${normalized}' ya existe`);
    }

    const appSecret = generateSecret();
    const secretHash = await bcrypt.hash(appSecret, BCRYPT_ROUNDS);
    const app = await createApplication(normalized, name.trim(), secretHash);

    return {
      app: {
        id: app.id,
        code: app.code,
        name: app.name,
        is_active: app.is_active,
      },
      appSecret,
    };
  },

  async login(
    code: string,
    appSecret: string,
  ): Promise<{ token: string; expiresIn: string }> {
    const secret = ensureJwtSecret();
    const normalized = code.trim().toLowerCase();

    const app = await getApplicationByCode(normalized);
    if (!app) throw new Error("Credenciales invalidas");
    if (!app.is_active) throw new Error("Aplicacion desactivada");

    const valid = await bcrypt.compare(appSecret, app.secret_hash);
    if (!valid) throw new Error("Credenciales invalidas");

    const companies = await getActiveCompanyDocNumbers(app.id);

    const payload: JwtPayload = {
      sub: app.code,
      app_id: app.id,
      companies,
    };

    const expiresInSeconds = parseExpiresIn(env.jwt.expiresIn);
    const token = jwt.sign(payload, secret, { expiresIn: expiresInSeconds });

    return { token, expiresIn: env.jwt.expiresIn };
  },

  verifyToken(token: string): JwtPayload {
    const secret = ensureJwtSecret();
    return jwt.verify(token, secret) as JwtPayload;
  },
};
