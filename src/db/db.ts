import { Pool, QueryResult } from "pg";
import { env } from "../config/env";

function toPositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.trunc(parsed) : fallback;
}

const connectionTimeoutMillis = toPositiveInt(
  process.env.DB_CONNECTION_TIMEOUT_MS,
  10000
);
const pingRetries = toPositiveInt(process.env.DB_PING_RETRIES, 5);
const pingRetryDelayMs = toPositiveInt(process.env.DB_PING_RETRY_DELAY_MS, 1500);

const dbConfig = {
  host: env.pg.host ?? process.env.DB_HOST ?? process.env.PGHOST ?? "localhost",
  user: env.pg.user ?? process.env.DB_USER ?? process.env.PGUSER ?? "postgres",
  password:
    env.pg.password ??
    process.env.DB_PASSWORD ??
    process.env.DB_PASS ??
    "postgres",
  database:
    env.pg.database ??
    process.env.DB_NAME ??
    process.env.PGDATABASE ??
    "indicators",
  port: Number(env.pg.port ?? process.env.DB_PORT ?? process.env.PGPORT ?? 5432),
  max: Number(process.env.DB_CONNECTION_LIMIT ?? 10),
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis,
};

export const pool = new Pool(dbConfig);

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function pingDB(): Promise<boolean> {
  for (let attempt = 1; attempt <= pingRetries; attempt++) {
    try {
      const client = await pool.connect();
      await client.query("SELECT 1");
      client.release();
      return true;
    } catch (error) {
      console.error(
        `[db] Database connection failed (attempt ${attempt}/${pingRetries}):`,
        error
      );
      if (attempt < pingRetries) {
        await wait(pingRetryDelayMs);
      }
    }
  }
  return false;
}

export type { QueryResult };
