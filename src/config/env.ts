import "dotenv/config";

interface DatabaseConfig {
  host: string | undefined;
  port: number;
  user: string | undefined;
  password: string | undefined;
  database: string | undefined;
}

interface JwtConfig {
  secret: string;
  expiresIn: string;
}

interface IndicatorSources {
  trmDatosGovUrl?: string;
  trmVercelUrl?: string;
  ibrUrl?: string;
  dtfUrl?: string;
  tipmUrl?: string;
  bceUrl?: string;
  ipcUrl?: string;
  ippUrl?: string;
  sofrMarketsUrl1?: string;
  sofrMarketsUrl2?: string;
  banrepBaseUrl?: string;
  ipcBaseUrl?: string;
  usuraAndIbcUrl?: string;
  cpiUsaUrl?: string;
  embiBcrpUrl?: string;
  betasDamodaranEmergUrl?: string;
  betasDamodaranGlobalUrl?: string;
  daneIppBaseUrl?: string;
  daneIpcBaseUrl?: string;
  federalReserveH15Url?: string;
  suamecaBaseUrl?: string;
  uvrSeedValue?: string;
  uvrSeedDate?: string;
}

interface EnvConfig {
  tz: string;
  redisUrl: string;
  indicatorQueueName: string;
  indicatorCron: string;
  port: number;
  dashPath: string;
  dashUser: string;
  dashPass: string;
  apiKey: string;
  jwt: JwtConfig;
  pg: DatabaseConfig;
  indicators: IndicatorSources;
}

function parseBoolean(value: string | undefined, defaultValue: boolean): boolean {
  const normalized = value?.trim().toLowerCase();
  if (!normalized) return defaultValue;
  if (normalized === "true") return true;
  if (normalized === "false") return false;
  return defaultValue;
}

export const env: EnvConfig = {
  tz: process.env.TZ || "America/Bogota",
  redisUrl: process.env.REDIS_URL || "redis://127.0.0.1:6379",
  indicatorQueueName: (process.env.INDICATOR_QUEUE_NAME || "indicators:tasks").replace(/:/g, "_"),
  indicatorCron: process.env.INDICATOR_CRON || "0 * * * *",
  port: Number(process.env.PORT || 3008),
  dashPath: process.env.DASHBOARD_PATH || "/admin/queues",
  dashUser: process.env.DASHUSER || "admin",
  dashPass: process.env.DASHPASS || "admin123",
  apiKey: process.env.API_KEY || "",
  jwt: {
    secret: process.env.JWT_SECRET || "",
    expiresIn: process.env.JWT_EXPIRES_IN || "1h",
  },
  pg: {
    host: process.env.PGHOST,
    port: Number(process.env.PGPORT || 5432),
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    database: process.env.PGDATABASE || "indicators",
  },
  indicators: {
    trmDatosGovUrl: process.env.TRM_DATOSGOV_URL,
    trmVercelUrl: process.env.TRM_VERCEL_URL,
    bceUrl: process.env.BCE_URL,
    banrepBaseUrl: process.env.BANREP_BASE_URL,
    ipcBaseUrl: process.env.IPC_BASE_URL,
    usuraAndIbcUrl: process.env.USURA_IBR_URL,
    cpiUsaUrl: process.env.CPI_USA_URL,
    embiBcrpUrl: process.env.EMBI_BCRP_URL,
    betasDamodaranEmergUrl: process.env.BETAS_DAMODARAN_EMERG_URL,
    betasDamodaranGlobalUrl: process.env.BETAS_DAMODARAN_GLOBAL_URL,
    daneIppBaseUrl: process.env.DANE_IPP_BASE_URL,
    daneIpcBaseUrl: process.env.DANE_IPC_BASE_URL,
    federalReserveH15Url: process.env.FEDERAL_RESERVE_H15_URL,
    suamecaBaseUrl: process.env.SUAMECA_BASE_URL,
    sofrMarketsUrl1: process.env.SOFR_MARKETS_URL_1,
    sofrMarketsUrl2: process.env.SOFR_MARKETS_URL_2,
    uvrSeedValue: process.env.UVR_SEED_VALUE,
    uvrSeedDate: process.env.UVR_SEED_DATE,
  },
};

// Validate required env vars at startup
const required = ["PGHOST", "PGUSER", "PGPASSWORD", "PGDATABASE"];
const missing = required.filter((v) => !process.env[v]);
if (missing.length > 0) {
  console.error("Missing required environment variables:", missing);
  process.exit(1);
}

// parseBoolean is used for future optional flags
void parseBoolean;
