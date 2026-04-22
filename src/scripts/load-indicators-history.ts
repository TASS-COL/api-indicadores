/**
 * Carga historica de indicadores (ejecutar UNA SOLA VEZ al configurar el ambiente).
 *
 * Los fetchers del cron solo traen el ultimo dato. Este script carga el historial
 * completo desde 2020 para los indicadores que lo soportan via SUAMECA API.
 *
 * Despues de ejecutar esto, el cron solo mantiene el dato mas reciente.
 *
 * Uso:
 *   npm run load-history
 */
import { pool } from "../db/db";
import {
  ensureIndicatorUniqueIndex,
} from "../db/indicator.repo";
import { IndicatorInsert } from "../models/indicator.model";
import { IndicatorSample } from "../indicators/types";
import { INDICATOR_JOBS, resolveCatalogKey, CatalogKey } from "../indicators/jobs";
import { fetchIppDaneHistory, fetchUsuraAndIbcHistory } from "../indicators/fetchers";
import { getIndicatorByCatalogKey, upsertIndicatorHistoric } from "../db/indicator-historic.repo";
import { TermUnit } from "../models/indicator-catalog.model";
import { env } from "../config/env";
import { logger } from "../utils/logger";

// ─── Fecha de inicio para carga histórica ────────────────────────────────────

const HISTORY_SINCE = new Date("2020-01-01T00:00:00Z").getTime();

// ─── Helpers ─────────────────────────────────────────────────────────────────

const ENSURE_TABLE_SQL = `
  CREATE TABLE IF NOT EXISTS indicator_record (
    id            SERIAL PRIMARY KEY,
    tenant_id     INTEGER,
    indicator     TEXT NOT NULL,
    variant       TEXT,
    value_number  TEXT,
    value_text    TEXT,
    unit          TEXT,
    observed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source        TEXT,
    metadata      JSONB DEFAULT '{}'::jsonb,
    is_active     BOOLEAN DEFAULT TRUE
  );
`;

const sanitizeNumberString = (value?: string | number | null): string | null => {
  if (value === undefined || value === null) return null;
  const str = String(value).replace(",", ".").trim();
  if (!str.length) return null;
  const num = Number(str);
  if (!Number.isFinite(num)) return null;
  return num.toString();
};

const normalizeSample = (sample: IndicatorSample): IndicatorInsert => ({
  indicator: sample.indicator,
  variant: sample.variant ?? null,
  value_number: sanitizeNumberString(sample.value_number),
  value_text: sample.value_text ?? null,
  unit: sample.unit ?? null,
  observed_at:
    sample.observed_at instanceof Date
      ? sample.observed_at
      : new Date(sample.observed_at),
  fetched_at: sample.fetched_at ?? new Date(),
  source: sample.source ?? null,
  metadata: sample.metadata ?? {},
  tenant_id: sample.tenant_id ?? null,
  is_active: sample.is_active ?? true,
});

async function fetchJson(url: string) {
  const res = await fetch(url, { headers: { accept: "application/json" } });
  if (!res.ok) throw new Error(`Error ${res.status}: ${url}`);
  return res.json();
}

function parseNum(v: unknown): string | null {
  if (v === undefined || v === null) return null;
  const n = typeof v === "string" ? Number(v.replace(",", ".")) : Number(v);
  return Number.isFinite(n) ? n.toString() : null;
}

// ─── Fetchers SUAMECA (BanRep) ───────────────────────────────────────────────

interface HistSerie { id: number; indicator: string; variant?: string; unit: string }

async function fetchSuamecaHistory(series: HistSerie[]): Promise<IndicatorSample[]> {
  const base = env.indicators.suamecaBaseUrl;
  if (!base) throw new Error("SUAMECA_BASE_URL no configurada");
  const samples: IndicatorSample[] = [];

  for (const serie of series) {
    const url = `${base}?idSerie=${serie.id}&tipoDato=1&cantDatos=800`;
    const json = await fetchJson(url);
    const entry = Array.isArray(json) ? json[0] : json;
    const dataPoints: [number, number][] = entry?.data ?? [];

    for (const [ts, val] of dataPoints) {
      if (ts < HISTORY_SINCE) continue;
      const value = parseNum(val);
      if (!value) continue;
      const isoDate = new Date(ts).toISOString().slice(0, 10);
      samples.push({
        indicator: serie.indicator,
        variant: serie.variant ?? "diaria",
        value_number: value,
        unit: serie.unit,
        observed_at: new Date(ts).toISOString(),
        from_date: isoDate,
        source: url,
        metadata: { idSerie: serie.id },
      });
    }
  }
  return samples;
}

// ─── Fetcher TRM histórico via datos.gov ─────────────────────────────────────

async function fetchTrmHistory(): Promise<IndicatorSample[]> {
  const base = env.indicators.trmDatosGovUrl;
  if (!base) throw new Error("TRM_DATOSGOV_URL no configurada");
  const sinceDate = new Date(HISTORY_SINCE).toISOString().slice(0, 10);
  const url = new URL(base);
  url.searchParams.set("$where", `vigenciadesde>='${sinceDate}'`);
  url.searchParams.set("$limit", "5000");
  url.searchParams.set("$order", "vigenciadesde ASC");
  const data = await fetchJson(url.toString());

  return (Array.isArray(data) ? data : [])
    .map((item: any) => {
      const value = parseNum(item.valor ?? item.value);
      if (!value) return null;
      return {
        indicator: "TRM",
        variant: "datos_gov",
        value_number: value,
        unit: "COP/USD",
        observed_at: item.vigenciadesde ?? item.fecha,
        from_date: item.vigenciadesde,
        to_date: item.vigenciahasta ?? null,
        source: base,
        metadata: item,
      } as IndicatorSample;
    })
    .filter((s): s is IndicatorSample => s !== null);
}

// ─── Fetcher IPC histórico via DANE API ──────────────────────────────────────

const IPC_SEGMENTS: Record<string, string> = {
  Mensual:   "mensual",
  Acumulado: "anio_corrido",
  "12Meses": "doce_meses",
};

async function fetchIpcHistory(): Promise<IndicatorSample[]> {
  const base = env.indicators.ipcBaseUrl;
  if (!base) throw new Error("IPC_BASE_URL no configurada");
  const samples: IndicatorSample[] = [];

  const sinceDate = new Date(HISTORY_SINCE);
  const sinceYear = sinceDate.getFullYear();
  const sinceMonth = sinceDate.getMonth();
  const now = new Date();
  const endYear = now.getFullYear();
  const endMonth = now.getMonth() - 1; // DANE publica con ~1 mes de retraso

  for (let y = sinceYear; y <= endYear; y++) {
    const startM = y === sinceYear ? sinceMonth : 0;
    const lastM  = y === endYear   ? endMonth   : 11;
    for (let m = startM; m <= lastM; m++) {
      const periodo = `${y}${String(m + 1).padStart(2, "0")}`;
      for (const [segment, variant] of Object.entries(IPC_SEGMENTS)) {
        try {
          const url = `${base}/${periodo}/${segment}`;
          const data = await fetchJson(url);
          if (Array.isArray(data) && data.length === 0) continue;
          const first = Array.isArray(data) ? data[0] : data;
          const value = parseNum(first?.value);
          if (!value) continue;
          const fromDate = `${y}-${String(m + 1).padStart(2, "0")}-01`;
          const lastDay  = new Date(Date.UTC(y, m + 1, 0)).toISOString().slice(0, 10);
          samples.push({
            indicator: "IPC",
            variant,
            value_number: value,
            unit: "%",
            observed_at: `${fromDate}T00:00:00.000Z`,
            from_date: fromDate,
            to_date: lastDay,
            source: url,
            metadata: { periodoConsultado: periodo, segment },
          });
        } catch {
          // mes no disponible aún, continuar
        }
      }
    }
  }
  return samples;
}

// ─── Series BanRep (idSerie de SUAMECA) ──────────────────────────────────────

const CDT_HIST: HistSerie[] = [
  { id: 67, indicator: "CDT", variant: "180_dias", unit: "%" },
  { id: 68, indicator: "CDT", variant: "360_dias", unit: "%" },
];

const IBR_HIST: HistSerie[] = [
  { id: 241,   indicator: "IBR", variant: "overnight_nominal",  unit: "%" },
  { id: 242,   indicator: "IBR", variant: "1_mes_nominal",      unit: "%" },
  { id: 243,   indicator: "IBR", variant: "3_meses_nominal",    unit: "%" },
  { id: 15324, indicator: "IBR", variant: "overnight_efectiva", unit: "%" },
  { id: 15325, indicator: "IBR", variant: "1_mes_efectiva",     unit: "%" },
  { id: 15326, indicator: "IBR", variant: "3_meses_efectiva",   unit: "%" },
  { id: 16560, indicator: "IBR", variant: "6_meses_nominal",    unit: "%" },
  { id: 16561, indicator: "IBR", variant: "6_meses_efectiva",   unit: "%" },
  { id: 16562, indicator: "IBR", variant: "12_meses_nominal",   unit: "%" },
  { id: 16563, indicator: "IBR", variant: "12_meses_efectiva",  unit: "%" },
];

const DTF_HIST: HistSerie[] = [
  { id: 65, indicator: "DTF", variant: "90_dias", unit: "%" },
];

const TIPM_HIST: HistSerie[] = [
  { id: 59, indicator: "TASA_INTERVENCION", variant: "banrep", unit: "%" },
];

const TIB_HIST: HistSerie[] = [
  { id: 89, indicator: "TIB", variant: "diaria", unit: "%" },
];

const CUPON_CERO_HIST: HistSerie[] = [
  { id: 15272, indicator: "CUPON_CERO_TES_PESOS_1A",  unit: "%" },
  { id: 15273, indicator: "CUPON_CERO_TES_PESOS_5A",  unit: "%" },
  { id: 15274, indicator: "CUPON_CERO_TES_PESOS_10A", unit: "%" },
  { id: 15275, indicator: "CUPON_CERO_TES_UVR_1A",    unit: "%" },
  { id: 15276, indicator: "CUPON_CERO_TES_UVR_5A",    unit: "%" },
  { id: 15277, indicator: "CUPON_CERO_TES_UVR_10A",   unit: "%" },
];

const BETAS_TES_HIST: HistSerie[] = [
  { id: 15278, indicator: "BETAS_TES_PESOS_B0",  unit: "Unidades" },
  { id: 15279, indicator: "BETAS_TES_PESOS_B1",  unit: "Unidades" },
  { id: 15280, indicator: "BETAS_TES_PESOS_B2",  unit: "Unidades" },
  { id: 15281, indicator: "BETAS_TES_PESOS_TAU", unit: "Unidades" },
  { id: 15282, indicator: "BETAS_TES_UVR_B0",    unit: "Unidades" },
  { id: 15283, indicator: "BETAS_TES_UVR_B1",    unit: "Unidades" },
  { id: 15284, indicator: "BETAS_TES_UVR_B2",    unit: "Unidades" },
  { id: 15285, indicator: "BETAS_TES_UVR_TAU",   unit: "Unidades" },
];

// ─── Overrides históricos por job key ────────────────────────────────────────

const HISTORICAL_OVERRIDES: Record<string, () => Promise<IndicatorSample[]>> = {
  TRM_DATOSGOV:   () => fetchTrmHistory(),
  CDT:            () => fetchSuamecaHistory(CDT_HIST),
  IBR:            () => fetchSuamecaHistory(IBR_HIST),
  DTF:            () => fetchSuamecaHistory(DTF_HIST),
  TIPM:           () => fetchSuamecaHistory(TIPM_HIST),
  TIB:            () => fetchSuamecaHistory(TIB_HIST),
  CUPON_CERO_TES: () => fetchSuamecaHistory(CUPON_CERO_HIST),
  BETAS_TES:      () => fetchSuamecaHistory(BETAS_TES_HIST),
  IPC:            () => fetchIpcHistory(),
  IPP:            () => fetchIppDaneHistory(2025, 1),
  USURA_IBC:      () => fetchUsuraAndIbcHistory(),
};

// ─── Inserción de muestras en indicator_historic ──────────────────────────────

interface InsertResult { historic: number }

async function insertSamples(
  jobKey: string,
  samples: IndicatorSample[],
): Promise<InsertResult> {
  const catalogCache = new Map<string, Awaited<ReturnType<typeof getIndicatorByCatalogKey>>>();
  const cacheKey = (k: CatalogKey) =>
    `${k.alias}|${k.segmentation ?? ""}|${k.term_value ?? ""}|${k.term_unit ?? ""}`;
  const lookupCatalog = async (key: CatalogKey) => {
    const ck = cacheKey(key);
    if (!catalogCache.has(ck)) {
      catalogCache.set(
        ck,
        await getIndicatorByCatalogKey(key.alias, key.segmentation, key.term_value, key.term_unit).catch(() => null),
      );
    }
    return catalogCache.get(ck)!;
  };

  let historic = 0;

  for (const sample of samples) {
    const normalized = normalizeSample(sample);
    if (normalized.value_number === null) continue;

    const catalogKey = resolveCatalogKey(jobKey, sample);
    const catalogRow = catalogKey ? await lookupCatalog(catalogKey) : null;
    if (!catalogRow) continue;

    const observedDate = normalized.observed_at instanceof Date
      ? normalized.observed_at
      : new Date(normalized.observed_at);

    const fromDate = sample.from_date ? new Date(String(sample.from_date)) : observedDate;
    const toDate   = sample.to_date   ? new Date(String(sample.to_date))   : null;

    try {
      await upsertIndicatorHistoric({
        indicator_id: catalogRow.id,
        from_date:    fromDate,
        to_date:      toDate,
        value:        normalized.value_number,
        periodicity:  (catalogRow.term_unit as TermUnit) ?? "diaria",
        status:       "definitive",
      });
      historic += 1;
    } catch {
      // duplicado o constraint, continuar
    }
  }

  return { historic };
}

// ─── Main ─────────────────────────────────────────────────────────────────────

interface JobResult {
  key: string;
  historic: number;
  estado: string;
  error: string;
  ms: number;
}

export async function runLoadHistory(): Promise<void> {
  console.log("[load-history] Conectando a PostgreSQL...");
  await pool.query(ENSURE_TABLE_SQL);
  console.log("[load-history] Tabla indicator_record OK");
  await ensureIndicatorUniqueIndex();
  console.log("[load-history] Índices OK");

  const sinceLabel = new Date(HISTORY_SINCE).toISOString().slice(0, 10);
  logger.info(`[load-history] Iniciando carga histórica desde ${sinceLabel}...\n`);

  const results: JobResult[] = [];

  for (const job of INDICATOR_JOBS) {
    const start = Date.now();
    try {
      const handler = HISTORICAL_OVERRIDES[job.key] ?? job.handler;
      const label   = HISTORICAL_OVERRIDES[job.key] ? "historico" : "actual";
      logger.info(`[load-history] → ${job.key} (${label})`);

      const samples = await handler();
      const { historic } = await insertSamples(job.key, samples);

      logger.info(`[load-history]   ${job.key}: ${historic} registros en ${Date.now() - start}ms`);
      results.push({ key: job.key, historic, estado: "OK", error: "", ms: Date.now() - start });
    } catch (e: any) {
      logger.error(`[load-history]   ${job.key} ERROR: ${e.message}`);
      results.push({ key: job.key, historic: 0, estado: "ERROR", error: e.message, ms: Date.now() - start });
    }
  }

  // ── Resumen ──────────────────────────────────────────────────────────────
  const total   = results.reduce((s, r) => s + r.historic, 0);
  const failed  = results.filter((r) => r.estado === "ERROR");

  console.log("\n════════════════════════════════════════════════════════════");
  console.log("  CARGA HISTÓRICA COMPLETADA");
  console.log("════════════════════════════════════════════════════════════\n");
  console.table(results.map((r) => ({
    Indicador: r.key,
    Registros: r.historic,
    Estado:    r.estado,
    "Tiempo (ms)": r.ms,
    Error:     r.error || "—",
  })));
  console.log(`\nTotal registros almacenados: ${total} | OK: ${results.length - failed.length} | Errores: ${failed.length}`);

  // Resumen desde la BD
  const { rows } = await pool.query(`
    SELECT i.alias, ih.periodicity, COUNT(*) AS registros,
           MIN(ih.from_date::date) AS desde, MAX(ih.from_date::date) AS hasta
    FROM indicator_historic ih
    JOIN indicator i ON i.id = ih.indicator_id
    GROUP BY i.alias, ih.periodicity
    ORDER BY i.alias, ih.periodicity
  `);
  console.log("\nResumen en BD (indicator_historic):");
  console.table(rows);

  if (failed.length > 0) process.exitCode = 1;
}

if (require.main === module) {
  runLoadHistory()
    .catch((e) => {
      console.error("[load-history] Error fatal:");
      console.error(e);
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}
