import { env } from "../config/env";
import { insertIndicatorRecord, insertIndicatorRecordSimple, deleteIndicatorsByName } from "../db/indicator.repo";
import { getIndicatorByCatalogKey, upsertIndicatorHistoric } from "../db/indicator-historic.repo";
import { IndicatorInsert } from "../models/indicator.model";
import { TermUnit } from "../models/indicator-catalog.model";
import { logger } from "../utils/logger";
import {
  fetchCdt,
  fetchDtf,
  fetchEurUsdBce,
  fetchIbr,
  fetchIpc,
  fetchSofrMarkets,
  fetchTipm,
  fetchTrmDatosGov,
  fetchTrmVercel,
  fetchUsuraAndIbc,
  fetchCpiUsa,
  fetchIppDane,
  fetchInflacionTotal,
  fetchUvr,
  fetchTib,
  fetchTbonds,
  fetchEmbi,
  fetchCuponCeroTes,
  fetchBetasTes,
} from "./fetchers";
import {
  IndicatorJobDefinition,
  IndicatorJobResult,
  IndicatorSample,
} from "./types";

const sanitizeNumberString = (value?: string | number | null): string | null => {
  if (value === undefined || value === null) return null;
  const str = String(value).replace(",", ".").trim();
  if (!str.length) return null;
  const num = Number(str);
  if (!Number.isFinite(num)) {
    throw new Error(`Valor numérico inválido: ${value}`);
  }
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

export const INDICATOR_JOBS: IndicatorJobDefinition[] = [
  {
    key: "TRM_DATOSGOV",
    description: "TRM via datos.gov",
    cron: env.indicatorCron,
    handler: fetchTrmDatosGov,
  },
  {
    key: "TRM_VERCEL",
    description: "TRM via endpoint Vercel",
    cron: env.indicatorCron,
    handler: fetchTrmVercel,
  },
  {
    key: "CDT",
    description: "CDT Banco de la República (180 y 360 días)",
    cron: env.indicatorCron,
    handler: fetchCdt,
  },
  {
    key: "IBR",
    description: "IBR Banco de la República",
    cron: env.indicatorCron,
    handler: fetchIbr,
  },
  {
    key: "DTF",
    description: "DTF Banco de la República",
    cron: env.indicatorCron,
    handler: fetchDtf,
  },
  {
    key: "TIPM",
    description: "Tasa de política monetaria BanRep",
    cron: env.indicatorCron,
    handler: fetchTipm,
  },
  {
    key: "TIB",
    description: "Tasa interbancaria (TIB) - BanRep",
    cron: env.indicatorCron,
    handler: fetchTib,
  },
  {
    key: "EUR_USD",
    description: "Paridad EUR/USD (BCE)",
    cron: env.indicatorCron,
    handler: fetchEurUsdBce,
  },
  {
    key: "IPC",
    description: "IPC - DANE",
    cron: env.indicatorCron,
    handler: fetchIpc,
  },
  {
    key: "IPP",
    description: "IPP Producción Nacional - DANE (Excel mensual)",
    cron: env.indicatorCron,
    handler: fetchIppDane,
  },
  {
    key: "SOFR_MARKETS",
    description: "SOFR Markets",
    cron: env.indicatorCron,
    handler: fetchSofrMarkets,
  },
  {
    key: "USURA_IBC",
    description: "Tasa de Usura e Interés bancario corriente - Superfinanciera (Excel histórico)",
    cron: env.indicatorCron,
    handler: fetchUsuraAndIbc,
  },
  {
    key: "CPI",
    description: "CPI Inflación USA - Bureau of Labor Statistics",
    cron: env.indicatorCron,
    handler: fetchCpiUsa,
  },
  {
    key: "INFLACION_TOTAL",
    description: "Inflación total al consumidor - DANE (Excel IPC mensual)",
    cron: env.indicatorCron,
    handler: fetchInflacionTotal,
  },
  {
    key: "UVR",
    description: "UVR - Cálculo oficial Resolución 13/2000 BanRep",
    cron: env.indicatorCron,
    handler: fetchUvr,
  },
  {
    key: "TBONDS",
    description: "T-Bonds USA - Federal Reserve H.15 Treasury Rates",
    cron: env.indicatorCron,
    handler: fetchTbonds,
  },
  {
    key: "EMBI",
    description: "EMBI (EMBIG) Perú y Colombia - Banco Central de Reserva del Perú",
    cron: env.indicatorCron,
    handler: fetchEmbi,
  },
  {
    key: "CUPON_CERO_TES",
    description: "Tasa Cupón Cero TES - SUAMECA BanRep",
    cron: env.indicatorCron,
    handler: fetchCuponCeroTes,
  },
  {
    key: "BETAS_TES",
    description: "Betas TES Nelson-Siegel - SUAMECA BanRep",
    cron: env.indicatorCron,
    handler: fetchBetasTes,
  },
];

const getDefinition = (jobKey: string) =>
  INDICATOR_JOBS.find((job) => job.key === jobKey);

export interface CatalogKey {
  alias: string;
  segmentation?: string | null;
  term_value?: number | null;
  term_unit?: TermUnit | null;
}

const JOB_KEY_TO_CATALOG: Record<string, CatalogKey> = {
  TRM_DATOSGOV:    { alias: "TRM",       segmentation: null, term_value: 1,  term_unit: "diaria" },
  TRM_VERCEL:      { alias: "TRM",       segmentation: null, term_value: 1,  term_unit: "diaria" },
  DTF:             { alias: "DTF",       segmentation: null, term_value: 90, term_unit: "diaria" },
  TIPM:            { alias: "TIPM",      segmentation: null, term_value: 1,  term_unit: "mensual" },
  TIB:             { alias: "TIB",       segmentation: null, term_value: 1,  term_unit: "diaria" },
  EUR_USD:         { alias: "EUR_USD",   segmentation: null, term_value: 1,  term_unit: "diaria" },
  IPC:             { alias: "IPC",       segmentation: null, term_value: 1,  term_unit: "mensual" },
  IPP:             { alias: "IPP",       segmentation: null, term_value: 1,  term_unit: "mensual" },
  SOFR_MARKETS:    { alias: "SOFR",      segmentation: null, term_value: 1,  term_unit: "diaria" },
  CPI:             { alias: "CPI",       segmentation: null, term_value: 1,  term_unit: "mensual" },
  INFLACION_TOTAL: { alias: "INFLACION", segmentation: null, term_value: 1,  term_unit: "mensual" },
  UVR:             { alias: "UVR",       segmentation: null, term_value: 1,  term_unit: "diaria" },
  EMBI:            { alias: "EMBI",      segmentation: null, term_value: 1,  term_unit: "mensual" },
};

const VARIANT_TO_CATALOG: Record<string, Record<string, CatalogKey>> = {
  CDT: {
    "180_dias": { alias: "CDT", segmentation: "180 días", term_value: 180, term_unit: "diaria" },
    "360_dias": { alias: "CDT", segmentation: "360 días", term_value: 360, term_unit: "diaria" },
  },
  IBR: {
    "overnight_nominal":  { alias: "IBR", segmentation: "overnight, nominal",  term_value: 1,  term_unit: "diaria" },
    "overnight_efectiva": { alias: "IBR", segmentation: "overnight, efectiva", term_value: 1,  term_unit: "diaria" },
    "1_mes_nominal":      { alias: "IBR", segmentation: "1 mes, nominal",      term_value: 1,  term_unit: "mensual" },
    "1_mes_efectiva":     { alias: "IBR", segmentation: "1 mes, efectiva",     term_value: 1,  term_unit: "mensual" },
    "3_meses_nominal":    { alias: "IBR", segmentation: "3 meses, nominal",    term_value: 3,  term_unit: "mensual" },
    "3_meses_efectiva":   { alias: "IBR", segmentation: "3 meses, efectiva",   term_value: 3,  term_unit: "mensual" },
    "6_meses_nominal":    { alias: "IBR", segmentation: "6 meses, nominal",    term_value: 6,  term_unit: "mensual" },
    "6_meses_efectiva":   { alias: "IBR", segmentation: "6 meses, efectiva",   term_value: 6,  term_unit: "mensual" },
    "12_meses_nominal":   { alias: "IBR", segmentation: "12 meses, nominal",   term_value: 12, term_unit: "mensual" },
    "12_meses_efectiva":  { alias: "IBR", segmentation: "12 meses, efectiva",  term_value: 12, term_unit: "mensual" },
  },
  TBONDS: {
    "1_mes":    { alias: "TBOND", segmentation: "1 mes",   term_value: 1,  term_unit: "mensual" },
    "3_meses":  { alias: "TBOND", segmentation: "3 meses", term_value: 3,  term_unit: "mensual" },
    "6_meses":  { alias: "TBOND", segmentation: "6 meses", term_value: 6,  term_unit: "mensual" },
    "1_anio":   { alias: "TBOND", segmentation: "1 año",   term_value: 1,  term_unit: "anual" },
    "2_anios":  { alias: "TBOND", segmentation: "2 años",  term_value: 2,  term_unit: "anual" },
    "3_anios":  { alias: "TBOND", segmentation: "3 años",  term_value: 3,  term_unit: "anual" },
    "5_anios":  { alias: "TBOND", segmentation: "5 años",  term_value: 5,  term_unit: "anual" },
    "7_anios":  { alias: "TBOND", segmentation: "7 años",  term_value: 7,  term_unit: "anual" },
    "10_anios": { alias: "TBOND", segmentation: "10 años", term_value: 10, term_unit: "anual" },
    "20_anios": { alias: "TBOND", segmentation: "20 años", term_value: 20, term_unit: "anual" },
    "30_anios": { alias: "TBOND", segmentation: "30 años", term_value: 30, term_unit: "anual" },
  },
};

const INDICATOR_NAME_TO_CATALOG: Record<string, CatalogKey> = {
  "CUPON_CERO_TES_PESOS_1A":  { alias: "CC", segmentation: "Pesos, 1 año",   term_value: 1,  term_unit: "anual" },
  "CUPON_CERO_TES_PESOS_5A":  { alias: "CC", segmentation: "Pesos, 5 años",  term_value: 5,  term_unit: "anual" },
  "CUPON_CERO_TES_PESOS_10A": { alias: "CC", segmentation: "Pesos, 10 años", term_value: 10, term_unit: "anual" },
  "CUPON_CERO_TES_UVR_1A":    { alias: "CC", segmentation: "UVR, 1 año",     term_value: 1,  term_unit: "anual" },
  "CUPON_CERO_TES_UVR_5A":    { alias: "CC", segmentation: "UVR, 5 años",    term_value: 5,  term_unit: "anual" },
  "CUPON_CERO_TES_UVR_10A":   { alias: "CC", segmentation: "UVR, 10 años",   term_value: 10, term_unit: "anual" },
  "BETAS_TES_PESOS_B0":  { alias: "BT", segmentation: "Pesos, B0",  term_value: 1, term_unit: "diaria" },
  "BETAS_TES_PESOS_B1":  { alias: "BT", segmentation: "Pesos, B1",  term_value: 1, term_unit: "diaria" },
  "BETAS_TES_PESOS_B2":  { alias: "BT", segmentation: "Pesos, B2",  term_value: 1, term_unit: "diaria" },
  "BETAS_TES_PESOS_TAU": { alias: "BT", segmentation: "Pesos, TAU", term_value: 1, term_unit: "diaria" },
  "BETAS_TES_UVR_B0":    { alias: "BT", segmentation: "UVR, B0",    term_value: 1, term_unit: "diaria" },
  "BETAS_TES_UVR_B1":    { alias: "BT", segmentation: "UVR, B1",    term_value: 1, term_unit: "diaria" },
  "BETAS_TES_UVR_B2":    { alias: "BT", segmentation: "UVR, B2",    term_value: 1, term_unit: "diaria" },
  "BETAS_TES_UVR_TAU":   { alias: "BT", segmentation: "UVR, TAU",   term_value: 1, term_unit: "diaria" },
  "USURA": { alias: "USURA", segmentation: "Crédito de consumo y ordinario", term_value: 1, term_unit: "mensual" },
  "IBC":   { alias: "IBC",   segmentation: null, term_value: 1, term_unit: "mensual" },
};

export const resolveCatalogKey = (jobKey: string, sample: IndicatorSample): CatalogKey | null => {
  const byName = INDICATOR_NAME_TO_CATALOG[sample.indicator];
  if (byName) return byName;
  if (sample.variant) {
    const byVariant = VARIANT_TO_CATALOG[jobKey]?.[sample.variant];
    if (byVariant) return byVariant;
  }
  return JOB_KEY_TO_CATALOG[jobKey] ?? null;
};


export async function runIndicatorJob(
  jobKey: string,
  handlerOverride?: () => Promise<IndicatorSample[]>,
): Promise<IndicatorJobResult> {
  const job = getDefinition(jobKey);
  if (!job) throw new Error(`Indicador ${jobKey} no está configurado`);

  const samples = await (handlerOverride ?? job.handler)();
  const insert = job.skipUpsert ? insertIndicatorRecordSimple : insertIndicatorRecord;

  if (job.skipUpsert && samples.length > 0) {
    await deleteIndicatorsByName(samples[0].indicator);
  }

  const catalogCache = new Map<string, Awaited<ReturnType<typeof getIndicatorByCatalogKey>>>();
  const cacheKey = (k: CatalogKey) => `${k.alias}|${k.segmentation ?? ''}|${k.term_value ?? ''}|${k.term_unit ?? ''}`;
  const lookupCatalog = async (key: CatalogKey) => {
    const ck = cacheKey(key);
    if (!catalogCache.has(ck)) {
      catalogCache.set(ck, await getIndicatorByCatalogKey(key.alias, key.segmentation, key.term_value, key.term_unit).catch(() => null));
    }
    return catalogCache.get(ck)!;
  };

  let stored = 0;
  for (const sample of samples) {
    const normalized = normalizeSample(sample);

    await insert(normalized);

    if (normalized.value_number !== null && normalized.value_number !== undefined) {
      const catalogKey = resolveCatalogKey(jobKey, sample);
      const catalogRow = catalogKey ? await lookupCatalog(catalogKey) : null;

      if (catalogRow) {
        const observedDate = normalized.observed_at instanceof Date
          ? normalized.observed_at
          : new Date(normalized.observed_at);

        const fromDate = sample.from_date
          ? new Date(String(sample.from_date))
          : observedDate;

        const periodicity = (catalogRow.term_unit as TermUnit) ?? "diaria";
        const toDate = sample.to_date
          ? new Date(String(sample.to_date))
          : null;

        await upsertIndicatorHistoric({
          indicator_id: catalogRow.id,
          from_date:    fromDate,
          to_date:      toDate,
          value:        normalized.value_number,
          periodicity,
          status:       "definitive",
        }).catch((err: Error) =>
          logger.warn(`[indicator_historic] ${catalogRow.alias} ${fromDate.toISOString().slice(0,10)}: ${err.message}`)
        );
      }
    }

    stored += 1;
  }

  const uniqueIndicators = [...new Set(samples.map((s) => s.indicator))];
  const uniqueVariants = [...new Set(samples.map((s) => s.variant).filter(Boolean))] as string[];
  const dates = samples
    .map((s) => (s.observed_at instanceof Date ? s.observed_at.toISOString() : String(s.observed_at)))
    .sort();

  logger.info(`[indicator] ${jobKey}: guardados ${stored} registros`);

  return {
    jobKey,
    description: job.description,
    stored,
    samples: samples.length,
    indicators: uniqueIndicators,
    variants: uniqueVariants,
    dateRange: {
      from: dates[0] ?? null,
      to: dates[dates.length - 1] ?? null,
    },
  };
}
