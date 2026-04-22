import { pool } from "../db/db";
import { upsertIndicatorCatalog, getIndicatorByCatalogKey } from "../db/indicator-historic.repo";
import { upsertIndicatorGroup, addGroupMember } from "../db/indicator-group.repo";
import { IndicatorCatalogInsert, TermUnit } from "../models/indicator-catalog.model";
import { logger } from "../utils/logger";

const INDICATORS: IndicatorCatalogInsert[] = [
  { alias: "TRM",       name: "Tasa Representativa del Mercado",       segmentation: null,                              term_value: 1,   term_unit: "diaria",  base: null, unit: "cop",          source: "SuperFinanciera / datos.gov.co",    is_active: true },
  { alias: "EUR_USD",   name: "Paridad EUR/USD",                       segmentation: null,                              term_value: 1,   term_unit: "diaria",  base: null, unit: "usd",          source: "Banco Central Europeo",             is_active: true },
  { alias: "TIPM",      name: "Tasa de Política Monetaria BanRep",     segmentation: null,                              term_value: 1,   term_unit: "mensual", base: null, unit: "percentage",   source: "Banco de la República",             is_active: true },
  { alias: "TIB",       name: "Tasa Interbancaria",                    segmentation: null,                              term_value: 1,   term_unit: "diaria",  base: 360,  unit: "percentage",   source: "Banco de la República",             is_active: true },
  { alias: "DTF",       name: "DTF - Banco de la República",           segmentation: null,                              term_value: 90,  term_unit: "diaria",  base: 360,  unit: "percentage",   source: "Banco de la República",             is_active: true },
  { alias: "IBC",       name: "Interés Bancario Corriente",            segmentation: null,                              term_value: 1,   term_unit: "mensual", base: 360,  unit: "percentage",   source: "Superfinanciera",                   is_active: true },
  { alias: "USURA",     name: "Tasa de Usura",                         segmentation: "Crédito de consumo y ordinario",  term_value: 1,   term_unit: "mensual", base: null, unit: "percentage",   source: "Superfinanciera",                   is_active: true },
  { alias: "USURA",     name: "Tasa de Usura",                         segmentation: "Crédito popular productivo",      term_value: 1,   term_unit: "mensual", base: null, unit: "percentage",   source: "Superfinanciera",                   is_active: true },
  { alias: "CDT",       name: "CDT - Banco de la República",           segmentation: "180 días",                        term_value: 180, term_unit: "diaria",  base: 360,  unit: "percentage",   source: "Banco de la República",             is_active: true },
  { alias: "CDT",       name: "CDT - Banco de la República",           segmentation: "360 días",                        term_value: 360, term_unit: "diaria",  base: 360,  unit: "percentage",   source: "Banco de la República",             is_active: true },
  { alias: "IBR", name: "Indicador Bancario de Referencia", segmentation: "overnight, nominal",  term_value: 1,  term_unit: "diaria",  base: 360, unit: "percentage", source: "Banco de la República", is_active: true },
  { alias: "IBR", name: "Indicador Bancario de Referencia", segmentation: "overnight, efectiva", term_value: 1,  term_unit: "diaria",  base: 365, unit: "percentage", source: "Banco de la República", is_active: true },
  { alias: "IBR", name: "Indicador Bancario de Referencia", segmentation: "1 mes, nominal",      term_value: 1,  term_unit: "mensual", base: 360, unit: "percentage", source: "Banco de la República", is_active: true },
  { alias: "IBR", name: "Indicador Bancario de Referencia", segmentation: "1 mes, efectiva",     term_value: 1,  term_unit: "mensual", base: 365, unit: "percentage", source: "Banco de la República", is_active: true },
  { alias: "IBR", name: "Indicador Bancario de Referencia", segmentation: "3 meses, nominal",    term_value: 3,  term_unit: "mensual", base: 360, unit: "percentage", source: "Banco de la República", is_active: true },
  { alias: "IBR", name: "Indicador Bancario de Referencia", segmentation: "3 meses, efectiva",   term_value: 3,  term_unit: "mensual", base: 365, unit: "percentage", source: "Banco de la República", is_active: true },
  { alias: "IBR", name: "Indicador Bancario de Referencia", segmentation: "6 meses, nominal",    term_value: 6,  term_unit: "mensual", base: 360, unit: "percentage", source: "Banco de la República", is_active: true },
  { alias: "IBR", name: "Indicador Bancario de Referencia", segmentation: "6 meses, efectiva",   term_value: 6,  term_unit: "mensual", base: 365, unit: "percentage", source: "Banco de la República", is_active: true },
  { alias: "IBR", name: "Indicador Bancario de Referencia", segmentation: "12 meses, nominal",   term_value: 12, term_unit: "mensual", base: 360, unit: "percentage", source: "Banco de la República", is_active: true },
  { alias: "IBR", name: "Indicador Bancario de Referencia", segmentation: "12 meses, efectiva",  term_value: 12, term_unit: "mensual", base: 365, unit: "percentage", source: "Banco de la República", is_active: true },
  { alias: "SOFR",      name: "SOFR",                                  segmentation: null,                              term_value: 1,   term_unit: "diaria",  base: 360,  unit: "percentage",   source: "Markets",                           is_active: true },
  { alias: "EMBI",      name: "EMBI Colombia",                         segmentation: null,                              term_value: 1,   term_unit: "mensual", base: null, unit: "basis_points", source: "Banco Central de Reserva del Perú", is_active: true },
  { alias: "TBOND", name: "T-Bond USA", segmentation: "1 mes",   term_value: 1,  term_unit: "mensual", base: 365, unit: "percentage", source: "Federal Reserve H.15", is_active: true },
  { alias: "TBOND", name: "T-Bond USA", segmentation: "3 meses", term_value: 3,  term_unit: "mensual", base: 365, unit: "percentage", source: "Federal Reserve H.15", is_active: true },
  { alias: "TBOND", name: "T-Bond USA", segmentation: "6 meses", term_value: 6,  term_unit: "mensual", base: 365, unit: "percentage", source: "Federal Reserve H.15", is_active: true },
  { alias: "TBOND", name: "T-Bond USA", segmentation: "1 año",   term_value: 1,  term_unit: "anual",   base: 365, unit: "percentage", source: "Federal Reserve H.15", is_active: true },
  { alias: "TBOND", name: "T-Bond USA", segmentation: "2 años",  term_value: 2,  term_unit: "anual",   base: 365, unit: "percentage", source: "Federal Reserve H.15", is_active: true },
  { alias: "TBOND", name: "T-Bond USA", segmentation: "3 años",  term_value: 3,  term_unit: "anual",   base: 365, unit: "percentage", source: "Federal Reserve H.15", is_active: true },
  { alias: "TBOND", name: "T-Bond USA", segmentation: "5 años",  term_value: 5,  term_unit: "anual",   base: 365, unit: "percentage", source: "Federal Reserve H.15", is_active: true },
  { alias: "TBOND", name: "T-Bond USA", segmentation: "7 años",  term_value: 7,  term_unit: "anual",   base: 365, unit: "percentage", source: "Federal Reserve H.15", is_active: true },
  { alias: "TBOND", name: "T-Bond USA", segmentation: "10 años", term_value: 10, term_unit: "anual",   base: 365, unit: "percentage", source: "Federal Reserve H.15", is_active: true },
  { alias: "TBOND", name: "T-Bond USA", segmentation: "20 años", term_value: 20, term_unit: "anual",   base: 365, unit: "percentage", source: "Federal Reserve H.15", is_active: true },
  { alias: "TBOND", name: "T-Bond USA", segmentation: "30 años", term_value: 30, term_unit: "anual",   base: 365, unit: "percentage", source: "Federal Reserve H.15", is_active: true },
  { alias: "IPC",       name: "IPC - DANE",                            segmentation: null,                              term_value: 1,   term_unit: "mensual", base: null, unit: "percentage",   source: "DANE",                              is_active: true },
  { alias: "IPP",       name: "IPP Producción Nacional - DANE",        segmentation: null,                              term_value: 1,   term_unit: "mensual", base: null, unit: "percentage",   source: "DANE",                              is_active: true },
  { alias: "INFLACION", name: "Inflación Total al Consumidor - DANE",  segmentation: null,                              term_value: 1,   term_unit: "mensual", base: null, unit: "percentage",   source: "DANE",                              is_active: true },
  { alias: "CPI",       name: "CPI Inflación USA - BLS",               segmentation: null,                              term_value: 1,   term_unit: "mensual", base: null, unit: "index",        source: "U.S. Bureau of Labor Statistics",    is_active: true },
  { alias: "UVR",       name: "UVR - Resolución 13/2000 BanRep",       segmentation: null,                              term_value: 1,   term_unit: "diaria",  base: null, unit: "cop",          source: "Banco de la República",             is_active: true },
  { alias: "BETAS",     name: "Betas por industria - Damodaran",       segmentation: null,                              term_value: 1,   term_unit: "anual",   base: null, unit: "index",        source: "Damodaran Online",                  is_active: true },
  { alias: "CC", name: "Cupón Cero TES", segmentation: "Pesos, 1 año",   term_value: 1,  term_unit: "anual", base: null, unit: "percentage", source: "SUAMECA - BanRep", is_active: true },
  { alias: "CC", name: "Cupón Cero TES", segmentation: "Pesos, 5 años",  term_value: 5,  term_unit: "anual", base: null, unit: "percentage", source: "SUAMECA - BanRep", is_active: true },
  { alias: "CC", name: "Cupón Cero TES", segmentation: "Pesos, 10 años", term_value: 10, term_unit: "anual", base: null, unit: "percentage", source: "SUAMECA - BanRep", is_active: true },
  { alias: "CC", name: "Cupón Cero TES", segmentation: "UVR, 1 año",     term_value: 1,  term_unit: "anual", base: null, unit: "percentage", source: "SUAMECA - BanRep", is_active: true },
  { alias: "CC", name: "Cupón Cero TES", segmentation: "UVR, 5 años",    term_value: 5,  term_unit: "anual", base: null, unit: "percentage", source: "SUAMECA - BanRep", is_active: true },
  { alias: "CC", name: "Cupón Cero TES", segmentation: "UVR, 10 años",   term_value: 10, term_unit: "anual", base: null, unit: "percentage", source: "SUAMECA - BanRep", is_active: true },
  { alias: "BT", name: "Beta TES Nelson-Siegel", segmentation: "Pesos, B0",  term_value: 1, term_unit: "diaria", base: null, unit: "index", source: "SUAMECA - BanRep", is_active: true },
  { alias: "BT", name: "Beta TES Nelson-Siegel", segmentation: "Pesos, B1",  term_value: 1, term_unit: "diaria", base: null, unit: "index", source: "SUAMECA - BanRep", is_active: true },
  { alias: "BT", name: "Beta TES Nelson-Siegel", segmentation: "Pesos, B2",  term_value: 1, term_unit: "diaria", base: null, unit: "index", source: "SUAMECA - BanRep", is_active: true },
  { alias: "BT", name: "Beta TES Nelson-Siegel", segmentation: "Pesos, TAU", term_value: 1, term_unit: "diaria", base: null, unit: "index", source: "SUAMECA - BanRep", is_active: true },
  { alias: "BT", name: "Beta TES Nelson-Siegel", segmentation: "UVR, B0",    term_value: 1, term_unit: "diaria", base: null, unit: "index", source: "SUAMECA - BanRep", is_active: true },
  { alias: "BT", name: "Beta TES Nelson-Siegel", segmentation: "UVR, B1",    term_value: 1, term_unit: "diaria", base: null, unit: "index", source: "SUAMECA - BanRep", is_active: true },
  { alias: "BT", name: "Beta TES Nelson-Siegel", segmentation: "UVR, B2",    term_value: 1, term_unit: "diaria", base: null, unit: "index", source: "SUAMECA - BanRep", is_active: true },
  { alias: "BT", name: "Beta TES Nelson-Siegel", segmentation: "UVR, TAU",   term_value: 1, term_unit: "diaria", base: null, unit: "index", source: "SUAMECA - BanRep", is_active: true },
];

type GroupMember = { alias: string; segmentation?: string | null; term_value?: number | null; term_unit?: TermUnit | null };

const GROUPS: { name: string; description: string; members: GroupMember[] }[] = [
  {
    name: "CDT",
    description: "Certificados de Depósito a Término - Banco de la República",
    members: [
      { alias: "CDT", segmentation: "180 días", term_value: 180, term_unit: "diaria" },
      { alias: "CDT", segmentation: "360 días", term_value: 360, term_unit: "diaria" },
    ],
  },
  {
    name: "IBR",
    description: "Indicador Bancario de Referencia - Banco de la República",
    members: [
      { alias: "IBR", segmentation: "overnight, nominal",  term_value: 1,  term_unit: "diaria" },
      { alias: "IBR", segmentation: "overnight, efectiva", term_value: 1,  term_unit: "diaria" },
      { alias: "IBR", segmentation: "1 mes, nominal",      term_value: 1,  term_unit: "mensual" },
      { alias: "IBR", segmentation: "1 mes, efectiva",     term_value: 1,  term_unit: "mensual" },
      { alias: "IBR", segmentation: "3 meses, nominal",    term_value: 3,  term_unit: "mensual" },
      { alias: "IBR", segmentation: "3 meses, efectiva",   term_value: 3,  term_unit: "mensual" },
      { alias: "IBR", segmentation: "6 meses, nominal",    term_value: 6,  term_unit: "mensual" },
      { alias: "IBR", segmentation: "6 meses, efectiva",   term_value: 6,  term_unit: "mensual" },
      { alias: "IBR", segmentation: "12 meses, nominal",   term_value: 12, term_unit: "mensual" },
      { alias: "IBR", segmentation: "12 meses, efectiva",  term_value: 12, term_unit: "mensual" },
    ],
  },
  {
    name: "T-Bonds USA",
    description: "Tasas del Tesoro Americano - Federal Reserve H.15",
    members: [
      { alias: "TBOND", segmentation: "1 mes",   term_value: 1,  term_unit: "mensual" },
      { alias: "TBOND", segmentation: "3 meses", term_value: 3,  term_unit: "mensual" },
      { alias: "TBOND", segmentation: "6 meses", term_value: 6,  term_unit: "mensual" },
      { alias: "TBOND", segmentation: "1 año",   term_value: 1,  term_unit: "anual" },
      { alias: "TBOND", segmentation: "2 años",  term_value: 2,  term_unit: "anual" },
      { alias: "TBOND", segmentation: "3 años",  term_value: 3,  term_unit: "anual" },
      { alias: "TBOND", segmentation: "5 años",  term_value: 5,  term_unit: "anual" },
      { alias: "TBOND", segmentation: "7 años",  term_value: 7,  term_unit: "anual" },
      { alias: "TBOND", segmentation: "10 años", term_value: 10, term_unit: "anual" },
      { alias: "TBOND", segmentation: "20 años", term_value: 20, term_unit: "anual" },
      { alias: "TBOND", segmentation: "30 años", term_value: 30, term_unit: "anual" },
    ],
  },
  {
    name: "Cupón Cero TES",
    description: "Tasas Cupón Cero TES - SUAMECA Banco de la República",
    members: [
      { alias: "CC", segmentation: "Pesos, 1 año",   term_value: 1,  term_unit: "anual" },
      { alias: "CC", segmentation: "Pesos, 5 años",  term_value: 5,  term_unit: "anual" },
      { alias: "CC", segmentation: "Pesos, 10 años", term_value: 10, term_unit: "anual" },
      { alias: "CC", segmentation: "UVR, 1 año",     term_value: 1,  term_unit: "anual" },
      { alias: "CC", segmentation: "UVR, 5 años",    term_value: 5,  term_unit: "anual" },
      { alias: "CC", segmentation: "UVR, 10 años",   term_value: 10, term_unit: "anual" },
    ],
  },
  {
    name: "Betas TES Nelson-Siegel",
    description: "Parámetros Nelson-Siegel TES - SUAMECA Banco de la República",
    members: [
      { alias: "BT", segmentation: "Pesos, B0",  term_value: 1, term_unit: "diaria" },
      { alias: "BT", segmentation: "Pesos, B1",  term_value: 1, term_unit: "diaria" },
      { alias: "BT", segmentation: "Pesos, B2",  term_value: 1, term_unit: "diaria" },
      { alias: "BT", segmentation: "Pesos, TAU", term_value: 1, term_unit: "diaria" },
      { alias: "BT", segmentation: "UVR, B0",    term_value: 1, term_unit: "diaria" },
      { alias: "BT", segmentation: "UVR, B1",    term_value: 1, term_unit: "diaria" },
      { alias: "BT", segmentation: "UVR, B2",    term_value: 1, term_unit: "diaria" },
      { alias: "BT", segmentation: "UVR, TAU",   term_value: 1, term_unit: "diaria" },
    ],
  },
];

export async function runSeed() {
  logger.info("[seed] Iniciando seed de catálogo de indicadores...");
  let ok = 0;
  let fail = 0;

  for (const ind of INDICATORS) {
    try {
      const row = await upsertIndicatorCatalog(ind);
      const seg = row.segmentation ? ` [${row.segmentation}]` : "";
      logger.info(`[seed] OK  ${row.alias.padEnd(10)}${seg.padEnd(30)} tv=${String(row.term_value ?? "").padEnd(4)} tu=${row.term_unit ?? ""} id=${row.id}`);
      ok++;
    } catch (err) {
      logger.error(`[seed] FAIL ${ind.alias} seg=${ind.segmentation ?? ""} tv=${ind.term_value}: ${(err as Error).message}`);
      fail++;
    }
  }

  logger.info(`[seed] Indicadores: ${ok} ok, ${fail} errores`);

  logger.info("[seed] Creando grupos...");
  for (const group of GROUPS) {
    try {
      const groupRow = await upsertIndicatorGroup(group.name, group.description);
      let added = 0;
      for (const m of group.members) {
        const ind = await getIndicatorByCatalogKey(m.alias, m.segmentation, m.term_value, m.term_unit);
        if (!ind) {
          logger.warn(`[seed] Grupo "${group.name}": ${m.alias} [${m.segmentation ?? ""}] no encontrado`);
          continue;
        }
        await addGroupMember(groupRow.id, ind.id);
        added++;
      }
      logger.info(`[seed] Grupo "${group.name}" → ${added}/${group.members.length} miembros`);
    } catch (err) {
      logger.error(`[seed] FAIL grupo "${group.name}": ${(err as Error).message}`);
    }
  }

  logger.info("[seed] Seed completado.");
}

if (require.main === module) {
  runSeed()
    .catch((err) => { console.error(err); process.exit(1); })
    .finally(() => pool.end());
}
