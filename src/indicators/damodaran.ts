import XLSX from "xlsx";
import { upsertDamodaranBatch } from "../db/damodaran.repo";
import { logger } from "../utils/logger";

// ─── Types ───────────────────────────────────────────────────────────────────

interface ColDef {
  key: string;
  match: (header: string) => boolean;
  isPercent?: boolean;
}

interface DatasetConfig {
  dataset: string;   // URL-safe key used in DB and endpoints
  name: string;      // Display name
  url: string;
  cols: ColDef[];
}

// ─── Dataset configurations ──────────────────────────────────────────────────

export const DAMODARAN_DATASETS: DatasetConfig[] = [
  {
    dataset: "betas-global",
    name: "Betas Globales",
    url: "https://pages.stern.nyu.edu/~adamodar/pc/datasets/betaGlobal.xls",
    cols: [
      { key: "unlevered_beta", match: (h) => h.includes("unlevered beta") && !h.includes("corrected") },
    ],
  },
  {
    dataset: "multiplo-ev-ebitda",
    name: "Múltiplo EV/EBITDA",
    url: "https://pages.stern.nyu.edu/~adamodar/pc/datasets/vebitda.xls",
    cols: [
      { key: "ev_ebitdard", match: (h) => h.includes("ev/ebitdar") },
      { key: "ev_ebitda",   match: (h) => h.includes("ev/ebitda") && !h.includes("r") },
      { key: "ev_ebit",     match: (h) => h.includes("ev/ebit") && !h.includes("da") && !h.includes("1-t") && !h.includes("(1") },
      { key: "ev_ebit_1t",  match: (h) => h.includes("ev/ebit") && (h.includes("1-t") || h.includes("(1")) },
    ],
  },
  {
    dataset: "multiplo-ev-sales",
    name: "Múltiplo EV/Sales",
    url: "https://pages.stern.nyu.edu/~adamodar/pc/datasets/psdata.xls",
    cols: [
      { key: "price_sales", match: (h) => h.includes("price/sales") || h.includes("price/ sales") },
      { key: "net_margin",  match: (h) => h.includes("net margin"), isPercent: true },
      { key: "ev_sales",    match: (h) => h.includes("ev/sales") },
    ],
  },
];

// ─── Generic XLS parser ───────────────────────────────────────────────────────

function parseNumberStr(val: any, isPercent = false): number | null {
  if (val === null || val === undefined || val === "") return null;
  const str = String(val).trim();
  if (str.toUpperCase() === "NA" || str === "#DIV/0!" || str === "#N/A") return null;
  const cleaned = str.replace(",", ".").replace("%", "").trim();
  const n = typeof val === "number" ? val : parseFloat(cleaned);
  if (!Number.isFinite(n)) return null;
  // Store percentages as decimal fraction (e.g. 28.89% → 0.2889)
  return isPercent ? n / 100 : n;
}

function excelSerialToDate(serial: number): Date {
  return new Date(new Date(1899, 11, 30).getTime() + serial * 86400000);
}

interface ParseResult {
  updateDate: string;
  rows: { industry_name: string; metrics: Record<string, number | null> }[];
}

function parseDamodaranXls(buffer: Buffer, config: DatasetConfig): ParseResult {
  const wb = XLSX.read(buffer, { type: "buffer" });
  const sheetName =
    wb.SheetNames.find((n) => n.toLowerCase().includes("industry")) ??
    wb.SheetNames[0];

  const data = XLSX.utils.sheet_to_json(wb.Sheets[sheetName], {
    header: 1,
    defval: "",
  }) as any[][];

  // Extract update date from first 10 rows
  let updateDate = `${new Date().getFullYear()}-01-01`;
  for (let i = 0; i < 10; i++) {
    const row = data[i];
    if (!row) continue;
    for (let j = 0; j < row.length; j++) {
      const cell = String(row[j] ?? "").toLowerCase();
      if (cell.includes("date updated") || cell.includes("last updated")) {
        const dateCell = row[j + 1] ?? row[j + 2];
        if (typeof dateCell === "number" && dateCell > 40000) {
          updateDate = excelSerialToDate(dateCell).toISOString().slice(0, 10);
        } else if (dateCell) {
          const parsed = new Date(String(dateCell));
          if (!Number.isNaN(parsed.getTime())) updateDate = parsed.toISOString().slice(0, 10);
        }
        break;
      }
    }
  }

  // Find header row (contains "industry name" or "number of firms")
  let headerIdx = -1;
  for (let i = 0; i < 20; i++) {
    const row = data[i];
    if (!row) continue;
    const joined = row.map((c: any) => String(c).toLowerCase()).join(" ");
    if (joined.includes("industry name") || joined.includes("number of firms")) {
      headerIdx = i;
      break;
    }
  }
  if (headerIdx < 0) throw new Error(`Damodaran ${config.dataset}: header row not found`);

  const headers = (data[headerIdx] as any[]).map((h) => String(h ?? "").toLowerCase().trim());

  // Map column definitions to indices
  const colIndices = config.cols.map((col) => ({
    key: col.key,
    idx: headers.findIndex((h, i) => i > 0 && col.match(h)),
    isPercent: col.isPercent ?? false,
  }));

  // Parse data rows
  const rows: ParseResult["rows"] = [];
  for (let i = headerIdx + 1; i < data.length; i++) {
    const row = data[i];
    if (!row) continue;
    const industry = String(row[0] ?? "").trim();
    if (!industry || industry.toLowerCase().includes("total market")) continue;

    const metrics: Record<string, number | null> = {};
    for (const { key, idx, isPercent } of colIndices) {
      metrics[key] = idx >= 0 ? parseNumberStr(row[idx], isPercent) : null;
    }

    // Skip rows where ALL metric values are NA (e.g. financial firms in EV/EBITDA)
    const hasAny = Object.values(metrics).some((v) => v !== null);
    if (!hasAny) continue;

    rows.push({ industry_name: industry, metrics });
  }

  return { updateDate, rows };
}

// ─── Job runner ───────────────────────────────────────────────────────────────

export interface DamodaranJobResult {
  dataset: string;
  stored: number;
  updateDate: string;
}

export async function runDamodaranJob(dataset: string): Promise<DamodaranJobResult> {
  const config = DAMODARAN_DATASETS.find((d) => d.dataset === dataset);
  if (!config) throw new Error(`Dataset Damodaran desconocido: ${dataset}`);

  logger.info(`[damodaran] Descargando ${config.name} desde ${config.url}`);
  const res = await fetch(config.url, {
    headers: { "User-Agent": "Mozilla/5.0 (compatible; indicators-api/1.0)" },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} al descargar ${config.name}`);

  const buffer = Buffer.from(await res.arrayBuffer());
  const { updateDate, rows } = parseDamodaranXls(buffer, config);

  logger.info(`[damodaran] ${config.name}: ${rows.length} industrias, fecha ${updateDate}`);
  const stored = await upsertDamodaranBatch(dataset, updateDate, rows);
  logger.info(`[damodaran] ${config.name}: ${stored} registros almacenados`);

  return { dataset, stored, updateDate };
}
