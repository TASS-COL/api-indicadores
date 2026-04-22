import * as XLSX from "xlsx";
import { env } from "../config/env";
import { IndicatorSample } from "./types";

const todayIso = () => new Date().toISOString();

const ensureUrl = (value: string | undefined, label: string): string => {
  if (!value) throw new Error(`${label} no configurada`);
  return value;
};

const parseNumberString = (value: unknown): string | null => {
  if (value === undefined || value === null) return null;
  const numeric =
    typeof value === "string"
      ? Number(value.replace(",", "."))
      : typeof value === "number"
        ? value
        : Number(value);
  if (!Number.isFinite(numeric)) return null;
  return numeric.toString();
};

const pickNumber = (payload: any, candidates: string[]): string | null => {
  for (const key of candidates) {
    const maybe = parseNumberString(payload?.[key]);
    if (maybe !== null) return maybe;
  }
  return null;
};

const fetchJson = async (url: string) => {
  const res = await fetch(url, { headers: { accept: "application/json" } });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Error ${res.status} al consultar ${url}: ${text}`);
  }
  return res.json();
};

const BROWSER_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36";

const fetchText = async (url: string, extraHeaders?: Record<string, string>) => {
  const res = await fetch(url, { headers: extraHeaders });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Error ${res.status} al consultar ${url}: ${text}`);
  }
  return res.text();
};

const fetchBuffer = async (
  url: string,
  extraHeaders?: Record<string, string>,
): Promise<Buffer> => {
  const res = await fetch(url, {
    headers: { "User-Agent": BROWSER_UA, ...extraHeaders },
  });
  if (!res.ok) throw new Error(`Error ${res.status} al consultar ${url}`);
  return Buffer.from(await res.arrayBuffer());
};

const fetchBufferOrNull = async (
  url: string,
  extraHeaders?: Record<string, string>,
): Promise<Buffer | null> => {
  const res = await fetch(url, {
    headers: { "User-Agent": BROWSER_UA, ...extraHeaders },
  });
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`Error ${res.status} al consultar ${url}`);
  return Buffer.from(await res.arrayBuffer());
};

const asObservedAt = (value?: string | Date | null) => {
  if (!value) return todayIso();
  const d = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(d.getTime())) return todayIso();
  return d.toISOString();
};

/** Retorna el último día del mes como string "YYYY-MM-DD" */
const lastDayOfMonth = (year: number, month: number): string => {
  return new Date(Date.UTC(year, month, 0)).toISOString().slice(0, 10);
};

/** Primer día del mes como string "YYYY-MM-DD" dado "YYYY-MM" */
const firstDayFromYearMonth = (yearMonth: string): string => `${yearMonth}-01`;

/** Último día del mes como string "YYYY-MM-DD" dado "YYYY-MM" */
const lastDayFromYearMonth = (yearMonth: string): string => {
  const [y, m] = yearMonth.split("-").map(Number);
  return lastDayOfMonth(y, m);
};

export async function fetchTrmDatosGov(): Promise<IndicatorSample[]> {
  const base = ensureUrl(env.indicators.trmDatosGovUrl, "TRM_DATOSGOV_URL");
  const day = new Date().toISOString().slice(0, 10);

  // Try today; fall back to most recent (weekends/holidays have no entry)
  let first: any;
  let usedFallback = false;

  const urlToday = new URL(base);
  urlToday.searchParams.set("$where", `vigenciadesde='${day}'`);
  const dataToday = await fetchJson(urlToday.toString());
  first = Array.isArray(dataToday) ? dataToday[0] : dataToday;

  if (!first || !pickNumber(first, ["valor", "value"])) {
    const urlLatest = new URL(base);
    urlLatest.searchParams.set("$order", "vigenciadesde DESC");
    urlLatest.searchParams.set("$limit", "1");
    const dataLatest = await fetchJson(urlLatest.toString());
    first = Array.isArray(dataLatest) ? dataLatest[0] : dataLatest;
    usedFallback = true;
  }

  const value = pickNumber(first, ["valor", "value"]);
  if (!value) throw new Error("No se encontró valor TRM en la respuesta de datos.gov");

  // Expandir rango vigenciadesde→vigenciahasta: un registro por día con el mismo valor.
  // Si es fallback (fin de semana/festivo), extender hasta hoy para cubrir el día actual.
  let fromStr = first?.vigenciadesde ? String(first.vigenciadesde).slice(0, 10) : day;
  let toStr   = first?.vigenciahasta ? String(first.vigenciahasta).slice(0, 10) : fromStr;

  if (usedFallback && toStr < day) toStr = day;

  const samples: IndicatorSample[] = [];
  const cursor = new Date(`${fromStr}T05:00:00Z`);
  const end    = new Date(`${toStr}T05:00:00Z`);

  while (cursor <= end) {
    const dateStr = cursor.toISOString().slice(0, 10);
    samples.push({
      indicator: "TRM",
      variant: "datos_gov",
      value_number: value,
      value_text: value,
      unit: "COP/USD",
      observed_at: `${dateStr}T05:00:00Z`,
      from_date: dateStr,
      to_date: toStr,
      source: base,
      metadata: first,
    });
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }

  return samples;
}

export async function fetchTrmVercel(): Promise<IndicatorSample[]> {
  const base = ensureUrl(env.indicators.trmVercelUrl, "TRM_VERCEL_URL");
  const url = new URL(base);
  const day = new Date().toISOString().slice(0, 10);
  url.searchParams.set("date", day);
  const data = await fetchJson(url.toString());
  const value = pickNumber(data, ["value", "valor"]);
  if (!value) throw new Error("No se encontró valor TRM en Vercel");
  const observed = data?.date || data?.vigenciadesde || `${day}T00:00:00Z`;

  return [
    {
      indicator: "TRM",
      variant: "vercel",
      value_number: value,
      value_text: value,
      unit: "COP/USD",
      observed_at: observed,
      from_date: data?.vigenciadesde ?? observed,
      to_date: data?.vigenciahasta ?? null,
      source: base,
      metadata: data,
    },
  ];
}

const parseBanrepDate = (raw: string | undefined): string => {
  if (!raw) return todayIso();
  const [d, m, y] = raw.split("/");
  if (!d || !m || !y) return todayIso();
  const iso = new Date(`${y}-${m}-${d}T00:00:00Z`);
  return Number.isNaN(iso.getTime()) ? todayIso() : iso.toISOString();
};

const IBR_VARIANTS: Record<number, string> = {
  241:   "overnight_nominal",
  242:   "1_mes_nominal",
  243:   "3_meses_nominal",
  15324: "overnight_efectiva",
  15325: "1_mes_efectiva",
  15326: "3_meses_efectiva",
  16560: "6_meses_nominal",
  16561: "6_meses_efectiva",
  16562: "12_meses_nominal",
  16563: "12_meses_efectiva",
};

const IBR_SERIES = Object.keys(IBR_VARIANTS).join(",");

export async function fetchIbr(): Promise<IndicatorSample[]> {
  const base = ensureUrl(env.indicators.banrepBaseUrl, "BANREP_BASE_URL");
  const url = `${base}${IBR_SERIES}`;
  const data = await fetchJson(url);
  const list: any[] = Array.isArray(data) ? data : [];
  const samples: IndicatorSample[] = [];

  for (const item of list) {
    const variant = IBR_VARIANTS[item.id as number];
    if (!variant) continue;
    const rawValor = item.valor ?? item.data?.[0]?.valor;
    const value = parseNumberString(rawValor);
    if (!value) continue;
    const rawFecha = item.fecha ?? item.data?.[0]?.fecha;
    const { data: _, ...itemMeta } = item;
    samples.push({
      indicator: "IBR",
      variant,
      value_number: value,
      unit: item.unidadCorta ?? "%",
      observed_at: parseBanrepDate(rawFecha),
      from_date: parseBanrepDate(rawFecha),
      source: url,
      metadata: itemMeta,
    });
  }

  if (!samples.length) throw new Error("No se encontraron datos IBR en BanRep");
  return samples;
}

const CDT_VARIANTS: Record<number, string> = {
  67: "180_dias",
  68: "360_dias",
};

const CDT_SERIES = Object.keys(CDT_VARIANTS).join(",");

export async function fetchCdt(): Promise<IndicatorSample[]> {
  const base = ensureUrl(env.indicators.banrepBaseUrl, "BANREP_BASE_URL");
  const url = `${base}${CDT_SERIES}`;
  const data = await fetchJson(url);
  const list: any[] = Array.isArray(data) ? data : [];
  const samples: IndicatorSample[] = [];

  for (const item of list) {
    const variant = CDT_VARIANTS[item.id as number];
    if (!variant) continue;
    const rawValor = item.valor ?? item.data?.[0]?.valor;
    const value = parseNumberString(rawValor);
    if (!value) continue;
    const rawFecha = item.fecha ?? item.data?.[0]?.fecha;
    const { data: _, ...itemMeta } = item;
    samples.push({
      indicator: "CDT",
      variant,
      value_number: value,
      unit: item.unidadCorta ?? "%",
      observed_at: parseBanrepDate(rawFecha),
      from_date: parseBanrepDate(rawFecha),
      source: url,
      metadata: itemMeta,
    });
  }

  if (!samples.length) throw new Error("No se encontraron datos CDT en BanRep");
  return samples;
}

export async function fetchDtf(): Promise<IndicatorSample[]> {
  const base = ensureUrl(env.indicators.banrepBaseUrl, "BANREP_BASE_URL");
  const url = `${base}65`;
  const data = await fetchJson(url);
  const list: any[] = Array.isArray(data) ? data : [];
  const item = list.find((i) => i.id === 65) ?? list[0];
  if (!item) throw new Error("No se encontraron datos DTF en BanRep");
  const rawValor = item.valor ?? item.data?.[0]?.valor;
  const value = parseNumberString(rawValor);
  if (!value) throw new Error("No se encontró valor numérico para DTF");
  const rawFecha = item.fecha ?? item.data?.[0]?.fecha;
  const { data: _, ...itemMeta } = item;
  return [
    {
      indicator: "DTF",
      variant: "90_dias",
      value_number: value,
      unit: item.unidadCorta ?? "%",
      observed_at: parseBanrepDate(rawFecha),
      from_date: parseBanrepDate(rawFecha),
      source: url,
      metadata: itemMeta,
    },
  ];
}

export async function fetchTipm(): Promise<IndicatorSample[]> {
  const base = ensureUrl(env.indicators.banrepBaseUrl, "BANREP_BASE_URL");
  const url = `${base}59`;
  const data = await fetchJson(url);
  const list: any[] = Array.isArray(data) ? data : [];
  const item = list.find((i) => i.id === 59) ?? list[0];
  if (!item) throw new Error("No se encontraron datos de tasa de intervención en BanRep");
  const rawValor = item.valor ?? item.data?.[0]?.valor;
  const value = parseNumberString(rawValor);
  if (!value) throw new Error("No se encontró valor numérico para tasa de intervención");
  const rawFecha = item.fecha ?? item.data?.[0]?.fecha;
  const { data: _, ...itemMeta } = item;
  return [
    {
      indicator: "TASA_INTERVENCION",
      variant: "banrep",
      value_number: value,
      unit: item.unidadCorta ?? "%",
      observed_at: parseBanrepDate(rawFecha),
      from_date: parseBanrepDate(rawFecha),
      source: url,
      metadata: itemMeta,
    },
  ];
}

export async function fetchTib(): Promise<IndicatorSample[]> {
  const base = ensureUrl(env.indicators.banrepBaseUrl, "BANREP_BASE_URL");
  const url = `${base}89`;
  const data = await fetchJson(url);
  const list: any[] = Array.isArray(data) ? data : [];
  const item = list.find((i) => i.id === 89) ?? list[0];
  if (!item) throw new Error("No se encontraron datos de TIB en BanRep");

  const unit = item.unidadCorta ?? "%";
  const entries: [number, number][] = Array.isArray(item.data) ? item.data : [];

  const latest = entries[entries.length - 1];
  if (!latest) throw new Error("No se encontraron datos TIB en BanRep");

  const [ts, val] = latest;
  const value = parseNumberString(val);
  if (!value) throw new Error("No se encontró valor numérico para TIB");
  const dt = new Date(ts);
  const dateStr = `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, "0")}-${String(dt.getDate()).padStart(2, "0")}`;

  return [
    {
      indicator: "TIB",
      variant: "diaria",
      value_number: value,
      unit,
      observed_at: `${dateStr}T05:00:00Z`,
      from_date: dateStr,
      source: url,
      metadata: { serieId: item.id, unidadCorta: item.unidadCorta },
    },
  ];
}

export async function fetchEurUsdBce(): Promise<IndicatorSample[]> {
  const url = ensureUrl(env.indicators.bceUrl, "BCE_URL");
  const html = await fetchText(url);
  const match = html.match(/id=["']USD["'][\s\S]*?class=["']rate["'][^>]*>([^<]+)</i);
  const raw = match?.[1]?.trim();
  const value = parseNumberString(raw ?? "");
  if (!value) throw new Error("No se pudo extraer la tasa EUR/USD");

  return [
    {
      indicator: "EUR_USD",
      variant: "bce",
      value_number: value,
      value_text: raw ?? value,
      unit: "EUR/USD",
      observed_at: todayIso(),
      from_date: new Date().toISOString().slice(0, 10),
      source: url,
      metadata: { snippet: match?.[0]?.slice(0, 200) },
    },
  ];
}

const IPC_VARIANTS: Record<string, string> = {
  Mensual:   "mensual",
  Acumulado: "anio_corrido",
  "12Meses": "doce_meses",
};

export async function fetchIpc(): Promise<IndicatorSample[]> {
  const base = ensureUrl(env.indicators.ipcBaseUrl, "IPC_BASE_URL");

  const now = new Date();
  const prev = new Date(now.getFullYear(), now.getMonth() - 1, 1);
  const periodoConsultado =
    prev.getFullYear().toString() + String(prev.getMonth() + 1).padStart(2, "0");

  const observed = `${periodoConsultado.slice(0, 4)}-${periodoConsultado.slice(4, 6)}-01T00:00:00.000Z`;
  const samples: IndicatorSample[] = [];

  for (const [segment, variant] of Object.entries(IPC_VARIANTS)) {
    const url = `${base}/${periodoConsultado}/${segment}`;
    const data = await fetchJson(url);
    if (Array.isArray(data) && data.length === 0) {
      console.warn(`IPC [${variant}]: sin datos para ${periodoConsultado}, el DANE aún no ha publicado.`);
      continue;
    }
    const first = Array.isArray(data) ? data[0] : data;
    const value = parseNumberString(first?.value);
    if (!value) continue;
    samples.push({
      indicator: "IPC",
      variant,
      value_number: value,
      unit: "%",
      observed_at: observed,
      from_date: `${periodoConsultado.slice(0, 4)}-${periodoConsultado.slice(4, 6)}-01`,
      to_date: lastDayOfMonth(Number(periodoConsultado.slice(0, 4)), Number(periodoConsultado.slice(4, 6))),
      source: url,
      metadata: { periodoConsultado, segment, raw: first },
    });
  }

  return samples;
}

const USURA_MODALITIES: { keywords: string[]; variant: string }[] = [
  { keywords: ["POPULAR", "PRODUCTIVO", "RURAL"],  variant: "popular_productivo_rural"  },
  { keywords: ["POPULAR", "PRODUCTIVO", "URBANO"], variant: "popular_productivo_urbano" },
  { keywords: ["PRODUCTIVO", "RURAL"],             variant: "productivo_rural"           },
  { keywords: ["PRODUCTIVO", "URBANO"],            variant: "productivo_urbano"          },
  { keywords: ["PRODUCTIVO", "MAYOR"],             variant: "productivo_mayor_monto"     },
  { keywords: ["BAJO MONTO"],                      variant: "consumo_bajo_monto"         },
  { keywords: ["CONSUMO", "ORDINARIO"],            variant: "consumo_ordinario"          },
];

function detectUsuraModality(path: string): string | null {
  const upper = path.toUpperCase();
  for (const { keywords, variant } of USURA_MODALITIES) {
    if (keywords.every((k) => upper.includes(k))) return variant;
  }
  return null;
}

export async function fetchUsuraAndIbc(): Promise<IndicatorSample[]> {
  const url = ensureUrl(env.indicators.usuraAndIbcUrl, "USURA_IBC_URL");

  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 30_000);
  let res: Response;
  try {
    res = await fetch(url, {
      signal: ctrl.signal,
      headers: {
        "User-Agent": `${BROWSER_UA} Chrome/120`,
        Accept:
          "application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
        "Accept-Language": "es-CO,es;q=0.9",
        Referer: "https://www.superfinanciera.gov.co/",
      },
    });
  } finally {
    clearTimeout(timer);
  }
  if (!res!.ok) throw new Error(`Error ${res!.status} al descargar Excel Usura`);

  const buffer = Buffer.from(await res.arrayBuffer());
  const workbook = XLSX.read(buffer, { type: "buffer", cellDates: true });
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  if (!sheet) throw new Error("Excel Usura: no se encontró hoja");
  const range = XLSX.utils.decode_range(sheet["!ref"] ?? "A1:A1");

  const cellMap = new Map<string, unknown>();
  for (const addr of Object.keys(sheet).filter((k) => k[0] !== "!")) {
    cellMap.set(addr, sheet[addr].v);
  }
  for (const merge of sheet["!merges"] ?? []) {
    const value = cellMap.get(XLSX.utils.encode_cell(merge.s));
    if (value == null) continue;
    for (let r = merge.s.r; r <= merge.e.r; r++) {
      for (let c = merge.s.c; c <= merge.e.c; c++) {
        cellMap.set(XLSX.utils.encode_cell({ r, c }), value);
      }
    }
  }

  const getCell = (r: number, c: number): string => {
    const v = cellMap.get(XLSX.utils.encode_cell({ r, c }));
    return v != null ? String(v).trim() : "";
  };
  const getCellRaw = (r: number, c: number): unknown =>
    cellMap.get(XLSX.utils.encode_cell({ r, c }));

  let headerEnd = -1;
  for (let r = range.e.r; r >= 0; r--) {
    for (let c = 0; c <= range.e.c; c++) {
      if (getCell(r, c).toUpperCase().includes("USURA")) {
        headerEnd = r;
        break;
      }
    }
    if (headerEnd >= 0) break;
  }
  if (headerEnd < 0) throw new Error("Excel Usura: no se encontró encabezado 'USURA'");

  let headerStart = headerEnd;
  for (let r = headerEnd - 1; r >= Math.max(0, headerEnd - 20); r--) {
    const rowEmpty = Array.from(
      { length: Math.min(range.e.c + 1, 16) },
      (_, c) => !getCell(r, c)
    ).every(Boolean);
    if (rowEmpty) break;
    headerStart = r;
  }

  const columnPaths: string[] = [];
  for (let c = 0; c <= range.e.c; c++) {
    const parts: string[] = [];
    for (let r = headerStart; r <= headerEnd; r++) {
      const v = getCell(r, c);
      if (v) parts.push(v.toUpperCase());
    }
    columnPaths.push(parts.join("|"));
  }

  type RateType = "USURA" | "IBC";
  type RateCol = { col: number; variant: string; type: RateType };
  const rateColumns: RateCol[] = [];

  for (let c = 0; c <= range.e.c; c++) {
    const path = columnPaths[c];
    const isUsura = path.includes("USURA");
    const isIbc = !isUsura && path.includes("BANCARIO");
    if (!isUsura && !isIbc) continue;

    const variant = detectUsuraModality(path);
    if (!variant) continue;

    const type: RateType = isUsura ? "USURA" : "IBC";
    if (rateColumns.some((rc) => rc.variant === variant && rc.type === type)) continue;
    rateColumns.push({ col: c, variant, type });
  }

  const usuraCols = rateColumns.filter((rc) => rc.type === "USURA");
  if (!usuraCols.length) {
    throw new Error(
      "Excel Usura: no se encontraron columnas USURA con modalidad conocida. " +
        `Paths con USURA: ${columnPaths.filter((p) => p.includes("USURA")).slice(0, 3).join(" || ")}`
    );
  }

  let lastDataRow = -1;
  for (let r = range.e.r; r > headerEnd; r--) {
    for (const { col } of usuraCols) {
      const v = parseNumberString(getCell(r, col));
      if (v !== null && Number(v) > 0) {
        lastDataRow = r;
        break;
      }
    }
    if (lastDataRow >= 0) break;
  }
  if (lastDataRow < 0) {
    lastDataRow = range.e.r;
    while (lastDataRow > headerEnd && !getCell(lastDataRow, 0) && !getCell(lastDataRow, 1))
      lastDataRow--;
  }
  if (lastDataRow <= headerEnd) throw new Error("Excel Usura: no hay filas de datos");

  const toDateObj = (raw: unknown): Date | null => {
    if (raw instanceof Date && !isNaN(raw.getTime())) return raw;
    if (typeof raw === "string" && raw) {
      const d = new Date(raw);
      return isNaN(d.getTime()) ? null : d;
    }
    return null;
  };

  const hastaDate =
    toDateObj(getCellRaw(lastDataRow, 1)) ??
    toDateObj(getCellRaw(lastDataRow, 0)) ??
    new Date();
  const year = hastaDate.getFullYear();
  const month = hastaDate.getMonth();

  const normDesde = new Date(Date.UTC(year, month, 1));
  const normHasta = new Date(Date.UTC(year, month + 1, 0));
  const observedAt = normHasta.toISOString();
  const desdeStr = normDesde.toISOString().slice(0, 10);
  const hastaStr = normHasta.toISOString().slice(0, 10);

  const now = new Date();
  const isCurrentMonth = year === now.getFullYear() && month === now.getMonth();

  const samples: IndicatorSample[] = [];
  for (const { col, variant, type } of rateColumns) {
    const value = parseNumberString(getCell(lastDataRow, col));
    if (!value) continue;
    samples.push({
      indicator: type,
      variant,
      value_number: value,
      unit: "%",
      observed_at: observedAt,
      from_date: desdeStr,
      to_date: hastaStr,
      source: url,
      metadata: {
        desde: desdeStr,
        hasta: hastaStr,
        data_year: year,
        data_month: month + 1,
        is_current_month: isCurrentMonth,
        column_index: col,
      },
    });
  }

  if (!samples.some((s) => s.indicator === "USURA")) {
    throw new Error("Excel Usura: no se encontraron valores USURA en la última fila con datos");
  }
  return samples;
}

export async function fetchUsuraAndIbcHistory(): Promise<IndicatorSample[]> {
  const url = ensureUrl(env.indicators.usuraAndIbcUrl, "USURA_IBC_URL");

  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), 30_000);
  let res: Response;
  try {
    res = await fetch(url, {
      signal: ctrl.signal,
      headers: {
        "User-Agent": `${BROWSER_UA} Chrome/120`,
        Accept: "application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
        "Accept-Language": "es-CO,es;q=0.9",
        Referer: "https://www.superfinanciera.gov.co/",
      },
    });
  } finally {
    clearTimeout(timer);
  }
  if (!res!.ok) throw new Error(`Error ${res!.status} al descargar Excel Usura`);

  const buffer = Buffer.from(await res.arrayBuffer());
  const workbook = XLSX.read(buffer, { type: "buffer", cellDates: true });
  const sheet = workbook.Sheets[workbook.SheetNames[0]];
  if (!sheet) throw new Error("Excel Usura: no se encontró hoja");
  const range = XLSX.utils.decode_range(sheet["!ref"] ?? "A1:A1");

  const cellMap = new Map<string, unknown>();
  for (const addr of Object.keys(sheet).filter((k) => k[0] !== "!")) {
    cellMap.set(addr, sheet[addr].v);
  }
  for (const merge of sheet["!merges"] ?? []) {
    const value = cellMap.get(XLSX.utils.encode_cell(merge.s));
    if (value == null) continue;
    for (let r = merge.s.r; r <= merge.e.r; r++) {
      for (let c = merge.s.c; c <= merge.e.c; c++) {
        cellMap.set(XLSX.utils.encode_cell({ r, c }), value);
      }
    }
  }

  const getCell = (r: number, c: number): string => {
    const v = cellMap.get(XLSX.utils.encode_cell({ r, c }));
    return v != null ? String(v).trim() : "";
  };
  const getCellRaw = (r: number, c: number): unknown =>
    cellMap.get(XLSX.utils.encode_cell({ r, c }));

  let headerEnd = -1;
  for (let r = range.e.r; r >= 0; r--) {
    for (let c = 0; c <= range.e.c; c++) {
      if (getCell(r, c).toUpperCase().includes("USURA")) { headerEnd = r; break; }
    }
    if (headerEnd >= 0) break;
  }
  if (headerEnd < 0) throw new Error("Excel Usura hist: no se encontró encabezado");

  let headerStart = headerEnd;
  for (let r = headerEnd - 1; r >= Math.max(0, headerEnd - 20); r--) {
    const rowEmpty = Array.from({ length: Math.min(range.e.c + 1, 16) }, (_, c) => !getCell(r, c)).every(Boolean);
    if (rowEmpty) break;
    headerStart = r;
  }

  const columnPaths: string[] = [];
  for (let c = 0; c <= range.e.c; c++) {
    const parts: string[] = [];
    for (let r = headerStart; r <= headerEnd; r++) {
      const v = getCell(r, c);
      if (v) parts.push(v.toUpperCase());
    }
    columnPaths.push(parts.join("|"));
  }

  type RateCol = { col: number; variant: string; type: "USURA" | "IBC" };
  const rateColumns: RateCol[] = [];
  for (let c = 0; c <= range.e.c; c++) {
    const path = columnPaths[c];
    const isUsura = path.includes("USURA");
    const isIbc = !isUsura && path.includes("BANCARIO");
    if (!isUsura && !isIbc) continue;
    const variant = detectUsuraModality(path);
    if (!variant) continue;
    const type = isUsura ? "USURA" as const : "IBC" as const;
    if (rateColumns.some((rc) => rc.variant === variant && rc.type === type)) continue;
    rateColumns.push({ col: c, variant, type });
  }

  if (!rateColumns.length) throw new Error("Excel Usura hist: no se encontraron columnas");

  const toDateObj = (raw: unknown): Date | null => {
    if (raw instanceof Date && !isNaN(raw.getTime())) return raw;
    if (typeof raw === "string" && raw) {
      const d = new Date(raw);
      return isNaN(d.getTime()) ? null : d;
    }
    return null;
  };

  const samples: IndicatorSample[] = [];
  for (let r = headerEnd + 1; r <= range.e.r; r++) {
    const hasValue = rateColumns.some(({ col }) => {
      const v = parseNumberString(getCell(r, col));
      return v !== null && Number(v) > 0;
    });
    if (!hasValue) continue;

    const hastaDate = toDateObj(getCellRaw(r, 1)) ?? toDateObj(getCellRaw(r, 0));
    if (!hastaDate) continue;

    const year = hastaDate.getFullYear();
    const month = hastaDate.getMonth();
    const normDesde = new Date(Date.UTC(year, month, 1));
    const normHasta = new Date(Date.UTC(year, month + 1, 0));
    const desdeStr = normDesde.toISOString().slice(0, 10);
    const hastaStr = normHasta.toISOString().slice(0, 10);

    for (const { col, variant, type } of rateColumns) {
      const value = parseNumberString(getCell(r, col));
      if (!value || Number(value) <= 0) continue;
      samples.push({
        indicator: type,
        variant,
        value_number: value,
        unit: "%",
        observed_at: normHasta.toISOString(),
        from_date: desdeStr,
        to_date: hastaStr,
        source: url,
        metadata: { desde: desdeStr, hasta: hastaStr, row: r },
      });
    }
  }

  return samples;
}

export async function fetchSofrMarkets(): Promise<IndicatorSample[]> {
  const samples: IndicatorSample[] = [];

  if (env.indicators.sofrMarketsUrl1) {
    const data = await fetchJson(env.indicators.sofrMarketsUrl1);
    const ref = Array.isArray(data?.refRates) ? data.refRates[0] : data;
    const value =
      pickNumber(ref, ["percentRate", "rate"]) ?? pickNumber(data, ["percentRate", "rate"]);
    if (value) {
      samples.push({
        indicator: "SOFR",
        variant: "market_1",
        value_number: value,
        unit: ref?.rateType || ref?.unit || "%",
        observed_at: ref?.effectiveDate || ref?.date || todayIso(),
        from_date: ref?.effectiveDate || ref?.date || new Date().toISOString().slice(0, 10),
        source: env.indicators.sofrMarketsUrl1,
        metadata: ref ?? data,
      });
    }
  }

  if (env.indicators.sofrMarketsUrl2) {
    const data = await fetchJson(env.indicators.sofrMarketsUrl2);
    const ref = Array.isArray(data?.refRates) ? data.refRates[0] : data;
    const value =
      pickNumber(ref, ["percentRate", "rate"]) ?? pickNumber(data, ["percentRate", "rate"]);
    if (value) {
      samples.push({
        indicator: "SOFR",
        variant: "market_2",
        value_number: value,
        unit: ref?.rateType || ref?.unit || "%",
        observed_at: ref?.effectiveDate || ref?.date || todayIso(),
        from_date: ref?.effectiveDate || ref?.date || new Date().toISOString().slice(0, 10),
        source: env.indicators.sofrMarketsUrl2,
        metadata: ref ?? data,
      });
    }
  }

  if (!samples.length) throw new Error("SOFR_MARKETS no configurado");
  return samples;
}

export async function fetchCpiUsa(): Promise<IndicatorSample[]> {
  const url = ensureUrl(env.indicators.cpiUsaUrl, "CPI_USA_URL");
  const data = await fetchJson(url);

  if (data?.status !== "REQUEST_SUCCEEDED") {
    throw new Error(`BLS API respondió con status: ${data?.status}`);
  }

  const series = data?.Results?.series;
  if (!Array.isArray(series) || !series.length) {
    throw new Error("BLS API: no se encontraron series en la respuesta");
  }

  const entries: { year: number; month: number; value: number }[] = [];
  for (const item of series[0].data) {
    if (!item.period?.startsWith("M")) continue;
    const val = parseNumberString(item.value);
    if (!val) continue;
    entries.push({
      year: Number(item.year),
      month: Number(item.period.slice(1)),
      value: Number(val),
    });
  }

  if (!entries.length) throw new Error("BLS API: sin datos CPI válidos");

  const indexMap = new Map<string, number>();
  for (const e of entries) {
    indexMap.set(`${e.year}-${e.month}`, e.value);
  }

  entries.sort((a, b) => a.year - b.year || a.month - b.month);

  const latest = entries[entries.length - 1];
  const lastYearValue = indexMap.get(`${latest.year - 1}-${latest.month}`);
  if (!lastYearValue) throw new Error("BLS API: no se encontró dato del año anterior para CPI YoY");

  const yoy = ((latest.value - lastYearValue) / lastYearValue) * 100;
  const observed = `${latest.year}-${String(latest.month).padStart(2, "0")}-01T05:00:00Z`;

  return [
    {
      indicator: "CPI",
      variant: "mensual",
      value_number: yoy.toFixed(2),
      unit: "%",
      observed_at: observed,
      from_date: `${latest.year}-${String(latest.month).padStart(2, "0")}-01`,
      to_date: lastDayOfMonth(latest.year, latest.month),
      source: url,
      metadata: {
        index_current: latest.value,
        index_previous_year: lastYearValue,
      },
    },
  ];
}

const DANE_IPP_MONTHS: Record<number, string> = {
  1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
  7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic",
};

const DANE_IPP_DEFAULT_BASE = "https://www.dane.gov.co/files/operaciones/IPP";

function buildIppUrl(year: number, month: number): string {
  const base = env.indicators.daneIppBaseUrl || DANE_IPP_DEFAULT_BASE;
  const mes = DANE_IPP_MONTHS[month];
  return `${base}/anex-IPP-${mes}${year}.xlsx`;
}

async function fetchIppExcel(year: number, month: number): Promise<Buffer | null> {
  const url = buildIppUrl(year, month);
  return fetchBufferOrNull(url, {
    Accept: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
  });
}

function parseIppExcel(buffer: Buffer): {
  indices: { month: string; value: number }[];
  variaciones: { mensual: number; anioCorrido: number; anual: number };
} {
  const wb = XLSX.read(buffer, { type: "buffer" });

  const sheet11 = wb.Sheets["1.1"];
  if (!sheet11) throw new Error("IPP Excel: no se encontró hoja 1.1");
  const data11 = XLSX.utils.sheet_to_json(sheet11, { header: 1, defval: "" }) as any[][];

  let headerIdx = -1;
  for (let i = 0; i < 10; i++) {
    if (data11[i]?.some((c: any) => String(c).includes("Dic-14"))) {
      headerIdx = i;
      break;
    }
  }
  if (headerIdx < 0) throw new Error("IPP Excel: no se encontraron headers en hoja 1.1");

  const headers = data11[headerIdx] as string[];
  const totalRow = data11.find((r: any[]) => r[0] === "TOTAL" && r[2] === "TOTAL");
  if (!totalRow) throw new Error("IPP Excel: no se encontró fila TOTAL en hoja 1.1");

  const minYear = new Date().getFullYear() - 1;
  const indices: { month: string; value: number }[] = [];
  for (let c = 3; c < headers.length; c++) {
    const h = String(headers[c] || "").trim();
    const val = totalRow[c];
    if (!h || val === "" || val === null || val === undefined) continue;
    const match = h.match(/^(\w{3})-(\d{2})/);
    if (!match) continue;
    const monthNames: Record<string, number> = {
      Ene: 1, Feb: 2, Mar: 3, Abr: 4, May: 5, Jun: 6,
      Jul: 7, Ago: 8, Sep: 9, Oct: 10, Nov: 11, Dic: 12,
    };
    const m = monthNames[match[1]];
    const y = 2000 + Number(match[2]);
    if (!m || y < minYear) continue;
    const numVal = typeof val === "number" ? val : Number(String(val).replace(",", "."));
    if (!Number.isFinite(numVal)) continue;
    const iso = `${y}-${String(m).padStart(2, "0")}`;
    const rounded = Math.round(numVal * 100) / 100;
    const existing = indices.findIndex((i) => i.month === iso);
    if (existing >= 0) {
      indices[existing].value = rounded;
    } else {
      indices.push({ month: iso, value: rounded });
    }
  }

  const sheet12 = wb.Sheets["1.2"];
  if (!sheet12) throw new Error("IPP Excel: no se encontró hoja 1.2");
  const data12 = XLSX.utils.sheet_to_json(sheet12, { header: 1, defval: "" }) as any[][];
  const totalRow12 = data12.find((r: any[]) => r[0] === "TOTAL" && r[2] === "TOTAL");
  if (!totalRow12) throw new Error("IPP Excel: no se encontró fila TOTAL en hoja 1.2");

  const mensual = Number(totalRow12[3]) || 0;
  const anioCorrido = Number(totalRow12[6]) || 0;
  const anual = Number(totalRow12[9]) || 0;

  return { indices, variaciones: { mensual, anioCorrido, anual } };
}

export async function fetchIppDane(): Promise<IndicatorSample[]> {
  const now = new Date();
  let buffer: Buffer | null = null;
  let targetYear = now.getFullYear();
  let targetMonth = now.getMonth() + 1;

  for (let attempt = 0; attempt < 3; attempt++) {
    buffer = await fetchIppExcel(targetYear, targetMonth);
    if (buffer) break;
    targetMonth--;
    if (targetMonth < 1) { targetMonth = 12; targetYear--; }
  }

  if (!buffer) throw new Error("No se encontró Excel IPP del DANE para los últimos 3 meses");

  const { indices, variaciones } = parseIppExcel(buffer);
  const url = buildIppUrl(targetYear, targetMonth);
  const samples: IndicatorSample[] = [];

  const lastIndex = indices[indices.length - 1];
  if (lastIndex) {
    samples.push({
      indicator: "IPP",
      variant: "mensual",
      value_number: lastIndex.value.toFixed(2),
      unit: "",
      observed_at: `${lastIndex.month}-01T05:00:00Z`,
      from_date: firstDayFromYearMonth(lastIndex.month),
      to_date: lastDayFromYearMonth(lastIndex.month),
      source: url,
      metadata: { base: "dic-2014=100", tipo: "produccion_nacional" },
    });
  }

  const lastMonth = lastIndex?.month;
  if (lastMonth) {
    const observed = `${lastMonth}-01T05:00:00Z`;

    samples.push({
      indicator: "IPP",
      variant: "variacion_mensual",
      value_number: variaciones.mensual.toFixed(2),
      unit: "%",
      observed_at: observed,
      from_date: firstDayFromYearMonth(lastMonth),
      to_date: lastDayFromYearMonth(lastMonth),
      source: url,
      metadata: { tipo: "produccion_nacional" },
    });

    samples.push({
      indicator: "IPP",
      variant: "variacion_anio_corrido",
      value_number: variaciones.anioCorrido.toFixed(2),
      unit: "%",
      observed_at: observed,
      from_date: firstDayFromYearMonth(lastMonth),
      to_date: lastDayFromYearMonth(lastMonth),
      source: url,
      metadata: { tipo: "produccion_nacional" },
    });

    samples.push({
      indicator: "IPP",
      variant: "variacion_anual",
      value_number: variaciones.anual.toFixed(2),
      unit: "%",
      observed_at: observed,
      from_date: firstDayFromYearMonth(lastMonth),
      to_date: lastDayFromYearMonth(lastMonth),
      source: url,
      metadata: { tipo: "produccion_nacional" },
    });
  }

  return samples;
}

export async function fetchIppDaneHistory(
  fromYear: number,
  fromMonth: number,
): Promise<IndicatorSample[]> {
  const now = new Date();
  const toYear = now.getFullYear();
  const toMonth = now.getMonth() + 1;

  const allSamples: IndicatorSample[] = [];

  let year = fromYear;
  let month = fromMonth;

  while (year < toYear || (year === toYear && month <= toMonth)) {
    const url = buildIppUrl(year, month);
    const buffer = await fetchIppExcel(year, month);

    if (!buffer) {
      console.warn(`[IPP history] Excel no disponible: ${url}`);
      month++;
      if (month > 12) { month = 1; year++; }
      continue;
    }

    let parsed: ReturnType<typeof parseIppExcel>;
    try {
      parsed = parseIppExcel(buffer);
    } catch (err) {
      console.warn(`[IPP history] Error parseando ${url}: ${err}`);
      month++;
      if (month > 12) { month = 1; year++; }
      continue;
    }

    const { indices, variaciones } = parsed;
    const isoMonth = `${year}-${String(month).padStart(2, "0")}`;
    const observed = `${isoMonth}-01T05:00:00Z`;

    const indexEntry = indices.find((i) => i.month === isoMonth)
      ?? indices[indices.length - 1];

    if (indexEntry) {
      allSamples.push({
        indicator: "IPP",
        variant: "mensual",
        value_number: indexEntry.value.toFixed(2),
        unit: "",
        observed_at: `${indexEntry.month}-01T05:00:00Z`,
        source: url,
        metadata: { base: "dic-2014=100", tipo: "produccion_nacional" },
      });
    }

    allSamples.push(
      { indicator: "IPP", variant: "variacion_mensual",     value_number: variaciones.mensual.toFixed(2),     unit: "%", observed_at: observed, source: url, metadata: { tipo: "produccion_nacional" } },
      { indicator: "IPP", variant: "variacion_anio_corrido", value_number: variaciones.anioCorrido.toFixed(2), unit: "%", observed_at: observed, source: url, metadata: { tipo: "produccion_nacional" } },
      { indicator: "IPP", variant: "variacion_anual",        value_number: variaciones.anual.toFixed(2),        unit: "%", observed_at: observed, source: url, metadata: { tipo: "produccion_nacional" } },
    );

    month++;
    if (month > 12) { month = 1; year++; }
  }

  return allSamples;
}

const DANE_IPC_DEFAULT_BASE = "https://www.dane.gov.co/files/operaciones/IPC";

const DANE_IPC_MONTHS: Record<number, string> = {
  1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
  7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic",
};

const MONTH_ROWS: Record<string, number> = {
  Enero: 1, Febrero: 2, Marzo: 3, Abril: 4, Mayo: 5, Junio: 6,
  Julio: 7, Agosto: 8, Septiembre: 9, Octubre: 10, Noviembre: 11, Diciembre: 12,
};

function buildDaneIpcUrl(year: number, month: number, suffix: string): string {
  const base = env.indicators.daneIpcBaseUrl || DANE_IPC_DEFAULT_BASE;
  const mes = DANE_IPC_MONTHS[month];
  return `${base}/${mes}${year}/anex-IPC-${suffix}-${mes}${year}.xlsx`;
}

export async function fetchInflacionTotal(): Promise<IndicatorSample[]> {
  const now = new Date();
  let targetYear = now.getFullYear();
  let targetMonth = now.getMonth() + 1;

  let buffer: Buffer | null = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    const url = buildDaneIpcUrl(targetYear, targetMonth, "Indices");
    buffer = await fetchBufferOrNull(url);
    if (buffer) break;
    targetMonth--;
    if (targetMonth < 1) { targetMonth = 12; targetYear--; }
  }

  if (!buffer) throw new Error("No se encontró Excel IPC Índices del DANE para los últimos 3 meses");

  const sourceUrl = buildDaneIpcUrl(targetYear, targetMonth, "Indices");

  const wb = XLSX.read(buffer, { type: "buffer" });
  const data = XLSX.utils.sheet_to_json(
    wb.Sheets["IndicesIPC"], { header: 1, defval: "" },
  ) as any[][];

  const minYear = now.getFullYear() - 1;
  let headerRowIdx = -1;
  for (let i = 0; i < 15; i++) {
    if (data[i]?.some((c: any) => c === minYear || c === minYear + 1)) {
      headerRowIdx = i;
      break;
    }
  }
  if (headerRowIdx < 0) throw new Error("IPC Excel: no se encontraron headers de años");

  const yearHeaders = data[headerRowIdx] as any[];

  const latestYear = Math.max(...yearHeaders.filter((y: any) => Number.isFinite(Number(y)) && Number(y) >= minYear).map(Number));
  const latestYearCol = yearHeaders.indexOf(latestYear);
  if (latestYearCol < 0) throw new Error("IPC Excel: no se encontró columna del año actual");

  const prevYearCol = yearHeaders.indexOf(latestYear - 1);

  let latestSample: IndicatorSample | null = null;
  for (let rowOffset = 12; rowOffset >= 1; rowOffset--) {
    const row = data[headerRowIdx + rowOffset];
    if (!row) continue;
    const monthName = String(row[0]).trim();
    const monthNum = MONTH_ROWS[monthName];
    if (!monthNum) continue;

    const currentVal = Number(row[latestYearCol]);
    if (!currentVal || !Number.isFinite(currentVal)) continue;
    if (prevYearCol < 0) continue;
    const prevVal = Number(row[prevYearCol]);
    if (!prevVal || !Number.isFinite(prevVal)) continue;

    const inflacionAnual = ((currentVal / prevVal) - 1) * 100;
    const observed = `${latestYear}-${String(monthNum).padStart(2, "0")}-01T05:00:00Z`;

    latestSample = {
      indicator: "INFLACION_TOTAL",
      variant: "mensual",
      value_number: inflacionAnual.toFixed(2),
      unit: "%",
      observed_at: observed,
      from_date: `${latestYear}-${String(monthNum).padStart(2, "0")}-01`,
      to_date: lastDayOfMonth(latestYear, monthNum),
      source: sourceUrl,
      metadata: {
        indice_actual: currentVal,
        indice_anterior: prevVal,
      },
    };
    break;
  }

  if (!latestSample) throw new Error("IPC Excel: no se pudo calcular inflación total");
  return [latestSample];
}

function parseUvrSeed(): { year: number; month: number; day: number; value: number } {
  const raw = env.indicators.uvrSeedDate;
  if (raw) {
    const [y, m, d] = raw.split("-").map(Number);
    if (y && m && d) {
      const val = Number(env.indicators.uvrSeedValue);
      if (Number.isFinite(val)) return { year: y, month: m, day: d, value: val };
    }
  }
  return { year: 2026, month: 3, day: 15, value: 403.0301 };
}

async function getIpcMonthlyVariations(): Promise<Map<string, number>> {
  const now = new Date();
  let targetYear = now.getFullYear();
  let targetMonth = now.getMonth() + 1;

  let buffer: Buffer | null = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    const mes = DANE_IPC_MONTHS[targetMonth];
    if (!mes) { targetMonth--; if (targetMonth < 1) { targetMonth = 12; targetYear--; } continue; }
    const url = buildDaneIpcUrl(targetYear, targetMonth, "Variacion");
    buffer = await fetchBufferOrNull(url);
    if (buffer) break;
    targetMonth--;
    if (targetMonth < 1) { targetMonth = 12; targetYear--; }
  }

  if (!buffer) throw new Error("UVR: no se pudo descargar Excel IPC Variaciones del DANE");

  const wb = XLSX.read(buffer, { type: "buffer" });
  const data = XLSX.utils.sheet_to_json(wb.Sheets["VarNal"], { header: 1, defval: "" }) as any[][];

  const headers = data[6] as any[];
  const variations = new Map<string, number>();

  const monthOrder = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"];

  for (let row = 7; row <= 18; row++) {
    const monthName = String(data[row][0]).trim();
    const monthIdx = monthOrder.indexOf(monthName);
    if (monthIdx < 0) continue;
    const monthNum = monthIdx + 1;

    for (let col = 1; col < headers.length; col++) {
      const year = Number(headers[col]);
      if (!year || year < 2024) continue;
      const val = Number(data[row][col]);
      if (!Number.isFinite(val) || val === 0) continue;
      variations.set(`${year}-${String(monthNum).padStart(2, "0")}`, val / 100);
    }
  }

  return variations;
}

function daysInPeriod(year: number, month: number): number {
  const start = new Date(year, month - 1, 16);
  const endMonth = month === 12 ? 1 : month + 1;
  const endYear = month === 12 ? year + 1 : year;
  const end = new Date(endYear, endMonth - 1, 15);
  return Math.round((end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24)) + 1;
}

function getUvrPeriod(date: Date): { periodMonth: number; periodYear: number; t: number; d: number } {
  const day = date.getDate();
  const month = date.getMonth() + 1;
  const year = date.getFullYear();

  if (day >= 16) {
    const t = day - 15;
    const d = daysInPeriod(year, month);
    return { periodMonth: month, periodYear: year, t, d };
  } else {
    const prevMonth = month === 1 ? 12 : month - 1;
    const prevYear = month === 1 ? year - 1 : year;
    const daysInPrevMonth = new Date(year, month - 1, 0).getDate();
    const t = (daysInPrevMonth - 15) + day;
    const d = daysInPeriod(prevYear, prevMonth);
    return { periodMonth: prevMonth, periodYear: prevYear, t, d };
  }
}

export async function fetchUvr(): Promise<IndicatorSample[]> {
  const seed = parseUvrSeed();
  const variations = await getIpcMonthlyVariations();
  const today = new Date();

  const uvr15: Map<string, number> = new Map();
  uvr15.set(`${seed.year}-${String(seed.month).padStart(2, "0")}`, seed.value);

  let currentYear = seed.year;
  let currentMonth = seed.month;
  for (let step = 0; step < 24; step++) {
    const key = `${currentYear}-${String(currentMonth).padStart(2, "0")}`;
    const uvr15val = uvr15.get(key);
    if (uvr15val === undefined) break;
    const ipcMonth = currentMonth === 1 ? 12 : currentMonth - 1;
    const ipcYear = currentMonth === 1 ? currentYear - 1 : currentYear;
    const ipcKey = `${ipcYear}-${String(ipcMonth).padStart(2, "0")}`;
    const ipc = variations.get(ipcKey);
    if (ipc === undefined) break;
    const nextMonth = currentMonth === 12 ? 1 : currentMonth + 1;
    const nextYear = currentMonth === 12 ? currentYear + 1 : currentYear;
    uvr15.set(`${nextYear}-${String(nextMonth).padStart(2, "0")}`, uvr15val * (1 + ipc));
    currentMonth = nextMonth;
    currentYear = nextYear;
  }

  currentYear = seed.year;
  currentMonth = seed.month;
  for (let step = 0; step < 24; step++) {
    const prevMonth = currentMonth === 1 ? 12 : currentMonth - 1;
    const prevYear = currentMonth === 1 ? currentYear - 1 : currentYear;
    const ipcMonth = prevMonth === 1 ? 12 : prevMonth - 1;
    const ipcYear = prevMonth === 1 ? prevYear - 1 : prevYear;
    const ipcKey = `${ipcYear}-${String(ipcMonth).padStart(2, "0")}`;
    const ipc = variations.get(ipcKey);
    if (ipc === undefined) break;
    const currentKey = `${currentYear}-${String(currentMonth).padStart(2, "0")}`;
    const uvr15current = uvr15.get(currentKey);
    if (uvr15current === undefined) break;
    const uvr15prev = uvr15current / (1 + ipc);
    uvr15.set(`${prevYear}-${String(prevMonth).padStart(2, "0")}`, uvr15prev);
    currentMonth = prevMonth;
    currentYear = prevYear;
  }

  const todayPeriod = getUvrPeriod(today);
  const periodKey = `${todayPeriod.periodYear}-${String(todayPeriod.periodMonth).padStart(2, "0")}`;
  const uvr15val = uvr15.get(periodKey);
  if (uvr15val === undefined) throw new Error(`UVR: no se encontró UVR(15) para período ${periodKey}`);

  const ipcMonth = todayPeriod.periodMonth === 1 ? 12 : todayPeriod.periodMonth - 1;
  const ipcYear = todayPeriod.periodMonth === 1 ? todayPeriod.periodYear - 1 : todayPeriod.periodYear;
  const ipcKey = `${ipcYear}-${String(ipcMonth).padStart(2, "0")}`;
  const ipc = variations.get(ipcKey);
  if (ipc === undefined) throw new Error(`UVR: no se encontró IPC para ${ipcKey}`);

  const uvrValue = uvr15val * Math.pow(1 + ipc, todayPeriod.t / todayPeriod.d);
  const dateStr = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-${String(today.getDate()).padStart(2, "0")}`;

  return [
    {
      indicator: "UVR",
      variant: "diaria",
      value_number: uvrValue.toFixed(4),
      unit: "",
      observed_at: `${dateStr}T05:00:00Z`,
      from_date: dateStr,
      source: "calculo_resolucion_externa_13_2000_banrep",
      metadata: {
        uvr_15_base: uvr15val,
        ipc_mensual: ipc,
        ipc_mes: ipcKey,
        t: todayPeriod.t,
        d: todayPeriod.d,
      },
    },
  ];
}

const TBOND_DEFAULT_URL =
  "https://www.federalreserve.gov/datadownload/Output.aspx?rel=H15&series=bf17364827e38702b42a58cf8eaa3f78&lastobs=100&from=&to=&filetype=csv&label=include&layout=seriescolumn";

const TBOND_HEADER_PATTERNS: { pattern: RegExp; variant: string }[] = [
  { pattern: /\b1-month\b/i, variant: "1_mes" },
  { pattern: /\b3-month\b/i, variant: "3_meses" },
  { pattern: /\b6-month\b/i, variant: "6_meses" },
  { pattern: /\b1-year\b/i, variant: "1_anio" },
  { pattern: /\b2-year\b/i, variant: "2_anios" },
  { pattern: /\b3-year\b/i, variant: "3_anios" },
  { pattern: /\b5-year\b/i, variant: "5_anios" },
  { pattern: /\b7-year\b/i, variant: "7_anios" },
  { pattern: /\b10-year\b/i, variant: "10_anios" },
  { pattern: /\b20-year\b/i, variant: "20_anios" },
  { pattern: /\b30-year\b/i, variant: "30_anios" },
];

export async function fetchTbonds(): Promise<IndicatorSample[]> {
  const url = env.indicators.federalReserveH15Url || TBOND_DEFAULT_URL;
  const csv = await fetchText(url, { "User-Agent": BROWSER_UA });
  const lines = csv.split("\n").filter((l) => l.trim());

  const descLine = lines[0] ?? "";
  const descCols = descLine.split(",").map((c) => c.replace(/"/g, "").trim());
  const colVariantMap = new Map<number, string>();

  for (let col = 1; col < descCols.length; col++) {
    for (const { pattern, variant } of TBOND_HEADER_PATTERNS) {
      if (pattern.test(descCols[col])) {
        colVariantMap.set(col, variant);
        break;
      }
    }
  }

  if (colVariantMap.size === 0) {
    throw new Error(
      "Federal Reserve H.15: no se encontraron columnas de rendimiento en headers CSV. " +
      `Primera línea: ${descLine.slice(0, 200)}`,
    );
  }

  let latestCols: string[] | null = null;
  let latestDate = "";

  for (let i = 5; i < lines.length; i++) {
    const cols = lines[i].split(",").map((c) => c.replace(/"/g, "").trim());
    const dateStr = cols[0];
    if (!dateStr || !dateStr.match(/^\d{4}-\d{2}-\d{2}$/)) continue;
    latestCols = cols;
    latestDate = dateStr;
  }

  if (!latestCols || !latestDate) throw new Error("Federal Reserve H.15: sin datos de T-Bonds");

  const samples: IndicatorSample[] = [];
  for (const [col, variant] of colVariantMap) {
    const val = parseNumberString(latestCols[col]);
    if (!val) continue;
    samples.push({
      indicator: "TBONDS",
      variant,
      value_number: val,
      unit: "%",
      observed_at: `${latestDate}T05:00:00Z`,
      from_date: latestDate,
      source: url,
      metadata: {},
    });
  }

  if (!samples.length) throw new Error("Federal Reserve H.15: sin datos de T-Bonds");
  return samples;
}

const BCRP_MONTH_MAP: Record<string, number> = {
  Ene: 1, Feb: 2, Mar: 3, Abr: 4, May: 5, Jun: 6,
  Jul: 7, Ago: 8, Sep: 9, Oct: 10, Nov: 11, Dic: 12,
};

function parseBcrpPeriod(name: string): string {
  const match = name.match(/^(\w{3})\.(\d{4})$/);
  if (!match) return todayIso();
  const month = BCRP_MONTH_MAP[match[1]];
  if (!month) return todayIso();
  return `${match[2]}-${String(month).padStart(2, "0")}-01T05:00:00Z`;
}

export async function fetchEmbi(): Promise<IndicatorSample[]> {
  const baseUrl = env.indicators.embiBcrpUrl
    || "https://estadisticas.bcrp.gob.pe/estadisticas/series/api";

  const now = new Date();
  const startYear = now.getFullYear() - 1;
  const endDate = `${now.getFullYear()}-${now.getMonth() + 1}`;
  const startDate = `${startYear}-1`;

  const seriesCode = "PN01133XM";
  const url = `${baseUrl}/${seriesCode}/json/${startDate}/${endDate}`;

  const raw = await fetchText(url);
  const htmlStart = raw.indexOf("<");
  const clean = htmlStart > 0 ? raw.slice(0, htmlStart).trim() : raw.trim();
  const data = JSON.parse(clean);

  const periods: { name: string; values: string[] }[] = data?.periods ?? [];
  if (!periods.length) throw new Error("BCRP API: no se encontraron períodos EMBI");

  const latest = periods[periods.length - 1];
  const rawValue = latest.values?.[0];
  const value = parseNumberString(rawValue);
  if (!value) throw new Error("BCRP API: sin valor EMBI válido en último período");

  const embiFrom = parseBcrpPeriod(latest.name).slice(0, 10);
  const [embiY, embiM] = embiFrom.split("-").map(Number);
  return [
    {
      indicator: "EMBI",
      variant: "mensual",
      value_number: value,
      unit: "bps",
      observed_at: parseBcrpPeriod(latest.name),
      from_date: embiFrom,
      to_date: lastDayOfMonth(embiY, embiM),
      source: url,
      metadata: {
        series_code: seriesCode,
        country: "Colombia",
        period_name: latest.name,
      },
    },
  ];
}

const BETAS_DAMODARAN_URL = "https://pages.stern.nyu.edu/~adamodar/pc/datasets/betaGlobal.xls";

function parseDamodaranBetasSheet(
  buffer: Buffer,
  region: string,
  label: string,
  sourceUrl: string
): IndicatorSample[] {
  const wb = XLSX.read(buffer, { type: "buffer" });
  const sheetName = wb.SheetNames.find((n) => n.toLowerCase().includes("industry"));
  if (!sheetName) throw new Error(`Damodaran ${label}: no se encontró hoja "Industry Averages"`);

  const data = XLSX.utils.sheet_to_json(wb.Sheets[sheetName], {
    header: 1,
    defval: "",
  }) as any[][];

  const excelSerialToDate = (serial: number): Date => {
    const epoch = new Date(1899, 11, 30);
    return new Date(epoch.getTime() + serial * 86400000);
  };

  let observedAt = `${new Date().getFullYear()}-01-01T05:00:00Z`;
  for (let i = 0; i < 10; i++) {
    const row = data[i];
    if (!row) continue;
    for (let j = 0; j < row.length; j++) {
      const cell = String(row[j] || "").toLowerCase();
      if (cell.includes("date updated") || cell.includes("last updated")) {
        const dateCell = row[j + 1] ?? row[j + 2];
        if (typeof dateCell === "number" && dateCell > 40000) {
          observedAt = excelSerialToDate(dateCell).toISOString();
        } else if (dateCell) {
          const parsed = new Date(dateCell);
          if (!Number.isNaN(parsed.getTime())) {
            observedAt = parsed.toISOString();
          }
        }
        break;
      }
    }
  }

  let headerIdx = -1;
  for (let i = 0; i < 15; i++) {
    const row = data[i];
    if (!row) continue;
    const joined = row.map((c: any) => String(c).toLowerCase()).join(" ");
    if (joined.includes("industry name") || joined.includes("number of firms")) {
      headerIdx = i;
      break;
    }
  }
  if (headerIdx < 0) throw new Error(`Damodaran ${label}: no se encontró fila de headers`);

  const headers = data[headerIdx] as string[];

  const findCol = (keywords: string[]): number =>
    headers.findIndex((h) =>
      keywords.every((k) => String(h).toLowerCase().includes(k))
    );

  const colIndustry = 0;
  const colNumFirms = findCol(["number", "firm"]);
  const colBeta = headers.findIndex(
    (h, idx) => idx > 0 && String(h).toLowerCase().trim() === "beta"
  );
  const colDE = findCol(["d/e"]);
  const colUnlevered = findCol(["unlevered beta"]);
  const colUnleveredCash = findCol(["corrected"]);
  const samples: IndicatorSample[] = [];

  const baseDate = new Date(observedAt);
  let industryIdx = 0;

  for (let i = headerIdx + 1; i < data.length; i++) {
    const row = data[i];
    if (!row) continue;
    const industry = String(row[colIndustry] || "").trim();
    if (!industry || industry.toLowerCase() === "total market") continue;

    const beta = parseNumberString(row[colBeta]);
    if (!beta) continue;

    const metadata: Record<string, unknown> = { industry, region: label };
    if (colNumFirms >= 0) metadata.num_firms = row[colNumFirms];
    if (colDE >= 0) metadata.debt_equity = parseNumberString(row[colDE]);
    const unleveredBeta = colUnlevered >= 0 ? parseNumberString(row[colUnlevered]) : null;
    if (colUnleveredCash >= 0) metadata.unlevered_beta_cash_corrected = parseNumberString(row[colUnleveredCash]);
    if (unleveredBeta) metadata.unlevered_beta = unleveredBeta;
    metadata.levered_beta = beta;

    const industrySlug = industry
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_|_$/g, "");
    metadata.industry_slug = industrySlug;

    const uniqueDate = new Date(baseDate.getTime() + industryIdx);
    industryIdx++;

    samples.push({
      indicator: "BETAS_GLOBAL",
      variant: "anual",
      value_number: unleveredBeta ?? beta,
      value_text: industry,
      unit: "",
      observed_at: uniqueDate.toISOString(),
      from_date: observedAt.slice(0, 10),
      source: sourceUrl,
      metadata,
    });
  }

  return samples;
}

export async function fetchBetasDamodaran(): Promise<IndicatorSample[]> {
  const url = env.indicators.betasDamodaranGlobalUrl || BETAS_DAMODARAN_URL;

  const res = await fetch(url, {
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
      Accept: "application/vnd.ms-excel,*/*",
    },
  });
  if (!res.ok) throw new Error(`Error ${res.status} al descargar Betas Damodaran Global`);

  const buffer = Buffer.from(await res.arrayBuffer());
  const samples = parseDamodaranBetasSheet(buffer, "global", "Global", url);

  if (!samples.length) throw new Error("Damodaran: sin datos de betas");
  return samples;
}

const YEAR_2026_START = new Date("2026-01-01T00:00:00Z").getTime();

interface SuamecaSerieConfig {
  id: number;
  indicator: string;
  unit: string;
}

async function fetchSuamecaSeries(
  series: SuamecaSerieConfig[],
): Promise<IndicatorSample[]> {
  const base = ensureUrl(env.indicators.suamecaBaseUrl, "SUAMECA_BASE_URL");
  const samples: IndicatorSample[] = [];

  for (const serie of series) {
    const url = `${base}?idSerie=${serie.id}&tipoDato=1&cantDatos=10`;
    const json = await fetchJson(url);
    const entry = Array.isArray(json) ? json[0] : json;
    const dataPoints: [number, number][] = entry?.data ?? [];

    for (const [ts, val] of dataPoints) {
      if (ts < YEAR_2026_START) continue;
      const value = parseNumberString(val);
      if (!value) continue;
      samples.push({
        indicator: serie.indicator,
        variant: "diaria",
        value_number: value,
        unit: serie.unit,
        observed_at: new Date(ts).toISOString(),
        from_date: new Date(ts).toISOString().slice(0, 10),
        source: url,
        metadata: { idSerie: serie.id },
      });
    }
  }

  if (!samples.length) throw new Error(`SUAMECA: sin datos para 2026`);
  return samples;
}

const CUPON_CERO_SERIES: SuamecaSerieConfig[] = [
  { id: 15272, indicator: "CUPON_CERO_TES_PESOS_1A",  unit: "%" },
  { id: 15273, indicator: "CUPON_CERO_TES_PESOS_5A",  unit: "%" },
  { id: 15274, indicator: "CUPON_CERO_TES_PESOS_10A", unit: "%" },
  { id: 15275, indicator: "CUPON_CERO_TES_UVR_1A",    unit: "%" },
  { id: 15276, indicator: "CUPON_CERO_TES_UVR_5A",    unit: "%" },
  { id: 15277, indicator: "CUPON_CERO_TES_UVR_10A",   unit: "%" },
];

export async function fetchCuponCeroTes(): Promise<IndicatorSample[]> {
  return fetchSuamecaSeries(CUPON_CERO_SERIES);
}

const BETAS_TES_SERIES: SuamecaSerieConfig[] = [
  { id: 15278, indicator: "BETAS_TES_PESOS_B0",  unit: "Unidades" },
  { id: 15279, indicator: "BETAS_TES_PESOS_B1",  unit: "Unidades" },
  { id: 15280, indicator: "BETAS_TES_PESOS_B2",  unit: "Unidades" },
  { id: 15281, indicator: "BETAS_TES_PESOS_TAU", unit: "Unidades" },
  { id: 15282, indicator: "BETAS_TES_UVR_B0",    unit: "Unidades" },
  { id: 15283, indicator: "BETAS_TES_UVR_B1",    unit: "Unidades" },
  { id: 15284, indicator: "BETAS_TES_UVR_B2",    unit: "Unidades" },
  { id: 15285, indicator: "BETAS_TES_UVR_TAU",   unit: "Unidades" },
];

export async function fetchBetasTes(): Promise<IndicatorSample[]> {
  return fetchSuamecaSeries(BETAS_TES_SERIES);
}

// Suppress unused import warnings
void fetchBuffer;
void asObservedAt;
