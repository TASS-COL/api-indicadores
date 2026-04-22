export type IndicatorSample = {
  indicator: string;
  variant?: string | null;
  value_number?: string | number | null;
  value_text?: string | null;
  unit?: string | null;
  observed_at: string | Date;
  /** Fecha de inicio de vigencia del valor (from_date en indicator_historic) */
  from_date?: string | Date | null;
  /** Fecha de fin de vigencia del valor (to_date en indicator_historic). Null = abierto */
  to_date?: string | Date | null;
  fetched_at?: string | Date;
  source?: string | null;
  metadata?: Record<string, unknown>;
  tenant_id?: number | null;
  is_active?: boolean;
};

export type IndicatorJobDefinition = {
  key: string;
  description: string;
  cron?: string;
  handler: () => Promise<IndicatorSample[]>;
  skipUpsert?: boolean;
};

export type IndicatorJobResult = {
  jobKey: string;
  description: string;
  stored: number;
  samples: number;
  indicators: string[];
  variants: string[];
  dateRange: { from: string | null; to: string | null };
};
