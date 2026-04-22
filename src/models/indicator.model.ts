export interface IndicatorRecord {
  id: number;
  tenant_id: number | null;
  indicator: string;
  variant: string | null;
  value_number: string | null;
  value_text: string | null;
  unit: string | null;
  observed_at: Date;
  fetched_at: Date;
  source: string | null;
  metadata: Record<string, unknown>;
  is_active: boolean;
}

export type IndicatorInsert = Omit<IndicatorRecord, "id" | "fetched_at"> & {
  fetched_at?: string | Date;
};
