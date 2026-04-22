import { RecordStatus, TermUnit } from "./indicator-catalog.model";

export interface IndicatorHistoric {
  id: number;
  indicator_id: number;
  from_date: Date;
  to_date: Date | null;
  value: string;
  periodicity: TermUnit | null;
  status: RecordStatus;
  created_at: Date;
  updated_at: Date;
}

export interface IndicatorHistoricUpsert {
  indicator_id: number;
  from_date: Date | string;
  to_date?: Date | string | null;
  value: number | string;
  periodicity?: TermUnit | null;
  status?: RecordStatus;
}
