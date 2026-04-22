export type TermUnit = 'diaria' | 'semanal' | 'mensual' | 'bimestral' | 'trimestral' | 'semestral' | 'anual';
export type UnitOfMeasure = 'percentage' | 'cop' | 'usd' | 'eur' | 'basis_points' | 'index';
export type RecordStatus = 'definitive' | 'provisional' | 'revised';

export interface IndicatorCatalog {
  id: number;
  name: string;
  alias: string;
  segmentation: string | null;
  term_value: number | null;
  term_unit: TermUnit | null;
  base: number | null;
  unit: UnitOfMeasure;
  source: string | null;
  is_active: boolean;
  created_at: Date;
  updated_at: Date;
}

export type IndicatorCatalogInsert = Omit<IndicatorCatalog, 'id' | 'created_at' | 'updated_at'>;
