import {
  listIndicators,
  getIndicatorByAlias,
  getIndicatorByCatalogKey,
  getLatestIndicatorHistoric,
  getIndicatorHistoricRange,
} from "../db/indicator-historic.repo";
import { getLatestIndicatorRecords, getLatestByIndicator } from "../db/indicator.repo";
import { listIndicatorGroups } from "../db/indicator-group.repo";
import type { IndicatorCatalog } from "../models/indicator-catalog.model";
import type { IndicatorHistoric } from "../models/indicator-historic.model";
import type { IndicatorRecord } from "../models/indicator.model";
import type { IndicatorGroupWithMembers } from "../db/indicator-group.repo";

export interface SnapshotEntry {
  alias: string;
  name: string;
  segmentation: string | null;
  term_unit: string | null;
  unit: string;
  value: string | null;
  from_date: Date | null;
  source: string | null;
}

export const IndicatorsService = {
  async listCatalog(onlyActive = true): Promise<IndicatorCatalog[]> {
    return listIndicators(onlyActive);
  },

  async getByAlias(alias: string): Promise<IndicatorCatalog | null> {
    return getIndicatorByAlias(alias.toUpperCase());
  },

  async listGroups(): Promise<IndicatorGroupWithMembers[]> {
    return listIndicatorGroups();
  },

  async snapshot(): Promise<SnapshotEntry[]> {
    const catalog = await listIndicators(true);
    const result: SnapshotEntry[] = [];

    for (const ind of catalog) {
      const latest = await getLatestIndicatorHistoric(ind.id);
      result.push({
        alias: ind.alias,
        name: ind.name,
        segmentation: ind.segmentation,
        term_unit: ind.term_unit,
        unit: ind.unit,
        value: latest?.value ?? null,
        from_date: latest?.from_date ?? null,
        source: ind.source,
      });
    }

    return result;
  },

  async getLatestForAlias(alias: string): Promise<SnapshotEntry | null> {
    const ind = await getIndicatorByAlias(alias.toUpperCase());
    if (!ind) return null;

    const latest = await getLatestIndicatorHistoric(ind.id);
    return {
      alias: ind.alias,
      name: ind.name,
      segmentation: ind.segmentation,
      term_unit: ind.term_unit,
      unit: ind.unit,
      value: latest?.value ?? null,
      from_date: latest?.from_date ?? null,
      source: ind.source,
    };
  },

  async getHistoric(
    alias: string,
    fromDate: string,
    toDate: string,
    segmentation?: string | null,
    termValue?: number | null,
    termUnit?: string | null,
  ): Promise<{ indicator: IndicatorCatalog; rows: IndicatorHistoric[] } | null> {
    const ind = await getIndicatorByCatalogKey(alias.toUpperCase(), segmentation, termValue, termUnit);
    if (!ind) {
      // Fallback: try by alias only
      const byAlias = await getIndicatorByAlias(alias.toUpperCase());
      if (!byAlias) return null;
      const rows = await getIndicatorHistoricRange(byAlias.id, fromDate, toDate);
      return { indicator: byAlias, rows };
    }
    const rows = await getIndicatorHistoricRange(ind.id, fromDate, toDate);
    return { indicator: ind, rows };
  },

  async getLatestRecords(): Promise<IndicatorRecord[]> {
    return getLatestIndicatorRecords();
  },

  async getLatestRecordsByIndicator(indicator: string): Promise<IndicatorRecord[]> {
    return getLatestByIndicator(indicator.toUpperCase());
  },
};
