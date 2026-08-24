export type StationStockBand = "critical" | "warning" | "normal" | "overflow";

export interface StationStockVisual {
  band: StationStockBand;
  capacityPercent: number;
  overflowPercent: number;
  ratioPercent: number;
}

export function stationStockVisual(current: number, capacity: number): StationStockVisual {
  /** 현재 재고율을 고정 크기 상세 도넛의 내부·초과 링 값으로 변환한다. */
  if (!Number.isFinite(current) || !Number.isFinite(capacity) || capacity <= 0) {
    return { band: "normal", capacityPercent: 0, overflowPercent: 0, ratioPercent: 0 };
  }
  const ratio = Math.max(0, current) / capacity;
  const ratioPercent = Math.round(ratio * 100);
  const capacityPercent = Math.min(100, ratioPercent);
  const overflowPercent = Math.min(100, Math.max(0, ratioPercent - 100));
  if (ratio <= 0.2) return { band: "critical", capacityPercent, overflowPercent, ratioPercent };
  if (ratio <= 0.4) return { band: "warning", capacityPercent, overflowPercent, ratioPercent };
  if (ratio > 1) return { band: "overflow", capacityPercent, overflowPercent, ratioPercent };
  return { band: "normal", capacityPercent, overflowPercent, ratioPercent };
}
