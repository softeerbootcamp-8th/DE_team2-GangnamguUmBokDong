export type StationStockBand = "critical" | "warning" | "normal" | "overflow";

export interface StationStockVisual {
  band: StationStockBand;
  ratioPercent: number;
}

export function stationStockVisual(current: number, capacity: number): StationStockVisual {
  /** 현재 재고율을 고정 크기 지도 링에 사용할 표시 구간으로 변환한다. */
  if (!Number.isFinite(current) || !Number.isFinite(capacity) || capacity <= 0) {
    return { band: "normal", ratioPercent: 0 };
  }
  const ratio = Math.max(0, current) / capacity;
  const ratioPercent = Math.round(ratio * 100);
  if (ratio <= 0.2) return { band: "critical", ratioPercent };
  if (ratio <= 0.4) return { band: "warning", ratioPercent };
  if (ratio > 1) return { band: "overflow", ratioPercent };
  return { band: "normal", ratioPercent };
}
