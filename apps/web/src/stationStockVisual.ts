export type StationStockBand = "critical" | "warning" | "normal" | "overflow";

export interface StationStockVisual {
  band: StationStockBand;
  capacityPercent: number;
  overflowArcPercent: number;
  ratioPercent: number;
}

const OVERFLOW_VISUAL_CAP_RATIO = 10;

export function stationStockVisual(current: number, capacity: number): StationStockVisual {
  /** 현재 재고율을 고정 크기 상세 도넛의 기본·초과 호 값으로 변환한다. */
  if (!Number.isFinite(current) || !Number.isFinite(capacity) || capacity <= 0) {
    return { band: "normal", capacityPercent: 0, overflowArcPercent: 0, ratioPercent: 0 };
  }
  const ratio = Math.max(0, current) / capacity;
  const ratioPercent = Math.round(ratio * 100);
  const capacityPercent = Math.min(100, ratioPercent);
  // 실제 초과 분포가 100~1000%대로 넓어 선형 척도는 보통 초과를 거의 숨긴다.
  // 정확한 비율은 숫자로 표시하고, 같은 링의 진초록 호는 100→1000%를 로그로
  // 압축한 초과 강도를 보여준다. 1000%는 운영 임계값이 아니라 시각화 상한이다.
  const overflowArcPercent = ratio <= 1
    ? 0
    : Math.min(
      100,
      Math.round((Math.log(ratio) / Math.log(OVERFLOW_VISUAL_CAP_RATIO)) * 100),
    );
  if (ratio <= 0.2) return { band: "critical", capacityPercent, overflowArcPercent, ratioPercent };
  if (ratio <= 0.4) return { band: "warning", capacityPercent, overflowArcPercent, ratioPercent };
  if (ratio > 1) return { band: "overflow", capacityPercent, overflowArcPercent, ratioPercent };
  return { band: "normal", capacityPercent, overflowArcPercent, ratioPercent };
}
