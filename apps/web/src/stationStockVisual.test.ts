import { describe, expect, it } from "vitest";
import { stationStockVisual } from "./stationStockVisual";

describe("stationStockVisual", () => {
  it.each([
    [2, 10, "critical", 20, 0, 20],
    [4, 10, "warning", 40, 0, 40],
    [5, 10, "normal", 50, 0, 50],
    [10, 10, "normal", 100, 0, 100],
    [13, 10, "overflow", 100, 11, 130],
    [20, 10, "overflow", 100, 30, 200],
    [30, 10, "overflow", 100, 48, 300],
    [50, 10, "overflow", 100, 70, 500],
    [100, 10, "overflow", 100, 100, 1000],
  ] as const)(
    "현재 %d대/정원 %d대를 %s 기본·초과 호로 표시한다",
    (current, capacity, band, capacityPercent, overflowArcPercent, ratioPercent) => {
      expect(stationStockVisual(current, capacity)).toEqual({
        band,
        capacityPercent,
        overflowArcPercent,
        ratioPercent,
      });
    },
  );

  it("1000%가 넘으면 진초록 호만 상한에 고정하고 숫자는 실제 비율을 유지한다", () => {
    expect(stationStockVisual(120, 10)).toEqual({
      band: "overflow",
      capacityPercent: 100,
      overflowArcPercent: 100,
      ratioPercent: 1200,
    });
  });

  it("유효하지 않은 정원은 중립 표시로 안전하게 처리한다", () => {
    expect(stationStockVisual(3, 0)).toEqual({
      band: "normal",
      capacityPercent: 0,
      overflowArcPercent: 0,
      ratioPercent: 0,
    });
  });
});
