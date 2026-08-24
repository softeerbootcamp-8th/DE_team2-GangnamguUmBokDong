import { describe, expect, it } from "vitest";
import { stationStockVisual } from "./stationStockVisual";

describe("stationStockVisual", () => {
  it.each([
    [2, 10, "critical", 20],
    [4, 10, "warning", 40],
    [5, 10, "normal", 50],
    [10, 10, "normal", 100],
    [13, 10, "overflow", 130],
  ] as const)("현재 %d대/정원 %d대를 %s로 표시한다", (current, capacity, band, ratioPercent) => {
    expect(stationStockVisual(current, capacity)).toEqual({ band, ratioPercent });
  });

  it("유효하지 않은 정원은 중립 표시로 안전하게 처리한다", () => {
    expect(stationStockVisual(3, 0)).toEqual({ band: "normal", ratioPercent: 0 });
  });
});
