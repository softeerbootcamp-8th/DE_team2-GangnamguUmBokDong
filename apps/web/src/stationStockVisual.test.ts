import { describe, expect, it } from "vitest";
import { stationStockVisual } from "./stationStockVisual";

describe("stationStockVisual", () => {
  it.each([
    [2, 10, "critical", 20, 0, 20],
    [4, 10, "warning", 40, 0, 40],
    [5, 10, "normal", 50, 0, 50],
    [10, 10, "normal", 100, 0, 100],
    [13, 10, "overflow", 100, 30, 130],
  ] as const)(
    "현재 %d대/정원 %d대를 %s 내부·초과 링으로 표시한다",
    (current, capacity, band, capacityPercent, overflowPercent, ratioPercent) => {
      expect(stationStockVisual(current, capacity)).toEqual({
        band,
        capacityPercent,
        overflowPercent,
        ratioPercent,
      });
    },
  );

  it("200%가 넘는 초과량은 외부 링을 가득 채우고 숫자는 실제 비율을 유지한다", () => {
    expect(stationStockVisual(33, 10)).toEqual({
      band: "overflow",
      capacityPercent: 100,
      overflowPercent: 100,
      ratioPercent: 330,
    });
  });

  it("유효하지 않은 정원은 중립 표시로 안전하게 처리한다", () => {
    expect(stationStockVisual(3, 0)).toEqual({
      band: "normal",
      capacityPercent: 0,
      overflowPercent: 0,
      ratioPercent: 0,
    });
  });
});
