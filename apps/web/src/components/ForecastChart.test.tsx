// @vitest-environment jsdom

import { cleanup, fireEvent, render } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ForecastPoint } from "../api";
import { ForecastChart } from "./ForecastChart";

afterEach(cleanup);

describe("ForecastChart", () => {
  it("대여와 반납 값이 같아도 두 선을 맞닿아 구분해서 그린다", () => {
    const points: ForecastPoint[] = [0, 1, 2].map((hour) => ({
      predicted_dttm: `2026-08-24T0${hour}:00:00Z`,
      predicted_rent_cnt: 4,
      predicted_return_cnt: 4,
      predicted_bikes: 10,
      action_type: "normal",
    }));
    const { container } = render(
      <ForecastChart baseDttm="2026-08-23T23:00:00Z" points={points} />,
    );
    const rentPath = container.querySelector<SVGPathElement>(
      '[data-series="predicted_rent_cnt"]',
    );
    const returnPath = container.querySelector<SVGPathElement>(
      '[data-series="predicted_return_cnt"]',
    );

    expect(rentPath?.getAttribute("d")).not.toBe(returnPath?.getAttribute("d"));
    expect(Number(rentPath?.getAttribute("d")?.split(" ")[1])).toBe(32);
  });

  it("겹침 구간에서는 직전에 위에 있던 선의 상하 순서를 유지한다", () => {
    const points: ForecastPoint[] = [
      { predicted_rent_cnt: 2, predicted_return_cnt: 6 },
      { predicted_rent_cnt: 4, predicted_return_cnt: 4 },
      { predicted_rent_cnt: 4, predicted_return_cnt: 4 },
    ].map((counts, hour) => ({
      predicted_dttm: `2026-08-24T0${hour}:00:00Z`,
      predicted_bikes: 10,
      action_type: "normal",
      ...counts,
    }));
    const { container } = render(
      <ForecastChart baseDttm="2026-08-23T23:00:00Z" points={points} />,
    );
    const rentPath = container.querySelector<SVGPathElement>(
      '[data-series="predicted_rent_cnt"]',
    );
    const returnPath = container.querySelector<SVGPathElement>(
      '[data-series="predicted_return_cnt"]',
    );

    const pointY = (path: SVGPathElement | null, index: number) => {
      const pathData = path?.getAttribute("d") ?? "";
      if (index === 0) return Number(pathData.split(" ")[2]);
      const segments = pathData.split(" C ")[index].split(", ");
      const endpoint = segments[segments.length - 1] ?? "";
      return Number(endpoint.trim().split(" ")[1]);
    };

    expect(pointY(returnPath, 0)).toBeLessThan(pointY(rentPath, 0));
    expect(pointY(returnPath, 1)).toBeLessThan(pointY(rentPath, 1));
    expect(pointY(returnPath, 2)).toBeLessThan(pointY(rentPath, 2));
  });

  it("마우스가 가리키는 한 시간 예측 구간 전체를 강조한다", () => {
    const points: ForecastPoint[] = [0, 1, 2].map((hour) => ({
      predicted_dttm: `2026-08-24T0${hour}:00:00Z`,
      predicted_rent_cnt: 4,
      predicted_return_cnt: 3,
      predicted_bikes: 10,
      action_type: "normal",
    }));
    const { container } = render(
      <ForecastChart baseDttm="2026-08-23T23:00:00Z" points={points} />,
    );
    const pointerTarget = container.querySelector<SVGRectElement>('rect[fill="transparent"]');
    vi.spyOn(pointerTarget as SVGRectElement, "getBoundingClientRect").mockReturnValue({
      bottom: 220,
      height: 220,
      left: 0,
      right: 600,
      top: 0,
      width: 600,
      x: 0,
      y: 0,
      toJSON: () => ({}),
    });

    fireEvent.pointerMove(pointerTarget as SVGRectElement, { clientX: 300 });

    const interval = container.querySelector<SVGRectElement>("[data-hover-interval]");
    expect(Number(interval?.getAttribute("width"))).toBe(184);
  });
});
