import { describe, expect, it } from "vitest";
import { ApiError } from "../api";
import { forecastUnavailableMessage } from "./ForecastPanel";

describe("forecastUnavailableMessage", () => {
  it("404를 예측 미지원 상태로 표시한다", () => {
    const error = new ApiError("/stations/ST-1/forecast", 404, "forecast_not_available");

    expect(forecastUnavailableMessage(error)).toBe("이 대여소는 예측을 지원하지 않습니다.");
  });

  it("503을 갱신 중 상태로 표시한다", () => {
    const error = new ApiError("/stations/ST-1/forecast", 503, "forecast_not_ready");

    expect(forecastUnavailableMessage(error)).toBe("예측 데이터를 갱신 중입니다.");
  });

  it("네트워크 오류는 일반 실패 상태로 표시한다", () => {
    expect(forecastUnavailableMessage(new Error("network unavailable"))).toBe(
      "예측 데이터를 불러오지 못했습니다.",
    );
  });
});
