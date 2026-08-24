// @vitest-environment jsdom

import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DetailPanel } from "./DetailPanel";
import type { CulturalEvent, StationDetail, WeatherResponse } from "../api";

const apiMock = vi.hoisted(() => ({
  station: vi.fn(),
  events: vi.fn(),
  weather: vi.fn(),
}));

vi.mock("../api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../api")>();
  return { ...actual, api: apiMock };
});

const DETAIL: StationDetail = {
  sta_id: "ST-1",
  sta_nm: "첫 번째 대여소",
  sta_addr: "서울시 테스트로 1",
  lat: 37.5,
  lon: 127,
  hold_cnt: 10,
  parking_bike_tot_cnt: 3,
  shared_rate: 0.3,
  region: "센터",
  base_dttm: "2026-08-20T00:00:00Z",
};
const EVENT: CulturalEvent = {
  event_id: "cultural:1",
  title: "테스트 행사",
  place: "테스트 광장",
  start_date: "2026-08-20",
  end_date: "2026-08-21",
  lat: 37.51,
  lon: 127.01,
  distance_km: 0.2,
};
const WEATHER: WeatherResponse = {
  sta_id: "ST-1",
  points: [
    {
      forecast_dttm: "2026-08-20T01:00:00Z",
      temperature: 28,
      sky_condition_cd: "clear",
      precipitation_type_cd: "none",
      precipitation_prob: 10,
      precipitation_amount: null,
      humidity: 70,
      wind_speed: 2,
    },
  ],
};

async function settleRequests(): Promise<void> {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((promiseResolve) => {
    resolve = promiseResolve;
  });
  return { promise, resolve };
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.clearAllMocks();
  apiMock.station.mockResolvedValue(DETAIL);
  apiMock.events.mockResolvedValue({ radius_km: 1, events: [EVENT] });
  apiMock.weather.mockResolvedValue(WEATHER);
});

afterEach(() => {
  cleanup();
  vi.clearAllTimers();
  vi.useRealTimers();
});

function renderDetail(onFocusEvent = vi.fn()) {
  const view = render(
    <DetailPanel
      stationId="ST-1"
      stationPoint={{ lat: DETAIL.lat, lon: DETAIL.lon }}
      onFocusEvent={onFocusEvent}
    />,
  );
  return { ...view, onFocusEvent };
}

describe("DetailPanel stale state", () => {
  it("대여소명·주소와 도넛 재고만 표시한다", async () => {
    renderDetail();
    await settleRequests();

    expect(screen.getByRole("heading", { name: DETAIL.sta_nm })).not.toBeNull();
    expect(screen.getByText(DETAIL.sta_addr)).not.toBeNull();
    expect(screen.getByRole("img", { name: "현재 자전거 3대, 거치대 10대" })).not.toBeNull();
    expect(screen.queryByText("현재 자전거")).toBeNull();
    expect(screen.queryByText("30% 이용 가능")).toBeNull();
    expect(screen.queryByText("재고 갱신")).toBeNull();
    expect(screen.queryByText(/갱신 시각/)).toBeNull();
  });

  it("station polling 실패 뒤 이전 상세를 경고와 함께 유지한다", async () => {
    apiMock.station.mockResolvedValueOnce(DETAIL).mockRejectedValueOnce(new Error("network unavailable"));
    renderDetail();
    await settleRequests();
    expect(screen.getByText(DETAIL.sta_nm)).not.toBeNull();

    await act(async () => {
      vi.advanceTimersByTime(60_000);
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(screen.getByText(DETAIL.sta_nm)).not.toBeNull();
    expect(screen.getByText("상세 조회에 실패해 마지막 결과를 표시합니다.")).not.toBeNull();
  });

  it("events polling 실패 뒤 이전 행사와 검색 반경 상태를 유지한다", async () => {
    apiMock.events
      .mockResolvedValueOnce({ radius_km: 1, events: [EVENT] })
      .mockRejectedValueOnce(new Error("network unavailable"));
    renderDetail();
    fireEvent.click(screen.getByRole("tab", { name: "주변 행사" }));
    await settleRequests();
    expect(screen.getByText(EVENT.title)).not.toBeNull();

    await act(async () => {
      vi.advanceTimersByTime(60_000);
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(screen.getByText(EVENT.title)).not.toBeNull();
    expect(screen.getByText("행사 조회에 실패해 마지막 결과를 표시합니다.")).not.toBeNull();
  });

  it("weather polling 실패 뒤 이전 예보를 유지한다", async () => {
    apiMock.weather.mockResolvedValueOnce(WEATHER).mockRejectedValueOnce(new Error("network unavailable"));
    renderDetail();
    fireEvent.click(screen.getByRole("tab", { name: "주변 날씨" }));
    await settleRequests();
    expect(screen.getByText("28℃")).not.toBeNull();

    await act(async () => {
      vi.advanceTimersByTime(60_000);
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(screen.getByText("28℃")).not.toBeNull();
    expect(screen.getByText("날씨 조회에 실패해 마지막 결과를 표시합니다.")).not.toBeNull();
  });

  it("station 변경 직후 이전 상세를 지운다", async () => {
    apiMock.station.mockResolvedValueOnce(DETAIL).mockReturnValueOnce(new Promise(() => {}));
    const view = renderDetail();
    await settleRequests();
    expect(screen.getByText(DETAIL.sta_nm)).not.toBeNull();

    view.rerender(
      <DetailPanel stationId="ST-2" stationPoint={{ lat: 37.6, lon: 127.1 }} onFocusEvent={vi.fn()} />,
    );

    expect(screen.queryByText(DETAIL.sta_nm)).toBeNull();
    expect(screen.getByText("불러오는 중...")).not.toBeNull();
  });

  it("행사 포커스에 행사 Point와 선택 station 검색 중심을 구분해 전달한다", async () => {
    const { onFocusEvent } = renderDetail();
    fireEvent.click(screen.getByRole("tab", { name: "주변 행사" }));
    await settleRequests();

    fireEvent.click(screen.getByRole("button", { name: new RegExp(EVENT.title) }));

    expect(onFocusEvent).toHaveBeenLastCalledWith({
      eventLat: EVENT.lat,
      eventLon: EVENT.lon,
      searchCenterLat: DETAIL.lat,
      searchCenterLon: DETAIL.lon,
      radiusKm: 1,
    });
  });

  it("느린 이전 station 성공이 최신 polling 실패 뒤 상세를 복원하지 못한다", async () => {
    const oldDetail = deferred<StationDetail>();
    apiMock.station.mockReturnValueOnce(oldDetail.promise).mockRejectedValueOnce(new Error("network unavailable"));
    renderDetail();

    await act(async () => {
      vi.advanceTimersByTime(60_000);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(screen.getByText("대여소 정보를 불러오지 못했습니다.")).not.toBeNull();

    oldDetail.resolve(DETAIL);
    await settleRequests();

    expect(screen.queryByText(DETAIL.sta_nm)).toBeNull();
    expect(screen.getByText("대여소 정보를 불러오지 못했습니다.")).not.toBeNull();
  });

  it("느린 이전 events 성공이 최신 polling 실패 뒤 행사를 복원하지 못한다", async () => {
    const oldEvents = deferred<{ radius_km: number; events: CulturalEvent[] }>();
    apiMock.events.mockReturnValueOnce(oldEvents.promise).mockRejectedValueOnce(new Error("network unavailable"));
    renderDetail();
    fireEvent.click(screen.getByRole("tab", { name: "주변 행사" }));

    await act(async () => {
      vi.advanceTimersByTime(60_000);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(screen.getByText("주변 행사 정보를 불러오지 못했습니다.")).not.toBeNull();

    oldEvents.resolve({ radius_km: 1, events: [EVENT] });
    await settleRequests();

    expect(screen.queryByText(EVENT.title)).toBeNull();
    expect(screen.getByText("주변 행사 정보를 불러오지 못했습니다.")).not.toBeNull();
  });

  it("느린 이전 weather 성공이 최신 polling 실패 뒤 예보를 복원하지 못한다", async () => {
    const oldWeather = deferred<WeatherResponse>();
    apiMock.weather.mockReturnValueOnce(oldWeather.promise).mockRejectedValueOnce(new Error("network unavailable"));
    renderDetail();
    fireEvent.click(screen.getByRole("tab", { name: "주변 날씨" }));

    await act(async () => {
      vi.advanceTimersByTime(60_000);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(screen.getByText("주변 날씨 정보를 불러오지 못했습니다.")).not.toBeNull();

    oldWeather.resolve(WEATHER);
    await settleRequests();

    expect(screen.queryByText("28℃")).toBeNull();
    expect(screen.getByText("주변 날씨 정보를 불러오지 못했습니다.")).not.toBeNull();
  });
});
