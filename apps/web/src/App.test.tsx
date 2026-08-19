// @vitest-environment jsdom

import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import type { Alert, ForecastResponse, StationSummary } from "./api";

const apiMock = vi.hoisted(() => ({
  stations: vi.fn(),
  station: vi.fn(),
  forecast: vi.fn(),
  events: vi.fn(),
  weather: vi.fn(),
  alerts: vi.fn(),
  status: vi.fn(),
  regions: vi.fn(),
}));

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, api: apiMock };
});

vi.mock("./components/Header", () => ({ Header: () => <div>header</div> }));
vi.mock("./components/StationMap", () => ({
  StationMap: ({
    stations,
    alerts,
    onSelect,
  }: {
    stations: StationSummary[];
    alerts: Alert[];
    onSelect: (stationId: string) => void;
  }) => (
    <div>
      <span data-testid="map-stations">{stations.map((station) => station.sta_id).join(",")}</span>
      <span data-testid="map-alerts">{alerts.map((alert) => alert.sta_id).join(",")}</span>
      <button type="button" onClick={() => onSelect("ST-2")}>
        두 번째 대여소 선택
      </button>
    </div>
  ),
}));
vi.mock("./components/AlertList", () => ({ AlertList: () => <div>alerts</div> }));
vi.mock("./components/DetailPanel", () => ({
  DetailPanel: ({ stationId }: { stationId: string | null }) => (
    <div data-testid="detail-station">{stationId ?? "none"}</div>
  ),
}));
vi.mock("./components/ForecastPanel", () => ({
  ForecastPanel: ({ forecast, error }: { forecast: ForecastResponse | null; error: Error | null }) => (
    <div data-testid="forecast-state">{error ? error.message : (forecast?.base_dttm ?? "empty")}</div>
  ),
}));
vi.mock("./components/StockPanel", () => ({ StockPanel: () => <div>stock</div> }));
vi.mock("@/components/ui/resizable", () => ({
  ResizablePanelGroup: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  ResizablePanel: ({ children }: { children: ReactNode }) => <div>{children}</div>,
  ResizableHandle: () => <div />,
}));

const STATIONS: StationSummary[] = [
  {
    sta_id: "ST-1",
    sta_nm: "첫 번째 대여소",
    lat: 37.5,
    lon: 127,
    hold_cnt: 10,
    parking_bike_tot_cnt: 3,
    shared_rate: 0.3,
    region: "센터",
    base_dttm: "2026-08-20T00:00:00Z",
  },
  {
    sta_id: "ST-2",
    sta_nm: "두 번째 대여소",
    lat: 37.51,
    lon: 127.01,
    hold_cnt: 10,
    parking_bike_tot_cnt: 4,
    shared_rate: 0.4,
    region: "센터",
    base_dttm: "2026-08-20T00:00:00Z",
  },
];
const ALERTS: Alert[] = [
  {
    sta_id: "ST-1",
    sta_nm: "첫 번째 대여소",
    action_type: "supply_needed",
    urgency_score: 50,
    minutes_until_critical: 10,
    region: "센터",
  },
];
const FORECAST: ForecastResponse = {
  sta_id: "ST-1",
  base_dttm: "2026-08-20T00:00:00Z",
  points: [],
};

async function settleRequests(): Promise<void> {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { promise, reject, resolve };
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.clearAllMocks();
  apiMock.alerts.mockResolvedValue(ALERTS);
  apiMock.forecast.mockResolvedValue(FORECAST);
  apiMock.regions.mockResolvedValue([]);
});

afterEach(() => {
  cleanup();
  vi.clearAllTimers();
  vi.useRealTimers();
});

describe("App polling state", () => {
  it("대여소 선택을 바꾸는 즉시 이전 forecast를 지운다", async () => {
    apiMock.stations.mockResolvedValue(STATIONS);
    apiMock.forecast.mockResolvedValueOnce(FORECAST).mockReturnValueOnce(new Promise(() => {}));
    render(<App />);
    await settleRequests();
    await settleRequests();
    expect(screen.getByTestId("forecast-state").textContent).toContain(FORECAST.base_dttm);

    fireEvent.click(screen.getByRole("button", { name: "두 번째 대여소 선택" }));

    expect(screen.getByTestId("forecast-state").textContent).toContain("empty");
    expect(screen.getByTestId("detail-station").textContent).toContain("ST-2");
  });

  it("stations polling 실패 뒤 선택과 하위 성공 데이터를 남기지 않는다", async () => {
    apiMock.stations.mockResolvedValueOnce(STATIONS).mockRejectedValueOnce(new Error("network unavailable"));
    render(<App />);
    await settleRequests();
    await settleRequests();
    expect(screen.getByTestId("detail-station").textContent).toContain("ST-1");
    expect(screen.getByTestId("forecast-state").textContent).toContain(FORECAST.base_dttm);

    await act(async () => {
      vi.advanceTimersByTime(15_000);
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(screen.getByTestId("detail-station").textContent).toContain("none");
    expect(screen.getByTestId("forecast-state").textContent).toContain("empty");
    expect(screen.getByText("대여소 정보를 갱신하지 못했습니다.")).not.toBeNull();
  });

  it("새 stations 목록에서 선택 ID가 사라지면 선택과 forecast를 해제한다", async () => {
    apiMock.stations.mockResolvedValueOnce(STATIONS).mockResolvedValueOnce([STATIONS[1]]);
    apiMock.alerts.mockResolvedValue([
      ...ALERTS,
      {
        ...ALERTS[0],
        sta_id: "ST-2",
        sta_nm: "두 번째 대여소",
      },
    ]);
    render(<App />);
    await settleRequests();
    await settleRequests();
    expect(screen.getByTestId("detail-station").textContent).toContain("ST-1");

    await act(async () => {
      vi.advanceTimersByTime(15_000);
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(screen.getByTestId("detail-station").textContent).toContain("none");
    expect(screen.getByTestId("forecast-state").textContent).toContain("empty");
  });

  it("alerts polling 실패 뒤 이전 우선순위를 현재값처럼 남기지 않는다", async () => {
    apiMock.stations.mockResolvedValue(STATIONS);
    apiMock.alerts.mockResolvedValueOnce(ALERTS).mockRejectedValueOnce(new Error("network unavailable"));
    render(<App />);
    await settleRequests();
    expect(screen.getByTestId("map-alerts").textContent).toBe("ST-1");

    await act(async () => {
      vi.advanceTimersByTime(15_000);
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(screen.getByTestId("map-alerts").textContent).toBe("");
    expect(screen.getByText("작업 우선순위를 갱신하지 못했습니다.")).not.toBeNull();
  });

  it("stations와 alerts의 느린 이전 요청이 최신 polling 결과를 복원하지 못한다", async () => {
    const oldStations = deferred<StationSummary[]>();
    const oldAlerts = deferred<Alert[]>();
    const newAlert = { ...ALERTS[0], sta_id: "ST-2", sta_nm: "두 번째 대여소" };
    apiMock.stations.mockReturnValueOnce(oldStations.promise).mockResolvedValue([STATIONS[1]]);
    apiMock.alerts.mockReturnValueOnce(oldAlerts.promise).mockResolvedValue([newAlert]);
    render(<App />);

    await act(async () => {
      vi.advanceTimersByTime(15_000);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(screen.getByTestId("map-stations").textContent).toBe("ST-2");
    expect(screen.getByTestId("map-alerts").textContent).toBe("ST-2");

    oldStations.resolve([STATIONS[0]]);
    oldAlerts.resolve(ALERTS);
    await settleRequests();

    expect(screen.getByTestId("map-stations").textContent).toBe("ST-2");
    expect(screen.getByTestId("map-alerts").textContent).toBe("ST-2");
  });

  it("느린 이전 forecast 성공이 최신 forecast를 덮지 못한다", async () => {
    const oldForecast = deferred<ForecastResponse>();
    const latestForecast = { ...FORECAST, base_dttm: "2026-08-20T01:00:00Z" };
    apiMock.stations.mockResolvedValue(STATIONS);
    apiMock.forecast.mockReturnValueOnce(oldForecast.promise).mockResolvedValue(latestForecast);
    render(<App />);
    await settleRequests();
    await settleRequests();

    await act(async () => {
      vi.advanceTimersByTime(60_000);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(screen.getByTestId("forecast-state").textContent).toContain(latestForecast.base_dttm);

    oldForecast.resolve(FORECAST);
    await settleRequests();

    expect(screen.getByTestId("forecast-state").textContent).toContain(latestForecast.base_dttm);
  });

  it("선택 click이 effect cleanup 전에도 기존 forecast 요청을 즉시 무효화한다", async () => {
    const oldForecast = deferred<ForecastResponse>();
    apiMock.stations.mockResolvedValue(STATIONS);
    apiMock.forecast.mockReturnValueOnce(oldForecast.promise).mockReturnValueOnce(new Promise(() => {}));
    render(<App />);
    await settleRequests();
    await settleRequests();

    fireEvent.click(screen.getByRole("button", { name: "두 번째 대여소 선택" }));
    oldForecast.resolve(FORECAST);
    await settleRequests();

    expect(screen.getByTestId("detail-station").textContent).toContain("ST-2");
    expect(screen.getByTestId("forecast-state").textContent).toContain("empty");
    expect(screen.getByTestId("forecast-state").textContent).not.toContain(FORECAST.base_dttm);
  });

  it("stations 실패 clear가 진행 중 forecast 요청을 즉시 무효화한다", async () => {
    const oldForecast = deferred<ForecastResponse>();
    apiMock.stations.mockResolvedValueOnce(STATIONS).mockRejectedValueOnce(new Error("network unavailable"));
    apiMock.forecast.mockReturnValueOnce(oldForecast.promise);
    render(<App />);
    await settleRequests();
    await settleRequests();

    await act(async () => {
      vi.advanceTimersByTime(15_000);
      await Promise.resolve();
      oldForecast.resolve(FORECAST);
      await Promise.resolve();
    });

    expect(screen.getByTestId("detail-station").textContent).toContain("none");
    expect(screen.getByTestId("forecast-state").textContent).toContain("empty");
  });

  it("stations 선택 ID 소실 clear가 진행 중 forecast 요청을 즉시 무효화한다", async () => {
    const oldForecast = deferred<ForecastResponse>();
    apiMock.stations.mockResolvedValueOnce(STATIONS).mockResolvedValueOnce([STATIONS[1]]);
    apiMock.forecast.mockReturnValueOnce(oldForecast.promise);
    render(<App />);
    await settleRequests();
    await settleRequests();

    await act(async () => {
      vi.advanceTimersByTime(15_000);
      await Promise.resolve();
      oldForecast.resolve(FORECAST);
      await Promise.resolve();
    });

    expect(screen.getByTestId("detail-station").textContent).toContain("none");
    expect(screen.getByTestId("forecast-state").textContent).toContain("empty");
  });
});
