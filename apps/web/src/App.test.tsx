// @vitest-environment jsdom

import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "./App";
import type { Alert, ForecastResponse, Route, StationSummary } from "./api";

const apiMock = vi.hoisted(() => ({
  stations: vi.fn(),
  station: vi.fn(),
  forecast: vi.fn(),
  events: vi.fn(),
  weather: vi.fn(),
  alerts: vi.fn(),
  status: vi.fn(),
  regions: vi.fn(),
  routes: vi.fn(),
  dispatchRoute: vi.fn(),
  completeRoute: vi.fn(),
  cancelRoute: vi.fn(),
  dismissRoute: vi.fn(),
  restoreRoute: vi.fn(),
}));

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, api: apiMock };
});

vi.mock("./components/Header", () => ({
  Header: ({ onRegionChange }: { onRegionChange: (region: string) => void }) => (
    <div>
      <span>header</span>
      <button type="button" onClick={() => onRegionChange("다른센터")}>권역 변경</button>
    </div>
  ),
}));
vi.mock("./components/StationMap", () => ({
  StationMap: ({
    stations,
    alerts,
    onSelect,
    selectedRoute,
  }: {
    stations: StationSummary[];
    alerts: Alert[];
    onSelect: (stationId: string) => void;
    selectedRoute: Route | null;
  }) => (
    <div>
      <span data-testid="map-stations">{stations.map((station) => station.sta_id).join(",")}</span>
      <span data-testid="map-alerts">{alerts.map((alert) => alert.sta_id).join(",")}</span>
      <span data-testid="map-route">{selectedRoute?.route_id ?? "none"}</span>
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
const ROUTES: Route[] = [
  {
    route_id: "11111111-1111-4111-8111-111111111111",
    region: "센터",
    status: "proposed",
    proposed_at: "2026-08-20T00:00:00Z",
    dispatched_at: null,
    completed_at: null,
    cancelled_at: null,
    dismissed_at: null,
    restored_from_route_id: null,
    stops: [
      {
        visit_order: 1,
        sta_id: "ST-1",
        sta_nm: "첫 번째 대여소",
        lat: 37.5,
        lon: 127,
        action: "pickup",
        bike_cnt: 2,
      },
      {
        visit_order: 2,
        sta_id: "ST-2",
        sta_nm: "두 번째 대여소",
        lat: 37.51,
        lon: 127.01,
        action: "dropoff",
        bike_cnt: 2,
      },
    ],
  },
];

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
  vi.setSystemTime(new Date("2026-08-20T00:05:00Z"));
  vi.clearAllMocks();
  apiMock.alerts.mockResolvedValue(ALERTS);
  apiMock.forecast.mockResolvedValue(FORECAST);
  apiMock.regions.mockResolvedValue([]);
  apiMock.routes.mockResolvedValue(ROUTES);
  apiMock.dispatchRoute.mockResolvedValue({ ...ROUTES[0], status: "dispatched" });
  apiMock.completeRoute.mockResolvedValue({ ...ROUTES[0], status: "completed" });
  apiMock.cancelRoute.mockResolvedValue({ ...ROUTES[0], status: "cancelled" });
});

afterEach(() => {
  cleanup();
  vi.clearAllTimers();
  vi.useRealTimers();
});

describe("App polling state", () => {
  it("작업 승인 버튼을 실제 상태 전이 API와 연결한다", async () => {
    apiMock.stations.mockResolvedValue(STATIONS);
    render(<App />);
    await settleRequests();
    await settleRequests();

    fireEvent.click(screen.getByRole("button", { name: "승인" }));
    await settleRequests();

    expect(apiMock.dispatchRoute).toHaveBeenCalledWith(ROUTES[0].route_id);
  });

  it("버튼으로 상태를 바꾼 결과 카드에 선택 테두리를 옮긴다", async () => {
    const second: Route = {
      ...ROUTES[0],
      route_id: "22222222-2222-4222-8222-222222222222",
      proposed_at: "2026-08-19T23:59:00Z",
    };
    apiMock.stations.mockResolvedValue(STATIONS);
    apiMock.routes.mockResolvedValue([ROUTES[0], second]);
    apiMock.dispatchRoute.mockResolvedValue({
      ...second,
      status: "dispatched",
      dispatched_at: "2026-08-20T00:05:00Z",
    });
    render(<App />);
    await settleRequests();
    await settleRequests();

    fireEvent.click(screen.getAllByRole("button", { name: "승인" })[1]);
    await settleRequests();

    expect(document.querySelector(`[data-route-id="${second.route_id}"]`)?.getAttribute("aria-current"))
      .toBe("true");
    expect(document.querySelector(`[data-route-id="${ROUTES[0].route_id}"]`)?.getAttribute("aria-current"))
      .toBeNull();
  });

  it("상태가 바뀌는 작업 카드를 view transition으로 이동한다", async () => {
    const scrollIntoView = vi.fn();
    Object.defineProperty(HTMLElement.prototype, "scrollIntoView", {
      configurable: true,
      value: scrollIntoView,
    });
    const startViewTransition = vi.fn((update: () => void) => {
      update();
      return {
        finished: Promise.resolve(),
        ready: Promise.resolve(),
        updateCallbackDone: Promise.resolve(),
        skipTransition: vi.fn(),
      };
    });
    Object.defineProperty(document, "startViewTransition", {
      configurable: true,
      value: startViewTransition,
    });
    apiMock.stations.mockResolvedValue(STATIONS);
    render(<App />);
    await settleRequests();
    await settleRequests();

    fireEvent.click(screen.getByRole("button", { name: "승인" }));
    await settleRequests();

    expect(startViewTransition).toHaveBeenCalledTimes(1);
    expect(scrollIntoView).toHaveBeenCalledWith({
      behavior: "smooth",
      block: "nearest",
      inline: "nearest",
    });
    expect(screen.getByRole("button", { name: "완료" })).not.toBeNull();
    Object.defineProperty(document, "startViewTransition", {
      configurable: true,
      value: undefined,
    });
    Object.defineProperty(HTMLElement.prototype, "scrollIntoView", {
      configurable: true,
      value: undefined,
    });
  });

  it("승인 전에 시작한 polling 응답이 승인 완료 상태를 덮지 못한다", async () => {
    const staleRoutes = deferred<Route[]>();
    apiMock.stations.mockResolvedValue(STATIONS);
    apiMock.routes.mockResolvedValueOnce(ROUTES).mockReturnValueOnce(staleRoutes.promise);
    render(<App />);
    await settleRequests();
    await settleRequests();

    await act(async () => {
      vi.advanceTimersByTime(30_000);
      await Promise.resolve();
    });
    fireEvent.click(screen.getByRole("button", { name: "승인" }));
    await settleRequests();
    expect(screen.getByRole("button", { name: "완료" })).not.toBeNull();

    staleRoutes.resolve(ROUTES);
    await settleRequests();

    expect(screen.queryByRole("button", { name: "승인" })).toBeNull();
    expect(screen.getByRole("button", { name: "완료" })).not.toBeNull();
  });

  it("완료된 작업을 삭제하면 목록에서 즉시 사라진다", async () => {
    const completed: Route = {
      ...ROUTES[0],
      status: "completed",
      dispatched_at: "2026-08-20T00:01:00Z",
      completed_at: "2026-08-20T00:02:00Z",
    };
    const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(true);
    apiMock.stations.mockResolvedValue(STATIONS);
    apiMock.routes.mockResolvedValue([completed]);
    apiMock.dismissRoute.mockResolvedValue({ ...completed, dismissed_at: "2026-08-20T00:03:00Z" });
    render(<App />);
    await settleRequests();
    await settleRequests();

    fireEvent.click(screen.getByRole("button", { name: "삭제" }));
    await settleRequests();

    expect(apiMock.dismissRoute).toHaveBeenCalledWith(completed.route_id);
    expect(screen.queryByRole("button", { name: "삭제" })).toBeNull();
    confirmSpy.mockRestore();
  });

  it("취소된 작업을 되돌리면 같은 작업이 진행 중으로 바뀐다", async () => {
    const cancelled: Route = {
      ...ROUTES[0],
      status: "cancelled",
      dispatched_at: "2026-08-20T00:01:00Z",
      cancelled_at: "2026-08-20T00:02:00Z",
    };
    const restored: Route = {
      ...ROUTES[0],
      status: "dispatched",
      dispatched_at: "2026-08-20T00:01:00Z",
      cancelled_at: null,
    };
    apiMock.stations.mockResolvedValue(STATIONS);
    apiMock.routes.mockResolvedValue([cancelled]);
    apiMock.restoreRoute.mockResolvedValue(restored);
    render(<App />);
    await settleRequests();
    await settleRequests();

    fireEvent.click(screen.getByRole("button", { name: "되돌리기" }));
    await settleRequests();

    expect(apiMock.restoreRoute).toHaveBeenCalledWith(cancelled.route_id);
    expect(screen.getByRole("button", { name: "완료" })).not.toBeNull();
    expect(screen.getByRole("button", { name: "취소" })).not.toBeNull();
    expect(screen.queryByRole("button", { name: "승인" })).toBeNull();
    expect(screen.queryByRole("button", { name: "되돌리기" })).toBeNull();
  });

  it("되돌리기 요청 중 대여소 모드로 바꾸면 이전 응답을 선택하지 않는다", async () => {
    const cancelled: Route = {
      ...ROUTES[0],
      status: "cancelled",
      dispatched_at: "2026-08-20T00:01:00Z",
      cancelled_at: "2026-08-20T00:02:00Z",
    };
    const restored: Route = {
      ...ROUTES[0],
      status: "dispatched",
      dispatched_at: "2026-08-20T00:01:00Z",
      cancelled_at: null,
    };
    const pendingRestore = deferred<Route>();
    apiMock.stations.mockResolvedValue(STATIONS);
    apiMock.routes.mockResolvedValue([cancelled]);
    apiMock.restoreRoute.mockReturnValue(pendingRestore.promise);
    render(<App />);
    await settleRequests();
    await settleRequests();

    fireEvent.click(screen.getByRole("button", { name: "되돌리기" }));
    fireEvent.click(screen.getByRole("button", { name: "대여소" }));
    pendingRestore.resolve(restored);
    await settleRequests();

    expect(screen.getByTestId("map-route").textContent).toBe("none");
  });

  it("되돌리기 요청 중 권역을 바꾸면 이전 응답을 선택하지 않는다", async () => {
    const cancelled: Route = {
      ...ROUTES[0],
      status: "cancelled",
      dispatched_at: "2026-08-20T00:01:00Z",
      cancelled_at: "2026-08-20T00:02:00Z",
    };
    const restored: Route = {
      ...ROUTES[0],
      status: "dispatched",
      dispatched_at: "2026-08-20T00:01:00Z",
      cancelled_at: null,
    };
    const pendingRestore = deferred<Route>();
    apiMock.stations.mockResolvedValue(STATIONS);
    apiMock.routes.mockResolvedValueOnce([cancelled]).mockResolvedValueOnce([]);
    apiMock.restoreRoute.mockReturnValue(pendingRestore.promise);
    render(<App />);
    await settleRequests();
    await settleRequests();

    fireEvent.click(screen.getByRole("button", { name: "되돌리기" }));
    fireEvent.click(screen.getByRole("button", { name: "권역 변경" }));
    pendingRestore.resolve(restored);
    await settleRequests();

    expect(screen.getByTestId("map-route").textContent).toBe("none");
  });

  it("되돌린 작업은 기존 카드를 갱신하고 중복 카드를 만들지 않는다", async () => {
    const cancelled: Route = {
      ...ROUTES[0],
      status: "cancelled",
      dispatched_at: "2026-08-20T00:01:00Z",
      cancelled_at: "2026-08-20T00:02:00Z",
    };
    const restored: Route = {
      ...ROUTES[0],
      status: "dispatched",
      dispatched_at: "2026-08-20T00:01:00Z",
      cancelled_at: null,
    };
    apiMock.stations.mockResolvedValue(STATIONS);
    apiMock.routes.mockResolvedValue([cancelled]);
    apiMock.restoreRoute.mockResolvedValue(restored);
    render(<App />);
    await settleRequests();
    await settleRequests();

    fireEvent.click(screen.getByRole("button", { name: "되돌리기" }));
    await settleRequests();

    expect(apiMock.restoreRoute).toHaveBeenCalledTimes(1);
    expect(screen.getAllByRole("button", { name: "완료" })).toHaveLength(1);
    expect(document.querySelectorAll(".route-card")).toHaveLength(1);
  });

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
    fireEvent.click(screen.getByRole("button", { name: "대여소" }));
    expect(screen.getByText("대여소 우선순위를 갱신하지 못했습니다.")).not.toBeNull();
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
