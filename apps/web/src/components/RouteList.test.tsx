// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { Alert, DispatchCenter, Route } from "../api";
import { RouteList } from "./RouteList";
import { RouteStopRail } from "./RouteStopRail";

const REGIONS: DispatchCenter[] = [
  { region: "강남", lat: 37.5, lon: 127.03 },
  { region: "영남", lat: 37.51, lon: 127.04 },
];

const ALERTS: Alert[] = [
  {
    sta_id: "ST-1",
    sta_nm: "첫 번째 대여소",
    action_type: "retrieval_needed",
    urgency_score: 82,
    minutes_until_critical: 12,
    region: "강남",
  },
  {
    sta_id: "ST-2",
    sta_nm: "두 번째 대여소",
    action_type: "supply_needed",
    urgency_score: 75,
    minutes_until_critical: 20,
    region: "강남",
  },
];

const ROUTES: Route[] = [
  {
    route_id: "11111111-1111-4111-8111-111111111111",
    region: "강남",
    status: "proposed",
    proposed_at: "2026-08-21T03:00:00Z",
    dispatched_at: null,
    completed_at: null,
    cancelled_at: null,
    stops: [
      {
        visit_order: 1,
        sta_id: "ST-1",
        sta_nm: "첫 번째 대여소",
        lat: 37.5,
        lon: 127.03,
        action: "pickup",
        bike_cnt: 2,
      },
      {
        visit_order: 2,
        sta_id: "ST-2",
        sta_nm: "두 번째 대여소",
        lat: 37.51,
        lon: 127.04,
        action: "dropoff",
        bike_cnt: 2,
      },
    ],
  },
  {
    route_id: "22222222-2222-4222-8222-222222222222",
    region: "영남",
    status: "dispatched",
    proposed_at: "2026-08-21T02:00:00Z",
    dispatched_at: "2026-08-21T02:05:00Z",
    completed_at: null,
    cancelled_at: null,
    stops: [],
  },
];

function renderRouteList(overrides: Partial<React.ComponentProps<typeof RouteList>> = {}) {
  const props: React.ComponentProps<typeof RouteList> = {
    routes: ROUTES,
    alerts: ALERTS,
    regions: REGIONS,
    selectedRegion: "all",
    selectedRouteId: null,
    busyRouteId: null,
    transitionError: null,
    onRegionChange: vi.fn(),
    onSelect: vi.fn(),
    onDispatch: vi.fn(),
    onComplete: vi.fn(),
    onCancel: vi.fn(),
    ...overrides,
  };
  render(<RouteList {...props} />);
  return props;
}

afterEach(cleanup);

describe("RouteList", () => {
  it("권역 선택과 상태 탭 필터를 연결한다", () => {
    const props = renderRouteList();

    fireEvent.click(screen.getByRole("button", { name: "영남" }));
    expect(props.onRegionChange).toHaveBeenCalledWith("영남");

    fireEvent.click(screen.getByRole("tab", { name: "승인 대기" }));
    expect(screen.getByText("대여소 2곳 · 회수 2대 · 공급 2대")).not.toBeNull();
    expect(screen.queryByText("대여소 0곳 · 회수 0대 · 공급 0대")).toBeNull();
  });

  it("작업 상태에 맞는 승인·완료·취소 동작을 호출한다", () => {
    const props = renderRouteList();

    fireEvent.click(screen.getByRole("button", { name: "승인" }));
    fireEvent.click(screen.getByRole("button", { name: "완료" }));
    fireEvent.click(screen.getByRole("button", { name: "취소" }));

    expect(props.onDispatch).toHaveBeenCalledWith(ROUTES[0]);
    expect(props.onComplete).toHaveBeenCalledWith(ROUTES[1]);
    expect(props.onCancel).toHaveBeenCalledWith(ROUTES[1]);
  });
});

describe("RouteStopRail", () => {
  it("방문 순서의 대여소를 선택하면 상세 선택 콜백을 호출한다", () => {
    const onSelectStation = vi.fn();
    render(
      <RouteStopRail
        route={ROUTES[0]}
        selectedStationId="ST-1"
        onSelectStation={onSelectStation}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: /두 번째 대여소/ }));

    expect(onSelectStation).toHaveBeenCalledWith("ST-2");
    expect(screen.getByText("공급 2대")).not.toBeNull();
    expect(screen.getByText("복귀 · 작업 완료")).not.toBeNull();
    expect(screen.queryByText("예상 거리")).toBeNull();
  });
});
