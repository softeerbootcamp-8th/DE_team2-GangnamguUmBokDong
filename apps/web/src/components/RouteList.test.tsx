// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { DispatchCenter, Route } from "../api";
import { RouteList } from "./RouteList";
import { RouteStopRail } from "./RouteStopRail";

const REGIONS: DispatchCenter[] = [
  { region: "강남", lat: 37.5, lon: 127.03 },
  { region: "영남", lat: 37.51, lon: 127.04 },
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
    dismissed_at: null,
    restored_from_route_id: null,
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
    dismissed_at: null,
    restored_from_route_id: null,
    stops: [],
  },
];

function renderRouteList(overrides: Partial<React.ComponentProps<typeof RouteList>> = {}) {
  const props: React.ComponentProps<typeof RouteList> = {
    routes: ROUTES,
    regions: REGIONS,
    selectedRouteId: null,
    busyRouteId: null,
    transitionError: null,
    onSelect: vi.fn(),
    onDispatch: vi.fn(),
    onComplete: vi.fn(),
    onCancel: vi.fn(),
    onDismiss: vi.fn(),
    onRestore: vi.fn(),
    ...overrides,
  };
  render(<RouteList {...props} />);
  return props;
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-08-21T03:05:00Z"));
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

describe("RouteList", () => {
  it("작업 상태별 2열 목록을 표시한다", () => {
    renderRouteList();
    expect(screen.getByRole("heading", { name: /작업 후보/ })).not.toBeNull();
    expect(screen.getByRole("heading", { name: /작업 현황/ })).not.toBeNull();
    expect(screen.getByText("대여소 2곳 · 회수 2대 · 공급 2대")).not.toBeNull();
    expect(screen.getByText("대여소 0곳 · 회수 0대 · 공급 0대")).not.toBeNull();
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

  it("완료된 작업에는 삭제 버튼만, 취소된 작업에는 되돌리기와 삭제 버튼을 준다", () => {
    renderRouteList({
      routes: [
        { ...ROUTES[1], route_id: "done", status: "completed", completed_at: "2026-08-21T02:30:00Z" },
        {
          ...ROUTES[1],
          route_id: "aborted",
          status: "cancelled",
          cancelled_at: "2026-08-21T02:40:00Z",
        },
      ],
    });

    expect(screen.getAllByRole("button", { name: "삭제" })).toHaveLength(2);
    expect(screen.getAllByRole("button", { name: "되돌리기" })).toHaveLength(1);
  });

  it("삭제는 확인을 받은 뒤에만 호출한다", () => {
    const confirmSpy = vi.spyOn(window, "confirm").mockReturnValue(false);
    const props = renderRouteList({
      routes: [
        { ...ROUTES[1], route_id: "done", status: "completed", completed_at: "2026-08-21T02:30:00Z" },
      ],
    });

    fireEvent.click(screen.getByRole("button", { name: "삭제" }));
    expect(props.onDismiss).not.toHaveBeenCalled();

    confirmSpy.mockReturnValue(true);
    fireEvent.click(screen.getByRole("button", { name: "삭제" }));
    expect(props.onDismiss).toHaveBeenCalledTimes(1);

    confirmSpy.mockRestore();
  });

  it("되돌리기는 확인 없이 바로 호출한다", () => {
    const props = renderRouteList({
      routes: [
        {
          ...ROUTES[1],
          route_id: "aborted",
          status: "cancelled",
          cancelled_at: "2026-08-21T02:40:00Z",
        },
      ],
    });

    fireEvent.click(screen.getByRole("button", { name: "되돌리기" }));

    expect(props.onRestore).toHaveBeenCalledTimes(1);
  });

  it("최신 제안보다 10분 이상 오래된 미승인 제안은 후보에서 제외한다", () => {
    renderRouteList({
      routes: [
        { ...ROUTES[0], route_id: "recent", proposed_at: "2026-08-21T03:00:00Z" },
        { ...ROUTES[0], route_id: "stale", proposed_at: "2026-08-21T02:49:00Z" },
      ],
    });

    expect(screen.getAllByRole("button", { name: "승인" })).toHaveLength(1);
  });

  it("진행 중인 작업이 종료된 작업보다 위에 표시된다", () => {
    renderRouteList({
      routes: [
        { ...ROUTES[1], route_id: "closed", status: "completed", proposed_at: "2026-08-21T03:00:00Z" },
        { ...ROUTES[1], route_id: "running", status: "dispatched", proposed_at: "2026-08-21T01:00:00Z" },
      ],
    });

    // 완료 버튼은 dispatched 카드에만 있으므로, 현황 열 첫 카드가 진행 중인지로 확인한다.
    const operationCards = screen.getByRole("heading", { name: /작업 현황/ })
      .parentElement!.querySelectorAll("li");
    expect(operationCards).toHaveLength(2);
    expect(operationCards[0].querySelector('button[aria-label="완료"]')).not.toBeNull();
  });

  it("처리 중인 작업은 버튼에 진행 상태를 표시한다", () => {
    renderRouteList({ busyRouteId: ROUTES[0].route_id });

    expect(screen.getByRole("button", { name: "처리 중" })).not.toBeNull();
    expect(screen.queryByRole("button", { name: "승인" })).toBeNull();
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
