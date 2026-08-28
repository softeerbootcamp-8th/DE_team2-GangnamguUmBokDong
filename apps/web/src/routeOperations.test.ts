import { describe, expect, it, vi } from "vitest";
import type { DispatchCenter, Route } from "./api";
import { estimateRoute, formatRouteDuration, groupWorkRoutes, isRebalanceRoute, routeKind, routeTransitionMessage } from "./routeOperations";

const CENTERS: DispatchCenter[] = [{ region: "강남", lat: 37.5, lon: 127.0 }];
const ROUTE: Route = {
  route_id: "11111111-1111-4111-8111-111111111111",
  work_no: null,
  region: "강남",
  status: "proposed",
  proposed_at: "2026-08-21T03:00:00Z",
  dispatched_at: null,
  completed_at: null,
  cancelled_at: null,
  dismissed_at: null,
  restored_from_route_id: null,
  route_priority_no: 1,
  stops: [
    { visit_order: 1, sta_id: "ST-1", sta_nm: "회수", lat: 37.51, lon: 127.01, action: "pickup", bike_cnt: 4 },
    { visit_order: 2, sta_id: "ST-2", sta_nm: "공급", lat: 37.52, lon: 127.02, action: "dropoff", bike_cnt: 4 },
  ],
};

describe("routeOperations", () => {
  it("센터 왕복 거리와 작업 시간을 보정해 5분 단위로 추정한다", () => {
    const estimate = estimateRoute(ROUTE, CENTERS);

    if (!estimate) throw new Error("경로 예상값이 필요합니다.");
    expect(estimate.distanceKm).toBeGreaterThan(0);
    expect(estimate.durationMinutes % 5).toBe(0);
    expect(formatRouteDuration(310)).toBe("5시간 10분");
  });

  it("재배치 경로와 센터 경로를 구분한다", () => {
    expect(isRebalanceRoute(ROUTE)).toBe(true);
    expect(isRebalanceRoute({
      ...ROUTE,
      stops: ROUTE.stops.map((stop) => stop.action === "dropoff" ? { ...stop, bike_cnt: 2 } : stop),
    })).toBe(false);
    expect(routeKind(ROUTE)).toBe("재배치");
  });

  it("최신 제안 기준 10분 이내의 미승인 제안만 후보로 남긴다", () => {
    const fresh = { ...ROUTE, route_id: "fresh", proposed_at: "2026-08-21T03:00:00Z" };
    const stale = { ...ROUTE, route_id: "stale", proposed_at: "2026-08-21T02:49:00Z" };

    const groups = groupWorkRoutes([stale, fresh]);

    expect(groups.candidates.map((route) => route.route_id)).toEqual(["fresh"]);
    expect(groups.hiddenCandidateCount).toBe(1);
    expect(groups.operations).toEqual([]);
  });

  it("같은 권역 후보는 서버가 계산한 작업 우선순위로 표시한다", () => {
    const routes = [
      { ...ROUTE, route_id: "third", route_priority_no: 3 },
      { ...ROUTE, route_id: "first", route_priority_no: 1 },
      { ...ROUTE, route_id: "second", route_priority_no: 2 },
    ];

    expect(groupWorkRoutes(routes).candidates.map((route) => route.route_id)).toEqual([
      "first",
      "second",
      "third",
    ]);
  });

  it("복원 후보가 기존 게시 후보의 신선도 기준을 밀어내지 않는다", () => {
    const published = {
      ...ROUTE,
      route_id: "published",
      proposed_at: "2026-08-21T03:00:00Z",
    };
    const restored = {
      ...ROUTE,
      route_id: "restored",
      proposed_at: "2026-08-21T07:00:00Z",
      restored_from_route_id: "cancelled-origin",
    };

    const groups = groupWorkRoutes([published, restored]);

    expect(groups.candidates.map((route) => route.route_id)).toEqual(["restored", "published"]);
    expect(groups.hiddenCandidateCount).toBe(0);
  });

  it("브라우저 시계가 틀어져도 최신 후보를 숨기지 않는다", () => {
    // 시스템 시계를 하루 앞으로 옮겨도 판정 기준은 응답의 proposed_at이다.
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-08-22T03:00:00Z"));
    try {
      expect(groupWorkRoutes([ROUTE]).candidates).toHaveLength(1);
      vi.setSystemTime(new Date("2026-08-20T03:00:00Z"));
      expect(groupWorkRoutes([ROUTE]).candidates).toHaveLength(1);
    } finally {
      vi.useRealTimers();
    }
  });

  it("proposed_at을 해석할 수 없으면 숨기지 않고 후보로 노출한다", () => {
    const broken = { ...ROUTE, route_id: "broken", proposed_at: "not-a-date" };

    expect(groupWorkRoutes([broken]).candidates.map((route) => route.route_id)).toEqual(["broken"]);
  });

  it("선택 중인 제안은 후보 창을 벗어나도 유지한다", () => {
    const fresh = { ...ROUTE, route_id: "fresh", proposed_at: "2026-08-21T03:00:00Z" };
    const stale = { ...ROUTE, route_id: "stale", proposed_at: "2026-08-21T02:49:00Z" };

    const groups = groupWorkRoutes([stale, fresh], { keepRouteId: "stale" });

    expect(groups.candidates.map((route) => route.route_id)).toEqual(["fresh", "stale"]);
  });

  it("작업 현황은 진행 중 작업을 종료 작업보다 위에 둔다", () => {
    const dispatched = {
      ...ROUTE,
      route_id: "dispatched",
      status: "dispatched" as const,
      proposed_at: "2026-08-21T01:00:00Z",
    };
    const completed = {
      ...ROUTE,
      route_id: "completed",
      status: "completed" as const,
      proposed_at: "2026-08-21T02:00:00Z",
    };
    const cancelled = {
      ...ROUTE,
      route_id: "cancelled",
      status: "cancelled" as const,
      proposed_at: "2026-08-21T03:00:00Z",
    };

    const groups = groupWorkRoutes([cancelled, completed, dispatched]);

    expect(groups.operations.map((route) => route.route_id))
      .toEqual(["dispatched", "completed", "cancelled"]);
  });

  it("서버가 시간창으로 조회한 종료 작업을 개수 제한 없이 유지한다", () => {
    const closed = Array.from({ length: 35 }, (_, index) => ({
      ...ROUTE,
      route_id: `closed-${index}`,
      status: "completed" as const,
      proposed_at: new Date(Date.UTC(2026, 7, 21, 3, index)).toISOString(),
    }));

    const groups = groupWorkRoutes(closed);

    expect(groups.operations).toHaveLength(35);
    expect(groups.operations[0].route_id).toBe("closed-34");
  });

  it("상태 전이 오류는 원인을 감추지 않고 표시한다", () => {
    const error = new Error("작업 전이 실패");

    expect(routeTransitionMessage(error)).toBe(error.message);
    expect(routeTransitionMessage("문자열")).toBe("작업 상태를 변경하지 못했습니다.");
  });
});
