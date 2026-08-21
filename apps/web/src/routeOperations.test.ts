import { describe, expect, it } from "vitest";
import type { Alert, DispatchCenter, Route } from "./api";
import { alertScoreMap, estimateRoute, formatRouteDuration, isRebalanceRoute, routeKind, routePriority } from "./routeOperations";

const CENTERS: DispatchCenter[] = [{ region: "강남", lat: 37.5, lon: 127.0 }];
const ROUTE: Route = {
  route_id: "11111111-1111-4111-8111-111111111111",
  region: "강남",
  status: "proposed",
  proposed_at: "2026-08-21T03:00:00Z",
  dispatched_at: null,
  completed_at: null,
  cancelled_at: null,
  stops: [
    { visit_order: 1, sta_id: "ST-1", sta_nm: "회수", lat: 37.51, lon: 127.01, action: "pickup", bike_cnt: 4 },
    { visit_order: 2, sta_id: "ST-2", sta_nm: "공급", lat: 37.52, lon: 127.02, action: "dropoff", bike_cnt: 4 },
  ],
};
const ALERTS: Alert[] = [
  { sta_id: "ST-1", sta_nm: "회수", action_type: "retrieval_needed", urgency_score: 63, minutes_until_critical: 30, region: "강남" },
  { sta_id: "ST-2", sta_nm: "공급", action_type: "supply_needed", urgency_score: 91, minutes_until_critical: 8, region: "강남" },
];

describe("routeOperations", () => {
  it("센터 왕복 거리와 작업 시간을 보정해 5분 단위로 추정한다", () => {
    const estimate = estimateRoute(ROUTE, CENTERS);

    if (!estimate) throw new Error("경로 예상값이 필요합니다.");
    expect(estimate.distanceKm).toBeGreaterThan(0);
    expect(estimate.durationMinutes % 5).toBe(0);
    expect(formatRouteDuration(310)).toBe("5시간 10분");
  });

  it("작업 내 가장 높은 현재 긴급도를 우선도로 사용한다", () => {
    expect(routePriority(ROUTE, alertScoreMap(ALERTS))).toBe(91);
    expect(isRebalanceRoute(ROUTE)).toBe(true);
    expect(isRebalanceRoute({
      ...ROUTE,
      stops: ROUTE.stops.map((stop) => stop.action === "dropoff" ? { ...stop, bike_cnt: 2 } : stop),
    })).toBe(false);
    expect(routeKind(ROUTE)).toBe("재배치");
  });
});
