import type { DispatchCenter, Route, RouteStatus } from "./api";

const EARTH_RADIUS_KM = 6371;
const ROAD_DISTANCE_FACTOR = 1.25;
const URBAN_TRUCK_SPEED_KMH = 18;
const STOP_SERVICE_MINUTES = 4;
const BIKE_HANDLING_MINUTES = 0.5;
const CANDIDATE_WINDOW_MS = 10 * 60 * 1000;
const OPERATION_HISTORY_LIMIT = 30;

// dispatched(진행 중) → proposed → completed → cancelled 순.
// 운영자가 지금 처리해야 하는 작업이 항상 목록 위에 있어야 한다.
const STATUS_ORDER: Record<RouteStatus, number> = {
  dispatched: 0,
  proposed: 1,
  completed: 2,
  cancelled: 3,
};

interface Point {
  lat: number;
  lon: number;
}

export interface RouteEstimate {
  distanceKm: number;
  durationMinutes: number;
}

function haversineKm(left: Point, right: Point): number {
  const toRadians = (degrees: number) => (degrees * Math.PI) / 180;
  const latitudeDelta = toRadians(right.lat - left.lat);
  const longitudeDelta = toRadians(right.lon - left.lon);
  const latitudeSin = Math.sin(latitudeDelta / 2);
  const longitudeSin = Math.sin(longitudeDelta / 2);
  const h = latitudeSin * latitudeSin
    + Math.cos(toRadians(left.lat))
      * Math.cos(toRadians(right.lat))
      * longitudeSin
      * longitudeSin;
  return 2 * EARTH_RADIUS_KM * Math.asin(Math.sqrt(h));
}

export function estimateRoute(route: Route, centers: DispatchCenter[]): RouteEstimate | null {
  const center = centers.find((item) => item.region === route.region);
  if (!center || route.stops.length === 0) return null;

  const orderedStops = [...route.stops].sort((left, right) => left.visit_order - right.visit_order);
  const path: Point[] = [center, ...orderedStops, center];
  const directDistanceKm = path.slice(1).reduce(
    (total, point, index) => total + haversineKm(path[index], point),
    0,
  );
  const distanceKm = directDistanceKm * ROAD_DISTANCE_FACTOR;
  const handledBikes = orderedStops.reduce((total, stop) => total + stop.bike_cnt, 0);
  const rawDurationMinutes = (distanceKm / URBAN_TRUCK_SPEED_KMH) * 60
    + orderedStops.length * STOP_SERVICE_MINUTES
    + handledBikes * BIKE_HANDLING_MINUTES;

  return {
    distanceKm: Math.round(distanceKm * 10) / 10,
    durationMinutes: Math.max(5, Math.ceil(rawDurationMinutes / 5) * 5),
  };
}

export function formatRouteDuration(minutes: number): string {
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  if (hours === 0) return `${remainingMinutes}분`;
  if (remainingMinutes === 0) return `${hours}시간`;
  return `${hours}시간 ${remainingMinutes}분`;
}

export function isRebalanceRoute(route: Route): boolean {
  const pickupQuantity = route.stops
    .filter((stop) => stop.action === "pickup")
    .reduce((total, stop) => total + stop.bike_cnt, 0);
  const dropoffQuantity = route.stops
    .filter((stop) => stop.action === "dropoff")
    .reduce((total, stop) => total + stop.bike_cnt, 0);
  return pickupQuantity > 0 && pickupQuantity === dropoffQuantity;
}

function proposedAtMs(route: Route): number | null {
  const parsed = Date.parse(route.proposed_at);
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * 후보 신선도의 기준 시각. 브라우저 시계가 아니라 응답에 담긴 가장 최근
 * proposed_at을 쓴다. 운영 PC 시계가 몇 분 틀어져도 방금 생성된 후보가
 * 아무 안내 없이 전부 사라지는 일이 없어야 한다.
 */
export function candidateReferenceMs(routes: Route[]): number | null {
  let latest: number | null = null;
  for (const route of routes) {
    if (route.status !== "proposed") continue;
    const ms = proposedAtMs(route);
    if (ms === null) continue;
    if (latest === null || ms > latest) latest = ms;
  }
  return latest;
}

export function isFreshCandidate(route: Route, referenceMs: number | null): boolean {
  if (route.status !== "proposed") return false;
  const ms = proposedAtMs(route);
  // 시각을 해석할 수 없으면 숨기는 대신 노출한다. 놓치는 쪽이 더 위험하다.
  if (ms === null || referenceMs === null) return true;
  return ms >= referenceMs - CANDIDATE_WINDOW_MS;
}

function byProposedAtDesc(left: Route, right: Route): number {
  return (proposedAtMs(right) ?? 0) - (proposedAtMs(left) ?? 0);
}

export interface WorkRouteGroups {
  candidates: Route[];
  operations: Route[];
  /** 후보 창을 벗어나 "작업 후보"에서 제외한 미승인 제안 수. */
  hiddenCandidateCount: number;
  /** 상한을 넘겨 "작업 현황"에서 잘라낸 종료 작업 수. */
  hiddenOperationCount: number;
}

/**
 * 작업 목록을 "작업 후보"와 "작업 현황" 두 열로 나눈다.
 *
 * - 후보는 최신 제안 기준 10분 이내의 미승인 제안. keepRouteId로 지정한 경로는
 *   운영자가 지금 보고 있는 항목이므로 창을 벗어나도 유지한다.
 * - 현황은 상태 우선순위로 정렬하고, 종료 작업은 최근 것부터 상한까지만 남긴다.
 *   /routes가 전체 이력을 내려주므로 상한이 없으면 목록이 하루 종일 길어진다.
 */
export function groupWorkRoutes(
  routes: Route[],
  options: { keepRouteId?: string | null } = {},
): WorkRouteGroups {
  const referenceMs = candidateReferenceMs(routes);
  const keepRouteId = options.keepRouteId ?? null;
  const isCandidate = (route: Route) =>
    route.status === "proposed"
    && (isFreshCandidate(route, referenceMs) || route.route_id === keepRouteId);

  const candidates = routes.filter(isCandidate).sort(byProposedAtDesc);
  const proposedCount = routes.filter((route) => route.status === "proposed").length;
  const hiddenCandidateCount = proposedCount - candidates.length;
  const active = routes
    .filter((route) => route.status !== "proposed")
    .sort((left, right) =>
      STATUS_ORDER[left.status] - STATUS_ORDER[right.status] || byProposedAtDesc(left, right));

  const closedFrom = active.findIndex((route) => STATUS_ORDER[route.status] > STATUS_ORDER.proposed);
  if (closedFrom === -1 || active.length - closedFrom <= OPERATION_HISTORY_LIMIT) {
    return { candidates, operations: active, hiddenCandidateCount, hiddenOperationCount: 0 };
  }
  return {
    candidates,
    operations: active.slice(0, closedFrom + OPERATION_HISTORY_LIMIT),
    hiddenCandidateCount,
    hiddenOperationCount: active.length - closedFrom - OPERATION_HISTORY_LIMIT,
  };
}

export function routeKind(route: Route): "재배치" | "센터 회수" | "센터 공급" {
  const hasPickup = route.stops.some((stop) => stop.action === "pickup");
  if (isRebalanceRoute(route)) return "재배치";
  return hasPickup ? "센터 회수" : "센터 공급";
}

export const ROUTE_ESTIMATE_BASIS = "직선거리×1.25 · 도심 18km/h · 정차 4분 · 1대당 30초";
