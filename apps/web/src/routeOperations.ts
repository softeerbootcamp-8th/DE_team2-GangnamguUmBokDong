import { ApiError } from "./api";
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
 * 후보 신선도의 기준 시각. 파이프라인이 게시한 일반 후보 중 가장 최근
 * proposed_at을 쓰고, 일반 후보가 없을 때만 복원 후보 시각을 쓴다.
 * 수동 복원 후보의 현재 시각이 기존 게시 후보를 한꺼번에 숨기면 안 된다.
 */
export function candidateReferenceMs(routes: Route[]): number | null {
  let latestPublished: number | null = null;
  let latestRestored: number | null = null;
  for (const route of routes) {
    if (route.status !== "proposed") continue;
    const ms = proposedAtMs(route);
    if (ms === null) continue;
    if (route.restored_from_route_id === null) {
      if (latestPublished === null || ms > latestPublished) latestPublished = ms;
    } else if (latestRestored === null || ms > latestRestored) {
      latestRestored = ms;
    }
  }
  return latestPublished ?? latestRestored;
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

export interface StationConflict {
  /** 대여소가 겹치는 진행 중 경로. 안내 문구에 작업 이름을 넣기 위해 경로 자체를 담는다. */
  blockingRoutes: Route[];
  /** 겹치는 대여소 수. */
  stationCount: number;
}

/**
 * 후보 route_id → 겹침. 진행 중 경로의 대여소를 한 번 인덱싱한 뒤 후보를 훑어
 * 전체 목록을 한 번에 판정한다.
 *
 * 진행 중(dispatched)만 판정 대상이다. 완료·취소된 작업은 그 대여소를 더 이상
 * 점유하지 않으므로, 진행 중 작업이 종료되면 다음 폴링에서 자동으로 승인 가능해진다.
 *
 * 입력 routes는 fetchAllRoutes가 isRebalanceRoute로 거른 뒤 넘어오므로 서버가
 * 보는 전체 대여소 겹침 집합의 진부분집합만 본다. 항상 프론트가 서버보다 덜 막는
 * 방향이라 불변식은 깨지지 않고, 누락분은 서버의 409 route_station_conflict가 잡는다.
 */
export function buildStationConflicts(routes: Route[]): Map<string, StationConflict> {
  const conflicts = new Map<string, StationConflict>();
  const dispatchedByStation = new Map<string, Route[]>();
  for (const route of routes) {
    if (route.status !== "dispatched") continue;
    for (const stop of route.stops) {
      const blockers = dispatchedByStation.get(stop.sta_id);
      if (blockers === undefined) {
        dispatchedByStation.set(stop.sta_id, [route]);
      } else if (!blockers.includes(route)) {
        blockers.push(route);
      }
    }
  }
  if (dispatchedByStation.size === 0) return conflicts;

  for (const route of routes) {
    if (route.status !== "proposed") continue;
    const stations = new Set<string>();
    const blockingRoutes: Route[] = [];
    for (const stop of route.stops) {
      const blockers = dispatchedByStation.get(stop.sta_id);
      if (blockers === undefined) continue;
      stations.add(stop.sta_id);
      for (const blocker of blockers) {
        if (!blockingRoutes.includes(blocker)) blockingRoutes.push(blocker);
      }
    }
    if (stations.size > 0) {
      conflicts.set(route.route_id, { blockingRoutes, stationCount: stations.size });
    }
  }
  return conflicts;
}

/** 한글 단어 끝이 받침(자음)으로 끝나는지 확인. 조사 "과/와" 선택에 사용. */
function hasTrailingConsonant(word: string): boolean {
  if (word.length === 0) return false;
  const lastChar = word.charCodeAt(word.length - 1);
  // 한글 범위 0xAC00 ~ 0xD7A3. 받침은 (코드 - 0xAC00) % 28이 0이 아닐 때.
  if (lastChar >= 0xAC00 && lastChar <= 0xD7A3) {
    return (lastChar - 0xAC00) % 28 !== 0;
  }
  return false;
}

/** 승인이 막힌 이유를 운영자가 읽을 한 줄로 만든다. */
export function describeStationConflict(conflict: StationConflict): string {
  const [first] = conflict.blockingRoutes;
  if (conflict.blockingRoutes.length === 1) {
    const kind = routeKind(first);
    const particle = hasTrailingConsonant(kind) ? "과" : "와";
    return `진행 중 '${first.region} ${kind}'${particle} 대여소 ${conflict.stationCount}곳 겹침`;
  }
  return `진행 중 작업 ${conflict.blockingRoutes.length}건과 대여소 ${conflict.stationCount}곳 겹침`;
}

export const ROUTE_ESTIMATE_BASIS = "직선거리×1.25 · 도심 18km/h · 정차 4분 · 1대당 30초";

/**
 * 상태 전이 실패를 운영자가 읽을 문구로 바꾼다. 대여소 겹침 거절만 전용 문구를
 * 주고, 나머지는 원인을 감추지 않도록 원문을 남긴다.
 */
export function routeTransitionMessage(error: unknown): string {
  if (
    error instanceof ApiError
    && error.status === 409
    && error.detail === "route_station_conflict"
  ) {
    return "진행 중 작업과 대여소가 겹쳐 승인할 수 없습니다.";
  }
  return error instanceof Error ? error.message : "작업 상태를 변경하지 못했습니다.";
}
