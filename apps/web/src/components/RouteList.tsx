import { Check, CheckCheck, CircleX, Clock3, Play, Route as RouteIcon, Timer } from "lucide-react";
import { useMemo } from "react";
import type { DispatchCenter, Route, RouteStatus } from "../api";
import { estimateRoute, formatRouteDuration, isVisibleWorkRoute, routeKind } from "../routeOperations";

const STATUS_META: Record<
  RouteStatus,
  { className: string; icon: typeof Clock3 }
> = {
  proposed: { className: "proposed", icon: Clock3 },
  dispatched: { className: "dispatched", icon: Play },
  completed: { className: "completed", icon: CheckCheck },
  cancelled: { className: "cancelled", icon: CircleX },
};

interface Props {
  routes: Route[];
  regions: DispatchCenter[];
  selectedRouteId: string | null;
  busyRouteId: string | null;
  transitionError: string | null;
  onSelect: (route: Route) => void;
  onDispatch: (route: Route) => void;
  onComplete: (route: Route) => void;
  onCancel: (route: Route) => void;
}

function routeSummary(route: Route): string {
  const pickup = route.stops
    .filter((stop) => stop.action === "pickup")
    .reduce((total, stop) => total + stop.bike_cnt, 0);
  const dropoff = route.stops
    .filter((stop) => stop.action === "dropoff")
    .reduce((total, stop) => total + stop.bike_cnt, 0);
  return `대여소 ${route.stops.length}곳 · 회수 ${pickup}대 · 공급 ${dropoff}대`;
}

export function RouteList({
  routes,
  regions,
  selectedRouteId,
  busyRouteId,
  transitionError,
  onSelect,
  onDispatch,
  onComplete,
  onCancel,
}: Props) {
  const now = Date.now();
  const items = useMemo(() => {
    return routes
      .filter((route) => isVisibleWorkRoute(route, now))
      .map((route) => ({
        route,
        estimate: estimateRoute(route, regions),
      }));
  }, [routes, regions, now]);
  const candidates = items.filter(({ route }) => route.status === "proposed");
  const operations = items.filter(({ route }) => route.status !== "proposed");

  function renderRouteCard({ route, estimate }: (typeof items)[number]) {
    const status = STATUS_META[route.status];
    const StatusIcon = status.icon;
    const isSelected = route.route_id === selectedRouteId;
    const isBusy = route.route_id === busyRouteId;
    const transitionsBlocked = busyRouteId !== null;
    const hasActions = route.status === "proposed" || route.status === "dispatched";
    return (
      <li key={route.route_id}>
        <article
          className={`route-card${isSelected ? " selected" : ""}${hasActions ? " has-actions" : ""}`}
          aria-current={isSelected ? "true" : undefined}
        >
          <button type="button" className="route-card-main" onClick={() => onSelect(route)}>
            <span className={`route-status-icon ${status.className}`} aria-hidden="true">
              <StatusIcon size={16} />
            </span>
            <span className="route-card-copy">
              <span className="route-card-title">
                <RouteIcon size={15} aria-hidden="true" />
                {route.region} {routeKind(route)}
              </span>
              <span className="route-card-summary">{routeSummary(route)}</span>
              {estimate && (
                <span className="route-card-meta">
                  <span title="직선거리×1.25, 도심 18km/h, 정차 4분, 자전거 1대당 30초 기준">
                    <Timer size={11} aria-hidden="true" />
                    예상 {estimate.distanceKm.toFixed(1)}km · 약 {formatRouteDuration(estimate.durationMinutes)}
                  </span>
                </span>
              )}
            </span>
          </button>

          <div className="route-card-actions">
            {route.status === "proposed" && (
              <button
                type="button"
                className="route-action primary icon-only"
                disabled={transitionsBlocked}
                onClick={() => onDispatch(route)}
                aria-label={isBusy ? "처리 중" : "승인"}
                title={isBusy ? "승인 처리 중" : "작업 승인"}
              >
                <Check size={14} aria-hidden="true" />
              </button>
            )}
            {route.status === "dispatched" && (
              <>
                <button
                  type="button"
                  className="route-action primary icon-only"
                  disabled={transitionsBlocked}
                  onClick={() => onComplete(route)}
                  aria-label="완료"
                  title="작업 완료"
                >
                  <CheckCheck size={14} aria-hidden="true" />
                </button>
                <button
                  type="button"
                  className="route-action danger icon-only"
                  disabled={transitionsBlocked}
                  onClick={() => onCancel(route)}
                  aria-label="취소"
                  title="작업 취소"
                >
                  <CircleX size={14} aria-hidden="true" />
                </button>
              </>
            )}
          </div>
        </article>
      </li>
    );
  }

  return (
    <div className="route-list-wrap">
      {transitionError && (
        <p className="route-transition-error" role="status">
          {transitionError}
        </p>
      )}

      <div className="route-workspace">
        <section className="route-column" aria-labelledby="candidate-routes-heading">
          <h3 id="candidate-routes-heading">
            <span>작업 후보</span>
            <strong>{candidates.length}</strong>
          </h3>
          {candidates.length === 0 ? (
            <p className="empty-state">작업 후보가 없습니다.</p>
          ) : (
            <ul className="route-column-list">{candidates.map(renderRouteCard)}</ul>
          )}
        </section>

        <section className="route-column" aria-labelledby="active-routes-heading">
          <h3 id="active-routes-heading">
            <span>작업 현황</span>
            <strong>{operations.length}</strong>
          </h3>
          {operations.length === 0 ? (
            <p className="empty-state">진행되었거나 종료된 작업이 없습니다.</p>
          ) : (
            <ul className="route-column-list">{operations.map(renderRouteCard)}</ul>
          )}
        </section>
      </div>
    </div>
  );
}
