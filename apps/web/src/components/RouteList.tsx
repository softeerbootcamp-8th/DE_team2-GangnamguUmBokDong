import {
  Check,
  CheckCheck,
  CircleX,
  Clock3,
  Loader2,
  Play,
  RotateCcw,
  Route as RouteIcon,
  Timer,
  Trash2,
} from "lucide-react";
import { useMemo } from "react";
import type { DispatchCenter, Route, RouteStatus } from "../api";
import {
  estimateRoute,
  formatRouteDuration,
  groupWorkRoutes,
  routeKind,
} from "../routeOperations";

const STATUS_META: Record<
  RouteStatus,
  { className: string; icon: typeof Clock3 }
> = {
  proposed: { className: "proposed", icon: Clock3 },
  dispatched: { className: "dispatched", icon: Play },
  completed: { className: "completed", icon: CheckCheck },
  cancelled: { className: "cancelled", icon: CircleX },
};

const KST_DATE = new Intl.DateTimeFormat("ko-KR", {
  timeZone: "Asia/Seoul",
  month: "numeric",
  day: "numeric",
});
const KST_TIME = new Intl.DateTimeFormat("ko-KR", {
  timeZone: "Asia/Seoul",
  hour: "2-digit",
  minute: "2-digit",
  hourCycle: "h23",
});

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
  onDismiss: (route: Route) => void;
  onRestore: (route: Route) => void;
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

function routeStateTime(route: Route, now = new Date()): string | null {
  const stateTime = route.status === "proposed"
    ? { label: "기준", value: route.proposed_at }
    : route.status === "dispatched"
      ? { label: "승인", value: route.dispatched_at }
      : route.status === "completed"
        ? { label: "완료", value: route.completed_at }
        : { label: "취소", value: route.cancelled_at };
  if (stateTime.value === null) return null;
  const parsed = new Date(stateTime.value);
  if (!Number.isFinite(parsed.getTime())) return null;
  const day = KST_DATE.format(parsed);
  const dayPrefix = day === KST_DATE.format(now) ? "" : `${day} `;
  return `${stateTime.label} ${dayPrefix}${KST_TIME.format(parsed)}`;
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
  onDismiss,
  onRestore,
}: Props) {
  const { candidates, operations, hiddenCandidateCount, hiddenOperationCount } = useMemo(
    () => groupWorkRoutes(routes, { keepRouteId: selectedRouteId }),
    [routes, selectedRouteId],
  );
  const estimates = useMemo(() => {
    const byRouteId = new Map<string, ReturnType<typeof estimateRoute>>();
    routes.forEach((route) => byRouteId.set(route.route_id, estimateRoute(route, regions)));
    return byRouteId;
  }, [routes, regions]);

  function confirmDismiss(route: Route) {
    // 삭제하면 화면에서 다시 꺼낼 방법이 없다. 실수 한 번을 막는 값이 크다.
    if (!window.confirm("이 작업을 목록에서 삭제할까요? 되돌릴 수 없습니다.")) return;
    onDismiss(route);
  }

  function renderRouteCard(route: Route) {
    const estimate = estimates.get(route.route_id) ?? null;
    const status = STATUS_META[route.status];
    const StatusIcon = status.icon;
    const isSelected = route.route_id === selectedRouteId;
    const isBusy = route.route_id === busyRouteId;
    const transitionsBlocked = busyRouteId !== null;
    const stateTime = routeStateTime(route);
    return (
      <li key={route.route_id}>
        <article
          className={`route-card${isSelected ? " selected" : ""} has-actions`}
          aria-current={isSelected ? "true" : undefined}
          data-route-id={route.route_id}
          style={isBusy
            ? { viewTransitionName: `route-${route.route_id.replace(/-/g, "")}` }
            : undefined}
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
              {(estimate || stateTime) && (
                <span className="route-card-meta">
                  {estimate && (
                    <span title="직선거리×1.25, 도심 18km/h, 정차 4분, 자전거 1대당 30초 기준">
                      <Timer size={11} aria-hidden="true" />
                      예상 {estimate.distanceKm.toFixed(1)}km · 약 {formatRouteDuration(estimate.durationMinutes)}
                    </span>
                  )}
                  {stateTime && <span>{stateTime}</span>}
                </span>
              )}
            </span>
          </button>

          <div className="route-card-actions">
            {route.status === "proposed" && (
              <button
                type="button"
                className={`route-action primary icon-only${isBusy ? " is-busy" : ""}`}
                disabled={transitionsBlocked}
                onClick={() => onDispatch(route)}
                aria-label={isBusy ? "처리 중" : "승인"}
                title={isBusy ? "승인 처리 중" : "작업 승인"}
              >
                {isBusy
                  ? <Loader2 size={14} aria-hidden="true" className="route-action-spinner" />
                  : <Check size={14} aria-hidden="true" />}
              </button>
            )}
            {route.status === "dispatched" && (
              <>
                <button
                  type="button"
                  className={`route-action primary icon-only${isBusy ? " is-busy" : ""}`}
                  disabled={transitionsBlocked}
                  onClick={() => onComplete(route)}
                  aria-label={isBusy ? "처리 중" : "완료"}
                  title={isBusy ? "처리 중" : "작업 완료"}
                >
                  {isBusy
                    ? <Loader2 size={14} aria-hidden="true" className="route-action-spinner" />
                    : <CheckCheck size={14} aria-hidden="true" />}
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
            {route.status === "completed" && (
              <button
                type="button"
                className={`route-action danger icon-only${isBusy ? " is-busy" : ""}`}
                disabled={transitionsBlocked}
                onClick={() => confirmDismiss(route)}
                aria-label={isBusy ? "처리 중" : "삭제"}
                title={isBusy ? "처리 중" : "작업 삭제"}
              >
                {isBusy
                  ? <Loader2 size={14} aria-hidden="true" className="route-action-spinner" />
                  : <Trash2 size={14} aria-hidden="true" />}
              </button>
            )}
            {route.status === "cancelled" && (
              <>
                <button
                  type="button"
                  className={`route-action primary icon-only${isBusy ? " is-busy" : ""}`}
                  disabled={transitionsBlocked}
                  onClick={() => onRestore(route)}
                  aria-label={isBusy ? "처리 중" : "되돌리기"}
                  title={isBusy ? "처리 중" : "작업 중으로 되돌리기"}
                >
                  {isBusy
                    ? <Loader2 size={14} aria-hidden="true" className="route-action-spinner" />
                    : <RotateCcw size={14} aria-hidden="true" />}
                </button>
                <button
                  type="button"
                  className="route-action danger icon-only"
                  disabled={transitionsBlocked}
                  onClick={() => confirmDismiss(route)}
                  aria-label="삭제"
                  title="작업 삭제"
                >
                  <Trash2 size={14} aria-hidden="true" />
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
          {hiddenCandidateCount > 0 && (
            <p className="column-note">기한이 지난 제안 {hiddenCandidateCount}건은 표시하지 않습니다.</p>
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
            <>
              <ul className="route-column-list">{operations.map(renderRouteCard)}</ul>
              {hiddenOperationCount > 0 && (
                <p className="column-note">이전 종료 작업 {hiddenOperationCount}건은 표시하지 않습니다.</p>
              )}
            </>
          )}
        </section>
      </div>
    </div>
  );
}
