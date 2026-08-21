import { Check, CheckCheck, CircleX, Clock3, Play, Route as RouteIcon, Timer } from "lucide-react";
import { useMemo, useState } from "react";
import type { Alert, DispatchCenter, Route, RouteStatus } from "../api";
import { formatIsoTime } from "../format";
import { alertScoreMap, estimateRoute, formatRouteDuration, routeKind, routePriority } from "../routeOperations";
import { RegionTabs } from "./RegionTabs";

type RouteTab = "all" | RouteStatus;

const TABS: { key: RouteTab; label: string }[] = [
  { key: "all", label: "전체" },
  { key: "proposed", label: "승인 대기" },
  { key: "dispatched", label: "작업중" },
  { key: "completed", label: "완료" },
  { key: "cancelled", label: "취소" },
];

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
  alerts: Alert[];
  regions: DispatchCenter[];
  selectedRegion: string;
  selectedRouteId: string | null;
  busyRouteId: string | null;
  transitionError: string | null;
  onRegionChange: (region: string) => void;
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
  alerts,
  regions,
  selectedRegion,
  selectedRouteId,
  busyRouteId,
  transitionError,
  onRegionChange,
  onSelect,
  onDispatch,
  onComplete,
  onCancel,
}: Props) {
  const [tab, setTab] = useState<RouteTab>("all");
  const priorityScores = useMemo(() => alertScoreMap(alerts), [alerts]);
  const ordered = useMemo(() => {
    const statusOrder: Record<RouteStatus, number> = {
      dispatched: 0,
      proposed: 1,
      completed: 2,
      cancelled: 3,
    };
    return routes
      .filter((route) => tab === "all" || route.status === tab)
      .map((route) => ({
        route,
        estimate: estimateRoute(route, regions),
        priority: routePriority(route, priorityScores),
      }))
      .sort((left, right) => {
        if (tab === "all" && statusOrder[left.route.status] !== statusOrder[right.route.status]) {
          return statusOrder[left.route.status] - statusOrder[right.route.status];
        }
        if (right.priority !== left.priority) return right.priority - left.priority;
        return left.route.route_id.localeCompare(right.route.route_id);
      });
  }, [routes, tab, regions, priorityScores]);

  return (
    <div className="route-list-wrap">
      <RegionTabs regions={regions} selectedRegion={selectedRegion} onChange={onRegionChange} />

      <div className="filter-tab-row" role="tablist" aria-label="작업 상태">
        {TABS.map((item) => (
          <button
            key={item.key}
            type="button"
            role="tab"
            aria-selected={tab === item.key}
            className={`alert-tab${tab === item.key ? " active" : ""}`}
            onClick={() => setTab(item.key)}
          >
            {item.label}
          </button>
        ))}
      </div>

      {transitionError && (
        <p className="route-transition-error" role="status">
          {transitionError}
        </p>
      )}

      {ordered.length === 0 ? (
        <p className="empty-state">해당 상태의 작업이 없습니다.</p>
      ) : (
        <ul className="route-list">
          {ordered.map(({ route, estimate, priority }) => {
            const status = STATUS_META[route.status];
            const StatusIcon = status.icon;
            const isSelected = route.route_id === selectedRouteId;
            const isBusy = route.route_id === busyRouteId;
            const transitionsBlocked = busyRouteId !== null;
            return (
              <li key={route.route_id}>
                <article
                  className={`route-card${isSelected ? " selected" : ""}`}
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
                      <span className="route-card-meta">
                        <span>현재 우선도 {Math.round(priority)}</span>
                        {estimate && (
                          <span title="직선거리×1.25, 도심 18km/h, 정차 4분, 자전거 1대당 30초 기준">
                            <Timer size={11} aria-hidden="true" />
                            예상 {estimate.distanceKm.toFixed(1)}km · 약 {formatRouteDuration(estimate.durationMinutes)}
                          </span>
                        )}
                        <span>제안 {formatIsoTime(route.proposed_at, { hour: "2-digit", minute: "2-digit" })}</span>
                      </span>
                    </span>
                  </button>

                  <div className="route-card-actions">
                    {route.status === "proposed" && (
                      <button
                        type="button"
                        className="route-action primary"
                        disabled={transitionsBlocked}
                        onClick={() => onDispatch(route)}
                      >
                        <Check size={14} aria-hidden="true" />
                        {isBusy ? "처리 중" : "승인"}
                      </button>
                    )}
                    {route.status === "dispatched" && (
                      <>
                        <button
                          type="button"
                          className="route-action primary"
                          disabled={transitionsBlocked}
                          onClick={() => onComplete(route)}
                        >
                          <CheckCheck size={14} aria-hidden="true" />
                          완료
                        </button>
                        <button
                          type="button"
                          className="route-action danger"
                          disabled={transitionsBlocked}
                          onClick={() => onCancel(route)}
                        >
                          <CircleX size={14} aria-hidden="true" />
                          취소
                        </button>
                      </>
                    )}
                  </div>
                </article>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
}
