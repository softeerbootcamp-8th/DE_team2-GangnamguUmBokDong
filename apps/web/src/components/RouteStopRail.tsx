import { MapPin, PackageMinus, PackagePlus, Route as RouteIcon, Warehouse } from "lucide-react";
import type { Route, RouteStop } from "../api";

interface Props {
  route: Route | null;
  selectedStationId: string | null;
  onSelectStation: (stationId: string) => void;
}

function stopLabel(stop: RouteStop): string {
  return stop.action === "pickup" ? `회수 ${stop.bike_cnt}대` : `공급 ${stop.bike_cnt}대`;
}

export function RouteStopRail({ route, selectedStationId, onSelectStation }: Props) {
  if (!route) {
    return (
      <div className="route-stop-rail empty">
        <RouteIcon size={17} aria-hidden="true" />
        작업을 선택하면 방문 순서가 여기에 표시됩니다.
      </div>
    );
  }

  const orderedStops = [...route.stops].sort((left, right) => left.visit_order - right.visit_order);

  return (
    <div className="route-stop-rail">
      <ol className="route-stop-sequence" aria-label={`${route.region} 작업 방문 순서`}>
        <li>
          <div className="route-center-chip">
            <Warehouse size={15} aria-hidden="true" />
            <span>{route.region} 센터</span>
            <small>출발</small>
          </div>
        </li>
        {orderedStops.map((stop) => {
          const isSelected = stop.sta_id === selectedStationId;
          const ActionIcon = stop.action === "pickup" ? PackageMinus : PackagePlus;
          return (
            <li key={`${stop.visit_order}-${stop.sta_id}`}>
              <button
                type="button"
                className={`route-stop-chip ${stop.action}${isSelected ? " selected" : ""}`}
                onClick={() => onSelectStation(stop.sta_id)}
              >
                <span className="route-stop-order">{stop.visit_order}</span>
                <span className="route-stop-name">
                  <span>
                    <MapPin size={12} aria-hidden="true" />
                    {stop.sta_nm}
                  </span>
                  <small>
                    <ActionIcon size={12} aria-hidden="true" />
                    {stopLabel(stop)}
                  </small>
                </span>
              </button>
            </li>
          );
        })}
        <li>
          <div className="route-center-chip">
            <Warehouse size={15} aria-hidden="true" />
            <span>{route.region} 센터</span>
            <small>복귀 · 작업 완료</small>
          </div>
        </li>
      </ol>
    </div>
  );
}
