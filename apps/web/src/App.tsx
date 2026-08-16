import { useEffect, useState } from "react";
import { api } from "./api";
import type { Alert, ForecastResponse, StationSummary } from "./api";
import { AlertList } from "./components/AlertList";
import { DetailPanel } from "./components/DetailPanel";
import { ForecastPanel } from "./components/ForecastPanel";
import { Header } from "./components/Header";
import { StationMap } from "./components/StationMap";
import type { MapFilterMode } from "./components/StationMap";
import { StockPanel } from "./components/StockPanel";
import { formatClock } from "./format";

const POLL_INTERVAL_MS = 15_000;
const FORECAST_POLL_INTERVAL_MS = 60_000;
const MAP_FILTER_TABS: { key: MapFilterMode; label: string }[] = [
  { key: "supply_only", label: "부족한것만" },
  { key: "all", label: "모두 보기" },
];

export default function App() {
  const [stations, setStations] = useState<StationSummary[]>([]);
  const [stationsUpdatedAt, setStationsUpdatedAt] = useState<Date | null>(null);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [selectedStationId, setSelectedStationId] = useState<number | null>(null);
  const [forecast, setForecast] = useState<ForecastResponse | null>(null);
  // 기본값은 공급필요만(이슈 #63) — 트럭 기사의 실제 작업 순서(어디가 비었나
  // -> 그 주변에서 뭘 가져올까)에 맞춘다. "모두 보기"는 그 전 동작으로 돌아가는
  // 탈출구다.
  const [mapFilterMode, setMapFilterMode] = useState<MapFilterMode>("supply_only");

  useEffect(() => {
    let cancelled = false;
    function refresh() {
      api.stations().then((data) => {
        if (!cancelled) {
          setStations(data);
          setStationsUpdatedAt(new Date());
        }
      });
      api.alerts().then((data) => {
        if (!cancelled) setAlerts(data);
      });
    }
    refresh();
    const timer = setInterval(refresh, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, []);

  useEffect(() => {
    if (selectedStationId === null) {
      setForecast(null);
      return;
    }
    let cancelled = false;
    function refresh() {
      api.forecast(selectedStationId as number).then((data) => {
        if (!cancelled) setForecast(data);
      });
    }
    refresh();
    const timer = setInterval(refresh, FORECAST_POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [selectedStationId]);

  const selectedStation = stations.find((s) => s.sta_id === selectedStationId) ?? null;

  return (
    <div className="dashboard">
      <Header />
      <div className="dashboard-grid">
        <section className="panel map-panel">
          <div className="panel-header">
            <span className="panel-title-group">
              <h2>대여소 지도</h2>
              <div className="alert-tabs" role="tablist" aria-label="지도 표시 범위">
                {MAP_FILTER_TABS.map((t) => (
                  <button
                    key={t.key}
                    type="button"
                    role="tab"
                    aria-selected={mapFilterMode === t.key}
                    className={`alert-tab${mapFilterMode === t.key ? " active" : ""}`}
                    onClick={() => setMapFilterMode(t.key)}
                  >
                    {t.label}
                  </button>
                ))}
              </div>
            </span>
            <span className="panel-meta">현황 기준 시각 {stationsUpdatedAt ? formatClock(stationsUpdatedAt) : "-"}</span>
          </div>
          <div className="panel-body">
            <StationMap
              stations={stations}
              alerts={alerts}
              selectedStationId={selectedStationId}
              onSelect={setSelectedStationId}
              mapFilterMode={mapFilterMode}
            />
          </div>
        </section>
        <section className="panel alert-panel">
          <h2>작업 우선순위 리스트</h2>
          <div className="panel-body">
            <AlertList alerts={alerts} selectedStationId={selectedStationId} onSelect={setSelectedStationId} />
          </div>
        </section>
        <section className="panel forecast-panel">
          <h2>반납/대여 수요 예측 그래프</h2>
          <div className="panel-body">
            <ForecastPanel station={selectedStation} forecast={forecast} />
          </div>
        </section>
        <section className="panel stock-panel">
          <h2>예측 재고 그래프</h2>
          <div className="panel-body">
            <StockPanel station={selectedStation} forecast={forecast} />
          </div>
        </section>
        <section className="panel detail-panel">
          <h2>대여소 상세 데이터</h2>
          <div className="panel-body">
            <DetailPanel stationId={selectedStationId} reasons={forecast?.reasons ?? []} />
          </div>
        </section>
      </div>
    </div>
  );
}
