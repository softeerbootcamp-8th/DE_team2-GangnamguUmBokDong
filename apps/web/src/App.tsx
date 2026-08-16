import { useEffect, useState } from "react";
import { api } from "./api";
import type { Alert, DispatchCenter, ForecastResponse, StationSummary } from "./api";
import { AlertList } from "./components/AlertList";
import { DetailPanel } from "./components/DetailPanel";
import { ForecastPanel } from "./components/ForecastPanel";
import { Header } from "./components/Header";
import { StationMap } from "./components/StationMap";
import { StockPanel } from "./components/StockPanel";
import { formatClock } from "./format";

const POLL_INTERVAL_MS = 15_000;
const FORECAST_POLL_INTERVAL_MS = 60_000;
const ALL_REGIONS = "all";

export default function App() {
  const [stations, setStations] = useState<StationSummary[]>([]);
  const [stationsUpdatedAt, setStationsUpdatedAt] = useState<Date | null>(null);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [selectedStationId, setSelectedStationId] = useState<number | null>(null);
  const [forecast, setForecast] = useState<ForecastResponse | null>(null);
  // 지도와 우선순위 리스트가 항상 같은 지역만 보여줘야 해서, 필터 상태를 두 패널의
  // 공통 부모인 여기서 들고 각각에 걸러진 배열을 내려보낸다. 지역센터 관할 경계는
  // 공개 자료가 없어 최근접 근사로 배정한 값이다(apps/api/regions.py 참고).
  const [selectedRegion, setSelectedRegion] = useState<string>(ALL_REGIONS);
  const [regionCenters, setRegionCenters] = useState<DispatchCenter[]>([]);

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

  // 지역센터 좌표는 대여소처럼 자주 바뀌지 않으니(고정 시설) 폴링 없이 한 번만 가져온다.
  useEffect(() => {
    api.regions().then(setRegionCenters);
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

  // 상세 패널(예측/재고/상세)은 지역 필터와 무관하게 이미 선택된 대여소를 계속
  // 보여줘야 하므로, 필터링 안 된 전체 stations에서 찾는다.
  const selectedStation = stations.find((s) => s.sta_id === selectedStationId) ?? null;

  const filteredStations =
    selectedRegion === ALL_REGIONS ? stations : stations.filter((s) => s.region === selectedRegion);
  const filteredAlerts = selectedRegion === ALL_REGIONS ? alerts : alerts.filter((a) => a.region === selectedRegion);

  return (
    <div className="dashboard">
      <Header />
      <div className="dashboard-grid">
        <section className="panel map-panel">
          <div className="panel-header">
            <span className="panel-title-group">
              <h2>대여소 지도</h2>
              <select
                className="region-select"
                value={selectedRegion}
                onChange={(e) => setSelectedRegion(e.target.value)}
                aria-label="지역센터 필터"
                title="지역센터 관할 경계는 근사치입니다(공개 자료 없음, 최근접 배정)"
              >
                <option value={ALL_REGIONS}>전체 지역센터</option>
                {regionCenters.map((c) => (
                  <option key={c.region} value={c.region}>
                    {c.region}
                  </option>
                ))}
              </select>
            </span>
            <span className="panel-meta">현황 기준 시각 {stationsUpdatedAt ? formatClock(stationsUpdatedAt) : "-"}</span>
          </div>
          <div className="panel-body">
            <StationMap
              stations={filteredStations}
              alerts={filteredAlerts}
              selectedStationId={selectedStationId}
              onSelect={setSelectedStationId}
              regionCenters={regionCenters}
              selectedRegion={selectedRegion}
            />
          </div>
        </section>
        <section className="panel alert-panel">
          <h2>작업 우선순위 리스트</h2>
          <div className="panel-body">
            <AlertList alerts={filteredAlerts} selectedStationId={selectedStationId} onSelect={setSelectedStationId} />
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
