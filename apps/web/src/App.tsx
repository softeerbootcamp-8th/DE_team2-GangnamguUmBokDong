import { useEffect, useState } from "react";
import { api } from "./api";
import type { Alert, DispatchCenter, ForecastResponse, StationSummary } from "./api";
import { AlertList } from "./components/AlertList";
import { DetailPanel } from "./components/DetailPanel";
import { ForecastPanel } from "./components/ForecastPanel";
import { Header } from "./components/Header";
import { StationMap } from "./components/StationMap";
import type { MapFilterMode } from "./components/StationMap";
import { StockPanel } from "./components/StockPanel";
import { formatClock } from "./format";
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from "@/components/ui/resizable";


const POLL_INTERVAL_MS = 15_000;
const FORECAST_POLL_INTERVAL_MS = 60_000;
const MAP_FILTER_TABS: { key: MapFilterMode; label: string }[] = [
  { key: "supply_only", label: "부족한것만" },
  { key: "all", label: "모두 보기" },
];
const ALL_REGIONS = "all";

export default function App() {
  const [stations, setStations] = useState<StationSummary[]>([]);
  const [stationsUpdatedAt, setStationsUpdatedAt] = useState<Date | null>(null);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [selectedStationId, setSelectedStationId] = useState<string | null>(null);
  const [forecast, setForecast] = useState<ForecastResponse | null>(null);
  // 기본값은 공급필요만(이슈 #63) — 트럭 기사의 실제 작업 순서(어디가 비었나
  // -> 그 주변에서 뭘 가져올까)에 맞춘다. "모두 보기"는 그 전 동작으로 돌아가는
  // 탈출구다.
  const [mapFilterMode, setMapFilterMode] = useState<MapFilterMode>("supply_only");
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
      api.forecast(selectedStationId as string).then((data) => {
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
    <div className="flex h-screen flex-col bg-background text-foreground p-3 gap-3">
      <Header />
      <div className="flex-1 overflow-hidden">
        <ResizablePanelGroup orientation="vertical" className="rounded-lg border">
          {/* Top Row: Map and Alert List */}
          <ResizablePanel defaultSize={67} minSize={30}>
            <ResizablePanelGroup orientation="horizontal">
              {/* Map */}
              <ResizablePanel defaultSize={66.666} minSize={30}>
                <div className="flex h-full flex-col p-4 bg-background">
                  <section className="flex flex-col h-full gap-4 min-w-0 min-h-0">
                    <div className="flex items-center justify-between">
                      <span className="flex items-center gap-2">
                        <h2 className="text-lg font-semibold tracking-tight">대여소 지도</h2>
                        <select
                          className="region-select rounded border px-2 py-1 text-sm bg-background"
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
                      <span className="text-sm text-muted-foreground">현황 기준 시각 {stationsUpdatedAt ? formatClock(stationsUpdatedAt) : "-"}</span>
                    </div>
                    <div className="flex-1 min-h-0 rounded-md border overflow-hidden relative">
                      <div className="absolute inset-0 z-0">
                        <StationMap
                          stations={filteredStations}
                          alerts={filteredAlerts}
                          selectedStationId={selectedStationId}
                          onSelect={setSelectedStationId}
                          regionCenters={regionCenters}
                          selectedRegion={selectedRegion}
                        />
                      </div>
                    </div>
                  </section>
                </div>
              </ResizablePanel>
              <ResizableHandle withHandle />
              {/* Alert List */}
              <ResizablePanel defaultSize={33.334} minSize={20}>
                <div className="flex h-full flex-col overflow-auto bg-card p-4 min-w-0 min-h-0">
                  <section className="flex flex-col h-full gap-4 min-w-0 min-h-0">
                    <h2 className="text-lg font-semibold tracking-tight">작업 우선순위</h2>
                    <div className="flex-1 overflow-y-auto">
                      <AlertList alerts={filteredAlerts} selectedStationId={selectedStationId} onSelect={setSelectedStationId} />
                    </div>
                  </section>
                </div>
              </ResizablePanel>
            </ResizablePanelGroup>
          </ResizablePanel>

          <ResizableHandle withHandle />

          {/* Bottom Row: Forecast, Stock, Details */}
          <ResizablePanel defaultSize={33} minSize={20}>
            <div className="grid h-full grid-cols-3 divide-x">
              <div className="flex h-full flex-col overflow-auto bg-card p-4 min-w-0 min-h-0">
                <section className="flex flex-col h-full gap-4 min-w-0 min-h-0">
                  <h2 className="text-lg font-semibold tracking-tight">반납 · 수요 예측 그래프</h2>
                  <div className="flex-1 min-w-0 min-h-0">
                    <ForecastPanel station={selectedStation} forecast={forecast} />
                  </div>
                </section>
              </div>
              <div className="flex h-full flex-col overflow-auto bg-card p-4 min-w-0 min-h-0">
                <section className="flex flex-col h-full gap-4 min-w-0 min-h-0">
                  <h2 className="text-lg font-semibold tracking-tight">재고 예측 그래프</h2>
                  <div className="flex-1 min-w-0 min-h-0">
                    <StockPanel station={selectedStation} forecast={forecast} />
                  </div>
                </section>
              </div>
              <div className="flex h-full flex-col overflow-auto bg-card p-4 min-w-0 min-h-0">
                <section className="flex flex-col h-full gap-4 min-w-0 min-h-0">
                  <h2 className="text-lg font-semibold tracking-tight">대여소 상세</h2>
                  <div className="flex-1 min-w-0 min-h-0">
                    <DetailPanel stationId={selectedStationId} reasons={forecast?.reasons ?? []} />
                  </div>
                </section>
              </div>
            </div>
          </ResizablePanel>
        </ResizablePanelGroup>
      </div>
    </div>
  );
}
