import { useCallback, useEffect, useRef, useState } from "react";
import { List, Route as RouteIcon } from "lucide-react";
import { api } from "./api";
import type { Alert, DispatchCenter, ForecastResponse, Route, StationSummary } from "./api";
import { AlertList } from "./components/AlertList";
import { DetailPanel } from "./components/DetailPanel";
import type { FocusedEvent } from "./components/DetailPanel";
import { ForecastPanel } from "./components/ForecastPanel";
import { Header } from "./components/Header";
import { RouteList } from "./components/RouteList";
import { RouteStopRail } from "./components/RouteStopRail";
import { StationMap } from "./components/StationMap";
import { StockPanel } from "./components/StockPanel";
import { candidateReferenceMs, isFreshCandidate, isRebalanceRoute } from "./routeOperations";
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from "@/components/ui/resizable";

const POLL_INTERVAL_MS = 15_000;
const FORECAST_POLL_INTERVAL_MS = 60_000;
const ROUTE_POLL_INTERVAL_MS = 30_000;
const ROUTE_PAGE_SIZE = 500;
const ALL_REGIONS = "all";
type ListMode = "routes" | "stations";
type RouteTransition = "dispatch" | "complete" | "cancel" | "dismiss" | "restore";

function preferredRoute(routes: Route[]): Route | null {
  const referenceMs = candidateReferenceMs(routes);
  return routes.find((route) => route.status === "dispatched")
    ?? routes.find((route) => isFreshCandidate(route, referenceMs))
    ?? routes.find((route) => route.status === "proposed")
    ?? routes[0]
    ?? null;
}

async function fetchAllRoutes(region: string): Promise<Route[]> {
  const routesById = new Map<string, Route>();
  let offset = 0;
  while (true) {
    const page = await api.routes({
      region: region === ALL_REGIONS ? undefined : region,
      limit: ROUTE_PAGE_SIZE,
      offset,
    });
    page.forEach((route) => routesById.set(route.route_id, route));
    if (page.length < ROUTE_PAGE_SIZE) {
      // 신선도 필터는 RouteList에서만 적용한다. 여기서 걸러내면 선택 중인
      // 제안이 후보 창을 벗어나는 순간 재선택이 돌아 지도와 상세가 튄다.
      return [...routesById.values()].filter(isRebalanceRoute);
    }
    offset += page.length;
  }
}

export default function App() {
  const [stations, setStations] = useState<StationSummary[]>([]);
  const [stationsUpdatedAt, setStationsUpdatedAt] = useState<Date | null>(null);
  const [stationsError, setStationsError] = useState(false);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [alertsError, setAlertsError] = useState(false);
  const [routes, setRoutes] = useState<Route[]>([]);
  const [routesError, setRoutesError] = useState(false);
  const [routesInitialized, setRoutesInitialized] = useState(false);
  const routeMutationGenerationRef = useRef(0);
  const [listMode, setListMode] = useState<ListMode>("routes");
  const [selectedRouteId, setSelectedRouteId] = useState<string | null>(null);
  const selectedRouteIdRef = useRef<string | null>(null);
  const [busyRouteId, setBusyRouteId] = useState<string | null>(null);
  const [routeTransitionError, setRouteTransitionError] = useState<string | null>(null);
  const [selectedStationId, setSelectedStationId] = useState<string | null>(null);
  const [stationFocusRequest, setStationFocusRequest] = useState(0);
  const selectedStationIdRef = useRef<string | null>(null);
  const didInitializeSelectionRef = useRef(false);
  const [forecast, setForecast] = useState<ForecastResponse | null>(null);
  const [forecastError, setForecastError] = useState<Error | null>(null);
  const forecastRequestGenerationRef = useRef(0);
  const [selectedRegion, setSelectedRegion] = useState<string>(ALL_REGIONS);
  const [regionCenters, setRegionCenters] = useState<DispatchCenter[]>([]);
  const [focusedEvent, setFocusedEvent] = useState<FocusedEvent | null>(null);

  const selectStation = useCallback((stationId: string) => {
    setStationFocusRequest((current) => current + 1);
    if (selectedStationIdRef.current === stationId) {
      setFocusedEvent(null);
      return;
    }
    didInitializeSelectionRef.current = true;
    selectedStationIdRef.current = stationId;
    forecastRequestGenerationRef.current += 1;
    setForecast(null);
    setForecastError(null);
    setFocusedEvent(null);
    setSelectedStationId(stationId);
  }, []);

  const selectRoute = useCallback(
    (route: Route) => {
      selectedRouteIdRef.current = route.route_id;
      setSelectedRouteId(route.route_id);
      setRouteTransitionError(null);
      const firstStop = [...route.stops].sort((a, b) => a.visit_order - b.visit_order)[0];
      if (firstStop) selectStation(firstStop.sta_id);
    },
    [selectStation],
  );

  useEffect(() => {
    let cancelled = false;
    let stationsGeneration = 0;
    let alertsGeneration = 0;
    function refresh() {
      const currentStationsGeneration = ++stationsGeneration;
      api.stations().then((data) => {
        if (cancelled || currentStationsGeneration !== stationsGeneration) return;
        setStations(data);
        setStationsUpdatedAt(new Date());
        setStationsError(false);
        const selectedId = selectedStationIdRef.current;
        if (selectedId !== null && !data.some((station) => station.sta_id === selectedId)) {
          selectedStationIdRef.current = null;
          forecastRequestGenerationRef.current += 1;
          setSelectedStationId(null);
          setForecast(null);
          setForecastError(null);
          setFocusedEvent(null);
        }
      }).catch(() => {
        if (!cancelled && currentStationsGeneration === stationsGeneration) {
          setStations([]);
          setStationsUpdatedAt(null);
          setStationsError(true);
          selectedStationIdRef.current = null;
          forecastRequestGenerationRef.current += 1;
          setSelectedStationId(null);
          setForecast(null);
          setForecastError(null);
          setFocusedEvent(null);
        }
      });

      const currentAlertsGeneration = ++alertsGeneration;
      api.alerts().then((data) => {
        if (!cancelled && currentAlertsGeneration === alertsGeneration) {
          setAlerts(data);
          setAlertsError(false);
        }
      }).catch(() => {
        if (!cancelled && currentAlertsGeneration === alertsGeneration) {
          setAlerts([]);
          setAlertsError(true);
        }
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
    let cancelled = false;
    let requestGeneration = 0;
    function refresh() {
      const currentGeneration = ++requestGeneration;
      const currentMutationGeneration = routeMutationGenerationRef.current;
      fetchAllRoutes(selectedRegion).then((data) => {
        if (
          cancelled
          || currentGeneration !== requestGeneration
          || currentMutationGeneration !== routeMutationGenerationRef.current
        ) return;
        setRoutes(data);
        setRoutesError(false);
        setRoutesInitialized(true);
        if (data.some((route) => route.route_id === selectedRouteIdRef.current)) return;
        if (listMode !== "routes") {
          selectedRouteIdRef.current = null;
          setSelectedRouteId(null);
          return;
        }

        const nextRoute = preferredRoute(data);
        selectedRouteIdRef.current = nextRoute?.route_id ?? null;
        setSelectedRouteId(nextRoute?.route_id ?? null);
        if (nextRoute?.stops[0]) selectStation(nextRoute.stops[0].sta_id);
      }).catch(() => {
        if (
          !cancelled
          && currentGeneration === requestGeneration
          && currentMutationGeneration === routeMutationGenerationRef.current
        ) {
          setRoutes([]);
          setRoutesError(true);
          setRoutesInitialized(true);
          selectedRouteIdRef.current = null;
          setSelectedRouteId(null);
        }
      });
    }
    refresh();
    const timer = setInterval(refresh, ROUTE_POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [listMode, selectedRegion, selectStation]);

  useEffect(() => {
    api.regions().then(setRegionCenters).catch(() => setRegionCenters([]));
  }, []);

  useEffect(() => {
    if (!routesInitialized || routes.length > 0) return;
    const defaultStation = alerts.find((alert) => stations.some((station) => station.sta_id === alert.sta_id));
    if (selectedStationId === null && defaultStation && !didInitializeSelectionRef.current) {
      selectStation(defaultStation.sta_id);
    }
  }, [alerts, routes.length, routesInitialized, selectStation, selectedStationId, stations]);

  useEffect(() => {
    if (selectedStationId === null) {
      forecastRequestGenerationRef.current += 1;
      setForecast(null);
      setForecastError(null);
      return;
    }
    let cancelled = false;
    const stationId = selectedStationId;
    function refresh() {
      const currentGeneration = ++forecastRequestGenerationRef.current;
      api.forecast(stationId).then((data) => {
        if (!cancelled && currentGeneration === forecastRequestGenerationRef.current) {
          setForecast(data);
          setForecastError(null);
        }
      }).catch((error: Error) => {
        if (!cancelled && currentGeneration === forecastRequestGenerationRef.current) {
          setForecast(null);
          setForecastError(error);
        }
      });
    }
    refresh();
    const timer = setInterval(refresh, FORECAST_POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      forecastRequestGenerationRef.current += 1;
      clearInterval(timer);
    };
  }, [selectedStationId]);

  const selectedRoute = routes.find((route) => route.route_id === selectedRouteId) ?? null;
  const selectedStation = stations.find((station) => station.sta_id === selectedStationId) ?? null;
  const filteredStations = selectedRegion === ALL_REGIONS
    ? stations
    : stations.filter((station) => station.region === selectedRegion);
  const filteredAlerts = selectedRegion === ALL_REGIONS
    ? alerts
    : alerts.filter((alert) => alert.region === selectedRegion);

  function changeRegion(region: string) {
    if (region === selectedRegion) return;
    setSelectedRegion(region);
    setRouteTransitionError(null);
    selectedRouteIdRef.current = null;
    setSelectedRouteId(null);
  }

  function changeListMode(mode: ListMode) {
    setListMode(mode);
    setRouteTransitionError(null);
    if (mode === "stations") {
      selectedRouteIdRef.current = null;
      setSelectedRouteId(null);
      return;
    }
    const nextRoute = preferredRoute(routes);
    if (!selectedRouteIdRef.current && nextRoute) selectRoute(nextRoute);
  }

  async function transitionRoute(route: Route, transition: RouteTransition) {
    if (busyRouteId) return;
    routeMutationGenerationRef.current += 1;
    setBusyRouteId(route.route_id);
    setRouteTransitionError(null);
    try {
      if (transition === "dismiss") {
        const dismissed = await api.dismissRoute(route.route_id);
        routeMutationGenerationRef.current += 1;
        setRoutes((current) => current.filter((item) => item.route_id !== dismissed.route_id));
        if (selectedRouteIdRef.current === dismissed.route_id) {
          selectedRouteIdRef.current = null;
          setSelectedRouteId(null);
        }
        return;
      }
      if (transition === "restore") {
        // 되돌리기는 원본을 그대로 두고 새 후보를 만든다. 새 후보의 proposed_at이
        // 현재 시각이라 후보 창을 통과하고, 바로 선택해 지도에 띄운다.
        const restored = await api.restoreRoute(route.route_id);
        routeMutationGenerationRef.current += 1;
        setRoutes((current) => [...current, restored]);
        selectRoute(restored);
        return;
      }
      const updated = transition === "dispatch"
        ? await api.dispatchRoute(route.route_id)
        : transition === "complete"
          ? await api.completeRoute(route.route_id)
          : await api.cancelRoute(route.route_id);
      routeMutationGenerationRef.current += 1;
      setRoutes((current) => current.map((item) => item.route_id === updated.route_id ? updated : item));
    } catch (error) {
      routeMutationGenerationRef.current += 1;
      setRouteTransitionError(error instanceof Error ? error.message : "작업 상태를 변경하지 못했습니다.");
    } finally {
      setBusyRouteId(null);
    }
  }

  return (
    <div className="flex h-screen flex-col gap-3 bg-background p-3 text-foreground">
      <Header
        regions={regionCenters}
        selectedRegion={selectedRegion}
        stationsUpdatedAt={stationsUpdatedAt}
        onRegionChange={changeRegion}
      />
      <div className="flex-1 overflow-hidden">
        <ResizablePanelGroup orientation="vertical" className="rounded-lg border bg-background">
          <ResizablePanel defaultSize={64} minSize={36}>
            <ResizablePanelGroup orientation="horizontal">
              <ResizablePanel id="map-col" defaultSize={50} minSize={35}>
                <div className="flex h-full flex-col bg-background px-4 py-2">
                  <section className="flex h-full min-h-0 min-w-0 flex-col gap-2">
                    <div className="map-panel-toolbar">
                      <span className="map-panel-title">
                        <h2>{selectedRoute ? "작업 경로 지도" : "대여소 지도"}</h2>
                      </span>
                    </div>
                    <div className="relative min-h-0 flex-1 overflow-hidden rounded-md border">
                      <div className="absolute inset-0 z-0">
                        <StationMap
                          stations={filteredStations}
                          alerts={filteredAlerts}
                          selectedStationId={selectedStationId}
                          stationFocusRequest={stationFocusRequest}
                          onSelect={selectStation}
                          mapFilterMode="all"
                          regionCenters={regionCenters}
                          selectedRegion={selectedRegion}
                          focusedEvent={focusedEvent}
                          selectedRoute={selectedRoute}
                        />
                        {stationsError && <p className="poll-error" role="status">대여소 정보를 갱신하지 못했습니다.</p>}
                      </div>
                    </div>
                  </section>
                </div>
              </ResizablePanel>

              <ResizableHandle withHandle />

              <ResizablePanel id="list-col" defaultSize={50} minSize={35}>
                <div className="flex h-full min-h-0 min-w-0 flex-col bg-card px-4 py-2">
                  <section className="flex h-full min-h-0 min-w-0 flex-col gap-2">
                    <div className="work-list-header">
                      <div className="work-list-title-group">
                        <h2>{listMode === "routes" ? "재배치 작업" : "대여소 우선순위"}</h2>
                      </div>
                      <button
                        type="button"
                        className="list-mode-toggle"
                        onClick={() => changeListMode(listMode === "routes" ? "stations" : "routes")}
                      >
                        {listMode === "routes" ? <List size={15} /> : <RouteIcon size={15} />}
                        {listMode === "routes" ? "대여소" : "작업"}
                      </button>
                    </div>

                    <div className="min-h-0 flex-1 overflow-hidden">
                      {listMode === "routes" ? routesError ? (
                        <p className="empty-state" role="status">작업 목록을 갱신하지 못했습니다.</p>
                      ) : (
                        <RouteList
                          routes={routes}
                          regions={regionCenters}
                          selectedRouteId={selectedRouteId}
                          busyRouteId={busyRouteId}
                          transitionError={routeTransitionError}
                          onSelect={selectRoute}
                          onDispatch={(route) => void transitionRoute(route, "dispatch")}
                          onComplete={(route) => void transitionRoute(route, "complete")}
                          onCancel={(route) => void transitionRoute(route, "cancel")}
                          onDismiss={(route) => void transitionRoute(route, "dismiss")}
                          onRestore={(route) => void transitionRoute(route, "restore")}
                        />
                      ) : alertsError ? (
                        <p className="empty-state" role="status">대여소 우선순위를 갱신하지 못했습니다.</p>
                      ) : (
                        <AlertList
                          alerts={filteredAlerts}
                          selectedStationId={selectedStationId}
                          onSelect={selectStation}
                        />
                      )}
                    </div>
                  </section>
                </div>
              </ResizablePanel>
            </ResizablePanelGroup>
          </ResizablePanel>

          <ResizableHandle withHandle />

          <ResizablePanel defaultSize={36} minSize={24}>
            <div className="flex h-full min-h-0 flex-col bg-card">
              <RouteStopRail
                route={selectedRoute}
                selectedStationId={selectedStationId}
                onSelectStation={selectStation}
              />
              <div className="grid min-h-0 flex-1 grid-cols-3 divide-x">
                <div className="flex h-full min-h-0 min-w-0 flex-col overflow-hidden bg-card px-4 py-2">
                  <section className="flex h-full min-h-0 min-w-0 flex-col gap-2">
                    <h2 className="text-base font-semibold tracking-tight">대여·반납 예측</h2>
                    <div className="min-h-0 min-w-0 flex-1"><ForecastPanel station={selectedStation} forecast={forecast} error={forecastError} /></div>
                  </section>
                </div>
                <div className="flex h-full min-h-0 min-w-0 flex-col overflow-hidden bg-card px-4 py-2">
                  <section className="flex h-full min-h-0 min-w-0 flex-col gap-2">
                    <h2 className="text-base font-semibold tracking-tight">재고 예측</h2>
                    <div className="min-h-0 min-w-0 flex-1"><StockPanel station={selectedStation} forecast={forecast} error={forecastError} /></div>
                  </section>
                </div>
                <div className="flex h-full min-h-0 min-w-0 flex-col overflow-hidden bg-card px-4 py-2">
                  <section className="flex h-full min-h-0 min-w-0 flex-col gap-2">
                    <h2 className="text-base font-semibold tracking-tight">대여소 상세</h2>
                    <div className="min-h-0 min-w-0 flex-1">
                      <DetailPanel
                        key={selectedStationId ?? "no-station"}
                        stationId={selectedStationId}
                        stationPoint={selectedStation ? { lat: selectedStation.lat, lon: selectedStation.lon } : null}
                        onFocusEvent={setFocusedEvent}
                      />
                    </div>
                  </section>
                </div>
              </div>
            </div>
          </ResizablePanel>
        </ResizablePanelGroup>
      </div>
    </div>
  );
}
