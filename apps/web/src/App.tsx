import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import type { LayoutChangedMeta, PanelImperativeHandle } from "react-resizable-panels";
import { List, Route as RouteIcon, TriangleAlert } from "lucide-react";
import { api } from "./api";
import type { Alert, DispatchCenter, ForecastResponse, Route, ServingHealthResponse, StationSummary } from "./api";
import { AlertList } from "./components/AlertList";
import { DetailPanel } from "./components/DetailPanel";
import type { FocusedEvent } from "./components/DetailPanel";
import { ForecastPanel } from "./components/ForecastPanel";
import { Header } from "./components/Header";
import { RouteList } from "./components/RouteList";
import { RouteStopRail } from "./components/RouteStopRail";
import { StationMap } from "./components/StationMap";
import { StockPanel } from "./components/StockPanel";
import { candidateReferenceMs, isFreshCandidate, isRebalanceRoute, routeTransitionMessage } from "./routeOperations";
import { updateRoutesWithMotion } from "./routeCardMotion";
import { detailPanelDefaultHeight } from "./panelLayout";
import { ResizableHandle, ResizablePanel, ResizablePanelGroup } from "@/components/ui/resizable";

const POLL_INTERVAL_MS = 15_000;
const FORECAST_POLL_INTERVAL_MS = 60_000;
const ROUTE_POLL_INTERVAL_MS = 30_000;
const STATUS_POLL_INTERVAL_MS = 30_000;
const ROUTE_PAGE_SIZE = 500;
const CLOSED_ROUTE_HISTORY_MINUTES = 60;
const ALL_REGIONS = "all";
type ListMode = "routes" | "stations";
type RouteTransition = "dispatch" | "complete" | "cancel" | "dismiss" | "restore";
type RouteRefreshState = "normal" | "soon" | "delayed";

function routeRefreshState(
  health: ServingHealthResponse | null,
  error: boolean,
): { state: RouteRefreshState; description: string } {
  const routeHealth = health?.components.routes;
  if (error || routeHealth?.state === "missing" || routeHealth?.state === "expired") {
    return { state: "delayed", description: "작업 후보 갱신이 지연되고 있습니다." };
  }
  const ageMinutes = routeHealth?.age_minutes;
  if (ageMinutes !== null && ageMinutes !== undefined && ageMinutes > 5) {
    return { state: "delayed", description: "5분 주기의 작업 후보 갱신이 지연되고 있습니다." };
  }
  if (ageMinutes !== null && ageMinutes !== undefined && ageMinutes >= 4) {
    return { state: "soon", description: "새 작업 후보가 곧 게시될 수 있습니다." };
  }
  return {
    state: "normal",
    description: routeHealth
      ? "작업 후보는 5분 주기로 갱신됩니다."
      : "작업 후보 갱신 상태를 확인하고 있습니다.",
  };
}

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
      closedWithinMinutes: CLOSED_ROUTE_HISTORY_MINUTES,
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
  const [servingHealth, setServingHealth] = useState<ServingHealthResponse | null>(null);
  const [servingHealthError, setServingHealthError] = useState(false);
  const routeMutationGenerationRef = useRef(0);
  const routeViewGenerationRef = useRef(0);
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
  const [selectedRegion, setSelectedRegion] = useState<string>("이수");
  const [regionCenters, setRegionCenters] = useState<DispatchCenter[]>([]);
  const [focusedEvent, setFocusedEvent] = useState<FocusedEvent | null>(null);
  const workspaceGroupElementRef = useRef<HTMLDivElement | null>(null);
  const detailPanelRef = useRef<PanelImperativeHandle | null>(null);
  const didAdjustDetailPanelRef = useRef(false);

  const applyDetailPanelDefault = useCallback(() => {
    if (didAdjustDetailPanelRef.current) return;
    const group = workspaceGroupElementRef.current;
    const panel = detailPanelRef.current;
    if (!group || !panel) return;

    const height = detailPanelDefaultHeight(group.getBoundingClientRect().height);
    if (height > 0) panel.resize(`${height}px`);
  }, []);

  useLayoutEffect(() => {
    applyDetailPanelDefault();
    const group = workspaceGroupElementRef.current;
    if (!group) return;

    const observer = typeof ResizeObserver === "undefined"
      ? null
      : new ResizeObserver(applyDetailPanelDefault);
    observer?.observe(group);
    window.addEventListener("resize", applyDetailPanelDefault);
    return () => {
      observer?.disconnect();
      window.removeEventListener("resize", applyDetailPanelDefault);
    };
  }, [applyDetailPanelDefault]);

  const preserveUserDetailLayout = useCallback(
    (_layout: Record<string, number>, meta: LayoutChangedMeta) => {
      if (meta.isUserInteraction) didAdjustDetailPanelRef.current = true;
    },
    [],
  );

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
          setStationsError(true);
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
        if (listMode !== "routes") {
          selectedRouteIdRef.current = null;
          setSelectedRouteId(null);
          return;
        }
        if (data.some((route) => route.route_id === selectedRouteIdRef.current)) return;

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
          setRoutesError(true);
          setRoutesInitialized(true);
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
    let cancelled = false;
    let requestGeneration = 0;
    function refresh() {
      const currentGeneration = ++requestGeneration;
      api.servingHealth().then((data) => {
        if (!cancelled && currentGeneration === requestGeneration) {
          setServingHealth(data);
          setServingHealthError(false);
        }
      }).catch(() => {
        if (!cancelled && currentGeneration === requestGeneration) {
          setServingHealthError(true);
        }
      });
    }
    refresh();
    const timer = setInterval(refresh, STATUS_POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
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
  const routeSelectionPending = listMode === "routes" && !routesInitialized;
  const selectedStation = stations.find((station) => station.sta_id === selectedStationId) ?? null;
  const filteredStations = selectedRegion === ALL_REGIONS
    ? stations
    : stations.filter((station) => station.region === selectedRegion);
  const filteredAlerts = selectedRegion === ALL_REGIONS
    ? alerts
    : alerts.filter((alert) => alert.region === selectedRegion);
  const canDispatchNewRoutes = !servingHealthError
    && servingHealth?.can_dispatch_new_routes === true;
  const stockHealth = servingHealth?.components.stock;
  const dispatchHealthUnavailable = servingHealthError
    || (servingHealth !== null && !canDispatchNewRoutes);
  const staleAlert = filteredAlerts.find((alert) => alert.data_status === "stale");
  const routeRefresh = routeRefreshState(servingHealth, servingHealthError);
  const listStatusMessage = listMode === "routes"
    ? routesError
      ? "작업 목록 조회에 실패해 마지막 결과를 표시합니다."
      : dispatchHealthUnavailable
        ? "핵심 데이터가 지연되거나 기준 시각이 달라 신규 승인을 잠시 중단합니다."
        : null
    : alertsError
      ? "우선순위 조회에 실패해 마지막 결과를 표시합니다."
      : staleAlert
        ? `긴급도 갱신이 지연되어 ${Math.floor(staleAlert.age_minutes)}분 전 마지막 성공 결과를 표시합니다.`
        : null;

  function changeRegion(region: string) {
    if (region === selectedRegion) return;
    routeViewGenerationRef.current += 1;
    setSelectedRegion(region);
    setRouteTransitionError(null);
    setRoutesInitialized(false);
    selectedRouteIdRef.current = null;
    setSelectedRouteId(null);
  }

  function changeListMode(mode: ListMode) {
    routeViewGenerationRef.current += 1;
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
    const routeViewGeneration = routeViewGenerationRef.current;
    routeMutationGenerationRef.current += 1;
    setBusyRouteId(route.route_id);
    setRouteTransitionError(null);
    try {
      if (transition === "dismiss") {
        const dismissed = await api.dismissRoute(route.route_id);
        routeMutationGenerationRef.current += 1;
        await updateRoutesWithMotion(null, () => setRoutes((current) =>
          current.filter((item) => item.route_id !== dismissed.route_id)));
        if (selectedRouteIdRef.current === dismissed.route_id) {
          selectedRouteIdRef.current = null;
          setSelectedRouteId(null);
        }
        return;
      }
      if (transition === "restore") {
        // 되돌리기는 route ID를 유지한 채 취소된 작업을 다시 진행 중으로 바꾼다.
        const restored = await api.restoreRoute(route.route_id);
        routeMutationGenerationRef.current += 1;
        // 요청 중 목록 모드나 권역이 바뀌었다면 이전 화면의 응답을 적용하지 않는다.
        if (routeViewGeneration !== routeViewGenerationRef.current) return;
        await updateRoutesWithMotion(restored.route_id, () => {
          setRoutes((current) => current.map((item) =>
            item.route_id === restored.route_id ? restored : item));
          selectedRouteIdRef.current = restored.route_id;
          setSelectedRouteId(restored.route_id);
        });
        return;
      }
      const updated = transition === "dispatch"
        ? await api.dispatchRoute(route.route_id)
        : transition === "complete"
          ? await api.completeRoute(route.route_id)
          : await api.cancelRoute(route.route_id);
      routeMutationGenerationRef.current += 1;
      await updateRoutesWithMotion(updated.route_id, () => {
        setRoutes((current) => current.map((item) =>
          item.route_id === updated.route_id ? updated : item));
        selectedRouteIdRef.current = updated.route_id;
        setSelectedRouteId(updated.route_id);
      });
    } catch (error) {
      routeMutationGenerationRef.current += 1;
      setRouteTransitionError(routeTransitionMessage(error));
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
        servingHealth={servingHealth}
        servingHealthError={servingHealthError}
        onRegionChange={changeRegion}
      />
      <div className="flex-1 overflow-hidden">
        <ResizablePanelGroup
          orientation="vertical"
          className="rounded-lg border bg-background"
          elementRef={workspaceGroupElementRef}
          onLayoutChanged={preserveUserDetailLayout}
        >
          <ResizablePanel id="workspace-row" defaultSize="50%">
            <ResizablePanelGroup orientation="horizontal">
              <ResizablePanel id="map-col" defaultSize="50%">
                <div className="flex h-full flex-col bg-background px-4 py-2">
                  <section className="flex h-full min-h-0 min-w-0 flex-col gap-2">
                    <div className="map-panel-toolbar">
                      <span className="map-panel-title">
                        <h2>{selectedRoute || routeSelectionPending ? "작업 경로 지도" : "대여소 지도"}</h2>
                      </span>
                    </div>
                    <div className="relative min-h-0 flex-1 overflow-hidden rounded-md border">
                      <div className="absolute inset-0 z-0">
                        <StationMap
                          stations={routeSelectionPending ? [] : filteredStations}
                          alerts={routeSelectionPending ? [] : filteredAlerts}
                          selectedStationId={selectedStationId}
                          stationFocusRequest={stationFocusRequest}
                          onSelect={selectStation}
                          mapFilterMode="all"
                          regionCenters={regionCenters}
                          selectedRegion={selectedRegion}
                          focusedEvent={focusedEvent}
                          selectedRoute={selectedRoute}
                        />
                        {(stationsError || (stockHealth && stockHealth.state !== "ready")) && (
                          <p className="poll-error" role="status">
                            {stationsError
                              ? "재고 조회에 실패해 마지막 정상 화면을 표시합니다."
                              : `재고 갱신 지연 · ${Math.floor(stockHealth?.age_minutes ?? 0)}분 전`}
                          </p>
                        )}
                      </div>
                    </div>
                  </section>
                </div>
              </ResizablePanel>

              <ResizableHandle withHandle />

              <ResizablePanel id="list-col" defaultSize="50%">
                <div className="flex h-full min-h-0 min-w-0 flex-col bg-card px-4 py-2">
                  <section className="flex h-full min-h-0 min-w-0 flex-col gap-2">
                    <div className="work-list-header">
                      <div className="work-list-title-group">
                        <h2>{listMode === "routes" ? "재배치 작업" : "대여소 우선순위"}</h2>
                        {listStatusMessage && (
                          <p className="work-list-status" role="status" title={listStatusMessage}>
                            <TriangleAlert size={12} aria-hidden="true" />
                            <span>{listStatusMessage}</span>
                          </p>
                        )}
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
                      {listMode === "routes" ? (
                        <div className="data-preserving-panel">
                          <RouteList
                            routes={routes}
                            regions={regionCenters}
                            selectedRouteId={selectedRouteId}
                            busyRouteId={busyRouteId}
                            transitionError={routeTransitionError}
                            canDispatchNewRoutes={canDispatchNewRoutes}
                            candidateRefresh={routeRefresh}
                            onSelect={selectRoute}
                            onDispatch={(route) => void transitionRoute(route, "dispatch")}
                            onComplete={(route) => void transitionRoute(route, "complete")}
                            onCancel={(route) => void transitionRoute(route, "cancel")}
                            onDismiss={(route) => void transitionRoute(route, "dismiss")}
                            onRestore={(route) => void transitionRoute(route, "restore")}
                          />
                        </div>
                      ) : (
                        <div className="data-preserving-panel">
                          <AlertList
                            alerts={filteredAlerts}
                            selectedStationId={selectedStationId}
                            onSelect={selectStation}
                          />
                        </div>
                      )}
                    </div>
                  </section>
                </div>
              </ResizablePanel>
            </ResizablePanelGroup>
          </ResizablePanel>

          <ResizableHandle withHandle />

          <ResizablePanel id="detail-row" defaultSize="50%" panelRef={detailPanelRef}>
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
