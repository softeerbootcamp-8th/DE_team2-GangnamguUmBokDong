const configuredApiBase = import.meta.env.VITE_API_BASE?.trim();
const apiPort = import.meta.env.VITE_API_PORT?.trim() || "8000";
const browserOrigin =
  typeof window === "undefined"
    ? "http://localhost"
    : `${window.location.protocol}//${window.location.hostname}`;
const API_BASE =
  configuredApiBase || `${browserOrigin}:${apiPort}`;

export type ActionType = "supply_needed" | "retrieval_needed" | "normal";
export type StationFilter = "all" | "supply_needed" | "retrieval_needed";

export interface StationSummary {
  sta_id: string;
  sta_nm: string;
  lat: number;
  lon: number;
  hold_cnt: number;
  parking_bike_tot_cnt: number;
  shared_rate: number;
  region: string;
  base_dttm: string;
}

export interface StationDetail extends StationSummary {
  sta_addr: string;
}

export interface ForecastPoint {
  predicted_dttm: string;
  predicted_rent_cnt: number;
  predicted_return_cnt: number;
  predicted_bikes: number;
  action_type: ActionType;
}

export interface ForecastResponse {
  sta_id: string;
  base_dttm: string;
  points: ForecastPoint[];
}

export interface Alert {
  sta_id: string;
  sta_nm: string;
  action_type: ActionType;
  urgency_score: number;
  minutes_until_critical: number;
  region: string;
}

export interface StatusResponse {
  base_dttm: string;
}

export interface DispatchCenter {
  region: string;
  lat: number;
  lon: number;
}

export type RouteStatus = "proposed" | "dispatched" | "completed" | "cancelled";
export type RouteAction = "pickup" | "dropoff";

export interface RouteStop {
  visit_order: number;
  sta_id: string;
  sta_nm: string;
  lat: number;
  lon: number;
  action: RouteAction;
  bike_cnt: number;
}

export interface Route {
  route_id: string;
  region: string;
  status: RouteStatus;
  proposed_at: string;
  dispatched_at: string | null;
  completed_at: string | null;
  cancelled_at: string | null;
  dismissed_at: string | null;
  restored_from_route_id: string | null;
  stops: RouteStop[];
}

export interface RouteQuery {
  region?: string;
  status?: RouteStatus;
  limit?: number;
  offset?: number;
}

export interface CulturalEvent {
  event_id: string;
  title: string;
  place: string | null;
  start_date: string;
  end_date: string;
  lat: number;
  lon: number;
  distance_km: number;
}

export interface EventsResponse {
  radius_km: number;
  events: CulturalEvent[];
}

export interface WeatherPoint {
  forecast_dttm: string;
  temperature: number;
  sky_condition_cd: "clear" | "mostly_cloudy" | "cloudy";
  precipitation_type_cd:
    | "none"
    | "rain"
    | "rain_snow"
    | "snow"
    | "shower"
    | "raindrop"
    | "raindrop_snow_flurry"
    | "snow_flurry";
  precipitation_prob: number | null;
  precipitation_amount: number | null;
  humidity: number | null;
  wind_speed: number | null;
}

export interface WeatherResponse {
  sta_id: string;
  points: WeatherPoint[];
}

export class ApiError extends Error {
  readonly status: number;
  readonly detail: unknown;

  constructor(path: string, status: number, detail: unknown) {
    const detailText = typeof detail === "string" ? detail : detail === undefined ? "" : JSON.stringify(detail);
    super(`${path} 요청 실패: ${status}${detailText ? ` ${detailText}` : ""}`);
    this.name = "ApiError";
    this.status = status;
    this.detail = detail;
  }
}

async function readErrorDetail(response: Response): Promise<unknown> {
  try {
    const body = (await response.json()) as unknown;
    if (typeof body === "object" && body !== null && "detail" in body) {
      return (body as { detail: unknown }).detail;
    }
    return body;
  } catch {
    return undefined;
  }
}

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    throw new ApiError(path, res.status, await readErrorDetail(res));
  }
  return res.json() as Promise<T>;
}

async function postJson<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, { method: "POST" });
  if (!res.ok) {
    throw new ApiError(path, res.status, await readErrorDetail(res));
  }
  return res.json() as Promise<T>;
}

function routeQueryString(query: RouteQuery): string {
  const params = new URLSearchParams();
  if (query.region) params.set("region", query.region);
  if (query.status) params.set("status", query.status);
  params.set("limit", String(query.limit ?? 500));
  params.set("offset", String(query.offset ?? 0));
  return params.toString();
}

export const api = {
  stations: () => getJson<StationSummary[]>("/stations"),
  station: (id: string) => getJson<StationDetail>(`/stations/${id}`),
  forecast: (id: string) => getJson<ForecastResponse>(`/stations/${id}/forecast`),
  events: (id: string) => getJson<EventsResponse>(`/stations/${id}/events`),
  weather: (id: string) => getJson<WeatherResponse>(`/stations/${id}/weather?hours=12`),
  alerts: () => getJson<Alert[]>("/alerts"),
  status: () => getJson<StatusResponse>("/status"),
  regions: () => getJson<DispatchCenter[]>("/regions"),
  routes: (query: RouteQuery = {}) => getJson<Route[]>(`/routes?${routeQueryString(query)}`),
  dispatchRoute: (routeId: string) => postJson<Route>(`/routes/${routeId}/dispatch`),
  completeRoute: (routeId: string) => postJson<Route>(`/routes/${routeId}/complete`),
  cancelRoute: (routeId: string) => postJson<Route>(`/routes/${routeId}/cancel`),
  dismissRoute: (routeId: string) => postJson<Route>(`/routes/${routeId}/dismiss`),
  restoreRoute: (routeId: string) => postJson<Route>(`/routes/${routeId}/restore`),
};
