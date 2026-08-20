const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8000";

export type ActionType = "supply_needed" | "retrieval_needed" | "normal";

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

export const api = {
  stations: () => getJson<StationSummary[]>("/stations"),
  station: (id: string) => getJson<StationDetail>(`/stations/${id}`),
  forecast: (id: string) => getJson<ForecastResponse>(`/stations/${id}/forecast`),
  events: (id: string) => getJson<EventsResponse>(`/stations/${id}/events`),
  weather: (id: string) => getJson<WeatherResponse>(`/stations/${id}/weather?hours=12`),
  alerts: () => getJson<Alert[]>("/alerts"),
  status: () => getJson<StatusResponse>("/status"),
  regions: () => getJson<DispatchCenter[]>("/regions"),
};
