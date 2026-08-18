const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8000";

export type ActionType = "supply_needed" | "retrieval_needed" | "normal";

export interface StationSummary {
  sta_id: string;
  sta_nm: string;
  gu: string;
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
  reasons: string[];
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
  category: string | null;
  place: string | null;
  start_date: string | null;
  end_date: string | null;
  is_free: string | null;
  distance_km: number;
}

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    throw new Error(`${path} 요청 실패: ${res.status}`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  stations: () => getJson<StationSummary[]>("/stations"),
  station: (id: string) => getJson<StationDetail>(`/stations/${id}`),
  forecast: (id: string) => getJson<ForecastResponse>(`/stations/${id}/forecast`),
  events: (id: string) => getJson<CulturalEvent[]>(`/stations/${id}/events`),
  alerts: () => getJson<Alert[]>("/alerts"),
  status: () => getJson<StatusResponse>("/status"),
  regions: () => getJson<DispatchCenter[]>("/regions"),
};
