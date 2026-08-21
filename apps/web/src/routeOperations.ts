import type { Alert, DispatchCenter, Route } from "./api";

const EARTH_RADIUS_KM = 6371;
const ROAD_DISTANCE_FACTOR = 1.25;
const URBAN_TRUCK_SPEED_KMH = 18;
const STOP_SERVICE_MINUTES = 4;
const BIKE_HANDLING_MINUTES = 0.5;

interface Point {
  lat: number;
  lon: number;
}

export interface RouteEstimate {
  distanceKm: number;
  durationMinutes: number;
}

function haversineKm(left: Point, right: Point): number {
  const toRadians = (degrees: number) => (degrees * Math.PI) / 180;
  const latitudeDelta = toRadians(right.lat - left.lat);
  const longitudeDelta = toRadians(right.lon - left.lon);
  const latitudeSin = Math.sin(latitudeDelta / 2);
  const longitudeSin = Math.sin(longitudeDelta / 2);
  const h = latitudeSin * latitudeSin
    + Math.cos(toRadians(left.lat))
      * Math.cos(toRadians(right.lat))
      * longitudeSin
      * longitudeSin;
  return 2 * EARTH_RADIUS_KM * Math.asin(Math.sqrt(h));
}

export function estimateRoute(route: Route, centers: DispatchCenter[]): RouteEstimate | null {
  const center = centers.find((item) => item.region === route.region);
  if (!center || route.stops.length === 0) return null;

  const orderedStops = [...route.stops].sort((left, right) => left.visit_order - right.visit_order);
  const path: Point[] = [center, ...orderedStops, center];
  const directDistanceKm = path.slice(1).reduce(
    (total, point, index) => total + haversineKm(path[index], point),
    0,
  );
  const distanceKm = directDistanceKm * ROAD_DISTANCE_FACTOR;
  const handledBikes = orderedStops.reduce((total, stop) => total + stop.bike_cnt, 0);
  const rawDurationMinutes = (distanceKm / URBAN_TRUCK_SPEED_KMH) * 60
    + orderedStops.length * STOP_SERVICE_MINUTES
    + handledBikes * BIKE_HANDLING_MINUTES;

  return {
    distanceKm: Math.round(distanceKm * 10) / 10,
    durationMinutes: Math.max(5, Math.ceil(rawDurationMinutes / 5) * 5),
  };
}

export function alertScoreMap(alerts: Alert[]): Map<string, number> {
  return new Map(alerts.map((alert) => [alert.sta_id, alert.urgency_score]));
}

export function routePriority(route: Route, scoresByStation: ReadonlyMap<string, number>): number {
  return route.stops.reduce(
    (highest, stop) => Math.max(highest, scoresByStation.get(stop.sta_id) ?? 0),
    0,
  );
}

export function formatRouteDuration(minutes: number): string {
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  if (hours === 0) return `${remainingMinutes}분`;
  if (remainingMinutes === 0) return `${hours}시간`;
  return `${hours}시간 ${remainingMinutes}분`;
}

export function isRebalanceRoute(route: Route): boolean {
  const pickupQuantity = route.stops
    .filter((stop) => stop.action === "pickup")
    .reduce((total, stop) => total + stop.bike_cnt, 0);
  const dropoffQuantity = route.stops
    .filter((stop) => stop.action === "dropoff")
    .reduce((total, stop) => total + stop.bike_cnt, 0);
  return pickupQuantity > 0 && pickupQuantity === dropoffQuantity;
}

export function routeKind(route: Route): "재배치" | "센터 회수" | "센터 공급" {
  const hasPickup = route.stops.some((stop) => stop.action === "pickup");
  if (isRebalanceRoute(route)) return "재배치";
  return hasPickup ? "센터 회수" : "센터 공급";
}

export const ROUTE_ESTIMATE_BASIS = "직선거리×1.25 · 도심 18km/h · 정차 4분 · 1대당 30초";
