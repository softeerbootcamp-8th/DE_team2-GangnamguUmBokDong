import { featureCollection, polygon } from "@turf/helpers";
import { intersect } from "@turf/intersect";
import "leaflet/dist/leaflet.css";
import { Delaunay } from "d3-delaunay";
import L from "leaflet";
import { useEffect, useMemo, useRef, useState } from "react";
import { CircleMarker, MapContainer, Marker, Polygon, TileLayer, Tooltip, useMapEvents } from "react-leaflet";
import type { ActionType, Alert, DispatchCenter, StationSummary } from "../api";
import seoulBoundary from "../seoul_boundary.json";

const ALL_REGIONS = "all"; // App.tsx의 선택 안 함 상태와 동일한 값이어야 한다.
const SEOUL_POLYGON = polygon(seoulBoundary.geometry.coordinates);
// 서울시 따릉이 서비스라 서울 윤곽선을 항상 기본으로 띄워둔다(권역 선택과 무관).
const SEOUL_OUTLINE: [number, number][] = seoulBoundary.geometry.coordinates[0].map(
  ([lon, lat]) => [lat, lon] as [number, number],
);

const GANGNAM_CENTER: [number, number] = [37.5172, 127.0473];
const DEFAULT_ZOOM = 13;
const BASE_ZOOM = DEFAULT_ZOOM; // 마커 크기의 기준 줌(이 레벨에서 MIN~MAX_RADIUS 그대로 나옴)
const COUNT_LABEL_MIN_ZOOM = 14; // 이 줌 레벨부터 재고 수를 라벨로 띄운다
const MIN_RADIUS = 9;
const MAX_RADIUS = 20;
const CLICK_PADDING = 10; // 시각적 마커보다 이만큼 더 넓게 클릭을 받는다
const SELECTED_BORDER = "#0b0b0b";
const IDLE_BORDER = "#fcfcfb";
const MAX_VISIBLE_MARKERS = 100; // 현재 화면 범위 안에 이보다 많으면 덜 급한 것부터 숨긴다

// 서비스 대상이 서울 전역이라, 그 밖으로 패닝/줌아웃해서 벗어날 이유가 없다.
// 실제 대여소 좌표 범위(위도 37.43~37.69, 경도 126.80~127.18)보다 여유를 두고
// 잡아서, 서울 경계 안쪽 대여소가 없는 지역(외곽 일부)도 화면에 들어오게 한다.
const SEOUL_SW: [number, number] = [37.39, 126.72];
const SEOUL_NE: [number, number] = [37.73, 127.21];
const SEOUL_BOUNDS: L.LatLngBoundsExpression = [SEOUL_SW, SEOUL_NE];
const SEOUL_MIN_ZOOM = 10; // 이보다 축소하면 서울 전체가 한 화면보다 작아져서 의미가 없다
const REGION_FILL = "#2a78d6";

// 지역센터 관할의 실제 경계 데이터는 없다(apps/api/regions.py 참고). 그래서
// "권역 면적"은 우리 배정 로직(최근접 지역센터)이 암묵적으로 정의하는 경계,
// 즉 보로노이 다이어그램으로 그린다 — 대여소 배정에 실제로 쓰인 것과 정확히
// 같은 기준이라 최소한 우리 시스템 안에서는 정직한 시각화다. 서울 밖으로
// 무한히 뻗어나가는 바깥쪽 셀들은 SEOUL_BOUNDS로 잘라낸다.
// turf 교집합 결과(Polygon 또는 MultiPolygon)의 바깥 링들을 Leaflet
// [위도, 경도] 좌표 배열로 바꾼다. 구멍(hole)은 우리 케이스에서 나올 일이
// 없어 각 폴리곤의 첫 링(바깥 링)만 쓴다.
function outerRingsToLatLng(geometry: { type: string; coordinates: number[][][] | number[][][][] }): [
  number,
  number,
][][] {
  const polygons = geometry.type === "Polygon" ? [geometry.coordinates as number[][][]] : (geometry.coordinates as number[][][][]);
  return polygons.map((rings) => rings[0].map(([lon, lat]) => [lat, lon] as [number, number]));
}

function computeRegionCell(centers: DispatchCenter[], selectedRegion: string): [number, number][][] | null {
  if (selectedRegion === ALL_REGIONS || centers.length === 0) return null;
  const index = centers.findIndex((c) => c.region === selectedRegion);
  if (index === -1) return null;

  // d3-delaunay는 평면 좌표를 쓰므로 (경도, 위도)를 (x, y)로 그대로 쓴다.
  // 서울 정도의 좁은 범위에서는 구면 보정 없이도 배정 결과와 시각적으로
  // 어긋나지 않는다(11장소 사이 실제 최근접 배정도 이 정밀도로 충분했다).
  const points: [number, number][] = centers.map((c) => [c.lon, c.lat]);
  const delaunay = Delaunay.from(points);
  const bounds = L.latLngBounds(SEOUL_SW, SEOUL_NE);
  const voronoi = delaunay.voronoi([bounds.getWest(), bounds.getSouth(), bounds.getEast(), bounds.getNorth()]);
  const rectClippedCell = voronoi.cellPolygon(index);
  if (!rectClippedCell) return null;

  // 사각형(SEOUL_BOUNDS)으로만 자른 셀은 서울시 바깥 땅도 포함한다. 서울시 실제
  // 경계(seoul_boundary.json, kostat 2013 단순화 데이터)와 교집합해서, 최소한
  // 바깥쪽 테두리는 실제 서울시 모양과 일치하게 만든다 — 지역센터 사이 안쪽
  // 경계선 자체는 여전히 최근접 근사다.
  const cellPolygon = polygon([rectClippedCell as unknown as number[][]]);
  const clipped = intersect(featureCollection([cellPolygon, SEOUL_POLYGON]));
  if (!clipped) return null;
  return outerRingsToLatLng(clipped.geometry);
}

export type MapFilterMode = "all" | "supply_only";

// 트럭 기사는 "어디가 비었나 -> 그 주변에서 뭘 가져올까" 순서로 판단하므로,
// 선택된 공급필요 대여소로부터 이 반경(직선거리) 안에 있는 회수필요 대여소만
// 후보로 드러낸다. 너무 멀리서 끌어오면 실제 동선과 안 맞고, 너무 많이
// 드러내면 오히려 "주변"이라는 의미가 흐려진다(이슈 #63).
const NEARBY_RADIUS_KM = 1;
const MAX_NEARBY_RETRIEVAL = 5;
const EARTH_RADIUS_KM = 6371;

function haversineKm(a: { lat: number; lon: number }, b: { lat: number; lon: number }): number {
  const toRad = (deg: number) => (deg * Math.PI) / 180;
  const dLat = toRad(b.lat - a.lat);
  const dLon = toRad(b.lon - a.lon);
  const sinLat = Math.sin(dLat / 2);
  const sinLon = Math.sin(dLon / 2);
  const h = sinLat * sinLat + Math.cos(toRad(a.lat)) * Math.cos(toRad(b.lat)) * sinLon * sinLon;
  return 2 * EARTH_RADIUS_KM * Math.asin(Math.sqrt(h));
}

// "부족한것만" 모드에서는 공급필요 대여소만 기본으로 보여주고, 그중 하나를
// 선택했을 때만 그 주변 회수필요 후보를 몇 개 더 드러낸다. "전체" 모드는
// 필터링 없이 기존 동작(모든 대여소 표시) 그대로 둔다.
function applyMapFilter(
  stations: StationSummary[],
  alertsByStation: Map<number, Alert>,
  mode: MapFilterMode,
  selectedStationId: number | null,
): StationSummary[] {
  if (mode === "all") return stations;

  const supplyStations = stations.filter((s) => alertsByStation.get(s.sta_id)?.action_type === "supply_needed");
  const visible = new Map(supplyStations.map((s) => [s.sta_id, s]));

  const selectedStation = stations.find((s) => s.sta_id === selectedStationId);
  const selectedIsSupply = selectedStation && alertsByStation.get(selectedStation.sta_id)?.action_type === "supply_needed";
  if (selectedStation && selectedIsSupply) {
    const retrievalStations = stations.filter((s) => alertsByStation.get(s.sta_id)?.action_type === "retrieval_needed");
    const nearby = retrievalStations
      .map((station) => ({ station, distanceKm: haversineKm(selectedStation, station) }))
      .filter((x) => x.distanceKm <= NEARBY_RADIUS_KM)
      .sort((a, b) => a.distanceKm - b.distanceKm)
      .slice(0, MAX_NEARBY_RETRIEVAL);
    for (const { station } of nearby) visible.set(station.sta_id, station);
  }

  // 선택된 대여소가 공급필요가 아니어도(예: 방금 드러난 회수필요 후보를 눌러
  // 상세를 보는 중) 지도에서 갑자기 사라지면 혼란스러우니 항상 보이게 둔다.
  if (selectedStation) visible.set(selectedStation.sta_id, selectedStation);

  return [...visible.values()];
}

// 지도를 확대하면 도로·건물이 커지는데 마커만 화면 픽셀 크기 그대로 고정돼
// 있으면 상대적으로 쪼그라들어 보여서 부자연스럽다. BASE_ZOOM보다 4레벨
// 올라갈 때마다 2배, 내려갈 때마다 절반이 되도록 줌에 비례해 키우고 줄인다.
function zoomScale(zoom: number): number {
  const scale = 2 ** ((zoom - BASE_ZOOM) / 4);
  return Math.min(2.5, Math.max(0.5, scale));
}

// Leaflet은 SVG 속성으로 색을 직접 넣어서 CSS 커스텀 프로퍼티 대신 팔레트 hex를 그대로 쓴다.
const DIVERGING_RED = "#e34948";
const DIVERGING_BLUE = "#2a78d6";
const DIVERGING_NEUTRAL = "#898781";

function mixHex(from: string, to: string, t: number): string {
  const a = parseInt(from.slice(1), 16);
  const b = parseInt(to.slice(1), 16);
  const mix = (shift: number) => {
    const av = (a >> shift) & 255;
    const bv = (b >> shift) & 255;
    return Math.round(av + (bv - av) * t);
  };
  const [r, g, bl] = [mix(16), mix(8), mix(0)];
  return `#${[r, g, bl].map((v) => v.toString(16).padStart(2, "0")).join("")}`;
}

// 채우기 색은 방향(공급 필요/회수 필요) 하나만 나타낸다. 긴급도까지 새로운
// 색상으로 같이 표현하면 두 색을 조합해서 읽어야 해서 한눈에 안 들어오고,
// 특히 "지금 텅 비었지만 곧 반납이 몰려 안 급한" 자가치유 케이스가 짙은
// 색으로 과장돼 보이게 된다. 그래서 마커 크기(markerRadius)로 긴급도를 따로
// 표현하고, 색은 그 방향의 짙기만 urgency_score(0~100)에 비례해서 바꾼다:
// 점수가 낮으면 회색에 가깝고, 점수가 오를수록 연속적으로 짙어져서 100점이면
// 순수한 빨강/파랑이 된다. 점수 0(정상)은 항상 순수 회색이다.
//
// 방향은 현재 재고 비율이 아니라 alert.action_type을 그대로 쓴다. 현재
// 비율로 따로 계산하면, 아직 재고는 정상 범위인데 추세·예측으로만 위험이
// 감지된 대여소(예: 신논현역, 2시간 뒤 위험)는 색이 회색(정상)으로 나오면서
// 크기만 커져서 "큰데 방향을 알 수 없는" 마커가 된다. urgency_score를 만든
// 바로 그 action_type을 색으로 쓰면 색·크기가 항상 같은 판단 결과를 가리킨다.
function markerColor(alert: Alert | undefined): string {
  const actionType: ActionType = alert?.action_type ?? "normal";
  if (actionType === "normal") return DIVERGING_NEUTRAL;
  const target = actionType === "supply_needed" ? DIVERGING_RED : DIVERGING_BLUE;
  // 두 방향의 실측 점수 분포가 원래부터 비대칭이다(공급필요는 대부분 저점,
  // 회수필요는 고르게 분포) — 곡선을 씌우면 이미 흐린 공급필요가 더 안 보이게
  // 될 뿐이라, 절대 점수를 그대로 선형으로 매핑한다.
  const t = Math.min(1, Math.max(0, (alert?.urgency_score ?? 0) / 100));
  return mixHex(DIVERGING_NEUTRAL, target, t);
}

function markerRadius(alert: Alert | undefined): number {
  const score = alert?.urgency_score ?? 0;
  return MIN_RADIUS + (score / 100) * (MAX_RADIUS - MIN_RADIUS);
}

// 재고 수는 마커 밖에 별도 라벨로 띄우지 않고 마커 안에 바로 적는다. 마커가
// 회색인데 라벨만 빨강/파랑으로 따로 떠 있으면 둘이 서로 다른 걸 가리키는
// 것처럼 헷갈리므로, 그냥 마커 채우기색 위에 흰 숫자만 얹는다. 채우기색이
// 옅은 회색일 때도 읽히도록 짙은 그림자를 살짝 깔아 대비를 보정한다. 글자
// 크기는 마커 반지름에 비례해서, 마커가 커지고 작아질 때 같이 커지고 작아진다.
function countIcon(count: number, markerRadiusPx: number) {
  const fontSize = Math.max(9, Math.round(markerRadiusPx * 0.8));
  const boxSize = Math.round(markerRadiusPx * 2);
  // line-height로 세로 중앙을 맞추면 폰트마다 글자 위아래 여백(ascent/descent)이
  // 달라서 결국 몇 px씩 어긋난다 — 원이 작을수록 그 몇 px이 원 크기 대비 크게
  // 보였다. 대신 마커 지름만큼 박스를 잡고 flex로 정가운데 맞추면 폰트 메트릭과
  // 무관하게 항상 박스 중심(=마커 중심)에 글자가 온다.
  return L.divIcon({
    className: "marker-count-label",
    html: `<div style="width:${boxSize}px;height:${boxSize}px;transform:translate(-50%, -50%);font-size:${fontSize}px;">${count}</div>`,
    iconSize: [0, 0],
  });
}

// 서울 전역 대여소를 다 그리면 원이 겹쳐서 안 읽힌다. urgency_score가 높은
// (우선순위가 급한) 대여소일수록 먼저 살아남게 순위를 매겨둔다. 정렬은
// stations/alerts가 바뀔 때만 다시 하고, 화면이 바뀔 때는 이미 정렬된 배열을
// 거르기만 한다.
function rankByUrgency(stations: StationSummary[], alertsByStation: Map<string, Alert>): StationSummary[] {
  return [...stations].sort((a, b) => {
    const scoreA = alertsByStation.get(a.sta_id)?.urgency_score ?? -1;
    const scoreB = alertsByStation.get(b.sta_id)?.urgency_score ?? -1;
    if (scoreB !== scoreA) return scoreB - scoreA;
    return a.sta_id.localeCompare(b.sta_id);
  });
}

// 전역 순위만으로 자르면, 특정 동네를 확대해도 그 동네 대여소들이 전역 순위에
// 못 들면 화면에 아무것도 안 보이는 문제가 생긴다. 그래서 먼저 "지금 화면에
// 보이는 범위(bounds) 안"으로만 좁히고, 그 안에서도 너무 많으면(MAX_VISIBLE_MARKERS)
// 그때서야 순위로 자른다 — 어떤 동네를 봐도 그 지역 대여소는 항상 보이고, 화면
// 안이 정말 붐빌 때만(줌아웃해서 서울 전체가 보일 때 등) 덜 급한 것부터 숨는다.
function visibleStations(
  ranked: StationSummary[],
  bounds: L.LatLngBounds | null,
  keepStationId: string | null,
): StationSummary[] {
  const inView = bounds ? ranked.filter((s) => bounds.contains([s.lat, s.lon])) : ranked;
  if (inView.length <= MAX_VISIBLE_MARKERS) return inView;

  const top = inView.slice(0, MAX_VISIBLE_MARKERS);
  // 선택된 대여소는 순위권 밖으로 밀려나도 화면에서 사라지면 안 된다(리스트에서
  // 골랐는데 지도에서 안 보이면 혼란스럽다). 없으면 마지막 자리를 양보해서 넣는다.
  if (keepStationId !== null && !top.some((s) => s.sta_id === keepStationId)) {
    const kept = inView.find((s) => s.sta_id === keepStationId);
    if (kept) {
      top[top.length - 1] = kept;
    }
  }
  return top;
}

// 원(SVG)과 숫자 라벨(divIcon)은 Leaflet에서 서로 다른 레이어(pane)에 그려진다.
// 라벨 pane이 원 pane보다 항상 위에 있는 건 상관없는데, 그 안에서의 순서 기준이
// 서로 다르면(원은 그린 순서, 라벨은 Leaflet 기본값인 화면 y좌표) 어떤 대여소는
// 원이 다른 원에 덮였는데 숫자만 둥둥 떠 있는 것처럼 보인다. 그래서 원·라벨 둘 다
// 이 우선순위(선택된 대여소가 최우선, 그다음 urgency_score)로 쌓이게 맞춘다.
function zPriority(alert: Alert | undefined, isSelected: boolean): number {
  if (isSelected) return Infinity;
  return alert?.urgency_score ?? -1;
}

interface Props {
  stations: StationSummary[];
  alerts: Alert[];
  selectedStationId: string | null;
  onSelect: (stationId: string) => void;
  regionCenters: DispatchCenter[];
  selectedRegion: string;
}

function StationMarkers({
  stations,
  alerts,
  selectedStationId,
  onSelect,
  mapFilterMode,
  regionCenters,
  selectedRegion,
}: Props) {
  const [zoom, setZoom] = useState(DEFAULT_ZOOM);
  const [bounds, setBounds] = useState<L.LatLngBounds | null>(null);
  // zoomend가 아니라 zoom을 듣는다. zoom은 줌 애니메이션이 진행되는 동안
  // 프레임마다(줌 버튼처럼 순간 전환일 때도 포함) 계속 발생해서, 마커 크기가
  // 애니메이션 도중에도 실시간으로 따라 커지고 작아진다. zoomend만 들으면
  // 애니메이션이 다 끝난 뒤에야 한 번에 크기가 바뀌어 뚝뚝 끊겨 보인다.
  // 화면 범위(bounds)는 반대로 moveend에서만 갱신한다 — 팬/줌 도중 매 프레임
  // 갱신할 필요가 없고(끝난 뒤 한 번이면 충분), 그때마다 필터링을 다시 하면 낭비다.
  const map = useMapEvents({
    zoom: () => setZoom(map.getZoom()),
    moveend: () => setBounds(map.getBounds()),
  });
  useEffect(() => {
    setBounds(map.getBounds());
  }, [map]);
  const scale = zoomScale(zoom);
  const showCounts = zoom >= COUNT_LABEL_MIN_ZOOM;
  const alertsByStation = useMemo(() => new Map(alerts.map((alert) => [alert.sta_id, alert])), [alerts]);
  const priorityOf = (station: StationSummary) =>
    zPriority(alertsByStation.get(station.sta_id), station.sta_id === selectedStationId);

  // "부족한것만" 모드에서는 랭킹/화면범위 필터링 전에 먼저 대여소 후보군 자체를
  // 좁힌다(공급필요 + 선택된 대여소 주변 회수필요 후보). 이후 파이프라인은
  // "전체" 모드와 동일하게 이 후보군 안에서 랭킹/화면범위로 자른다.
  const filteredStations = useMemo(
    () => applyMapFilter(stations, alertsByStation, mapFilterMode, selectedStationId),
    [stations, alertsByStation, mapFilterMode, selectedStationId],
  );

  // urgency_score 정렬은 filteredStations/alerts가 바뀔 때만 다시 계산한다.
  // 화면 범위가 바뀔 때는(팬/줌) 이미 정렬된 배열을 거르기만 한다.
  const ranked = useMemo(() => rankByUrgency(filteredStations, alertsByStation), [filteredStations, alertsByStation]);
  const visible = useMemo(
    () => visibleStations(ranked, bounds, selectedStationId),
    [ranked, bounds, selectedStationId],
  );
  // SVG는 나중에 그린 요소가 위에 쌓인다. 우선순위가 높은(급하거나 선택된)
  // 대여소의 원이 겹쳤을 때 항상 위로 오도록, 낮은 우선순위부터 그리는 순서로
  // 뒤집어둔다(라벨의 zIndexOffset도 같은 zPriority를 쓴다 — 위 zPriority 참고).
  const stackOrder = useMemo(
    () => [...visible].sort((a, b) => priorityOf(a) - priorityOf(b)),
    [visible, alertsByStation, selectedStationId],
  );

  // z-order를 맞춰도, 작은 원이 큰 원에 완전히 덮여서 화면에 안 보이는데 그
  // 대여소의 숫자 라벨은 (다른 pane이라) 여전히 그려지는 문제가 남는다 —
  // "원 없는 숫자"가 떠 있는 것처럼 보인다. 화면 픽셀 좌표로 "내 중심이 위에
  // 그려진 다른 원 안에 들어있는지"를 계산해서, 그러면 라벨 자체를 안 그린다.
  //
  // 반드시 stackOrder를 그대로 뒤집어서 써야 한다 — 여기서 별도로 다시
  // 정렬하면, urgency_score가 같은(동점) 대여소들의 순서가 stackOrder와
  // 어긋날 수 있다(정렬은 안정적이지만 오름차순/내림차순을 따로 계산하면
  // 동점 순서가 반대로 나온다). 그러면 실제로는 위에 그려진 원인데 겹침
  // 판정에서는 아래로 취급돼서, 위에 있는 쪽의 라벨이 반대로 사라진다.
  const occludedStationIds = useMemo(() => {
    if (!showCounts) return new Set<string>();
    const withPixel = stackOrder.map((station) => {
      const alert = alertsByStation.get(station.sta_id);
      return {
        sta_id: station.sta_id,
        point: map.latLngToContainerPoint([station.lat, station.lon]),
        radius: markerRadius(alert) * scale,
      };
    });
    const topToBottom = [...withPixel].reverse();
    const occluded = new Set<string>();
    for (let i = 1; i < topToBottom.length; i++) {
      const station = topToBottom[i];
      for (let j = 0; j < i; j++) {
        const higher = topToBottom[j];
        const dx = station.point.x - higher.point.x;
        const dy = station.point.y - higher.point.y;
        if (Math.sqrt(dx * dx + dy * dy) < higher.radius) {
          occluded.add(station.sta_id);
          break;
        }
      }
    }
    return occluded;
  }, [stackOrder, alertsByStation, scale, map, showCounts]);

  // stations는 15초마다 폴링으로 새 배열이 들어오는데, 그때마다 이 effect가
  // 다시 돌면 선택을 바꾸지 않았는데도 지도가 계속 재이동한다. ref로 최신
  // 위치만 참조하고, effect 자체는 선택이 바뀔 때만 실행되게 한다.
  const stationsRef = useRef(stations);
  stationsRef.current = stations;

  useEffect(() => {
    if (selectedStationId === null) return;
    const station = stationsRef.current.find((s) => s.sta_id === selectedStationId);
    if (!station) return;
    // flyTo()의 애니메이션 도중에는 지도 타일과 마커(SVG 벡터 레이어)가 이
    // 개발 환경에서 서로 다른 프레임에 갱신돼 마커가 잠깐 멈춰 있다가 도착
    // 시점에 툭 튀는 것처럼 보인다. setView는 애니메이션 없이 한 번에
    // 이동시켜서 지도와 마커가 항상 같은 프레임에 같이 자리 잡는다.
    map.setView([station.lat, station.lon], Math.max(map.getZoom(), COUNT_LABEL_MIN_ZOOM));
  }, [selectedStationId, map]);

  const regionCell = useMemo(
    () => computeRegionCell(regionCenters, selectedRegion),
    [regionCenters, selectedRegion],
  );

  useEffect(() => {
    if (selectedRegion === ALL_REGIONS) return;
    if (!regionCell || regionCell.length === 0) return;
    // 선택된 권역의 경계(보로노이 셀) 전체가 화면에 들어오게 이동한다. 대여소
    // 이동(setView, 위 effect)과 달리 여기는 넓은 영역을 한 번에 보여줘야 하므로
    // fitBounds를 쓴다.
    map.fitBounds(L.latLngBounds(regionCell.flat()), { padding: [24, 24] });
  }, [selectedRegion, regionCell, map]);

  return (
    <>
      <Polygon positions={SEOUL_OUTLINE} pathOptions={{ color: "#9a9a9a", weight: 1.5, fill: false }} interactive={false} />
      {regionCell && (
        <Polygon
          positions={regionCell}
          pathOptions={{ color: REGION_FILL, weight: 2, fillColor: REGION_FILL, fillOpacity: 0.08 }}
          interactive={false}
        />
      )}
      {/* 마커 자체보다 넓게 깔아 두는 투명 클릭 영역. 시각적 마커 밑에 먼저 그려야
          마커 위에서의 호버가 그대로 툴팁을 띄우고, 마커 밖 여백(CLICK_PADDING)만
          이 레이어가 받아서 대충 눌러도 선택되게 한다. */}
      {stackOrder.map((station) => {
        const alert = alertsByStation.get(station.sta_id);
        return (
          <CircleMarker
            key={`hit-${station.sta_id}`}
            center={[station.lat, station.lon]}
            radius={(markerRadius(alert) + CLICK_PADDING) * scale}
            pathOptions={{ stroke: false, fillOpacity: 0.01 }}
            eventHandlers={{ click: () => onSelect(station.sta_id) }}
          />
        );
      })}
      {stackOrder.map((station) => {
        const isSelected = station.sta_id === selectedStationId;
        const alert = alertsByStation.get(station.sta_id);
        const radius = markerRadius(alert) * scale;
        const handleClick = () => onSelect(station.sta_id);
        return (
          <CircleMarker
            key={station.sta_id}
            center={[station.lat, station.lon]}
            radius={radius}
            pathOptions={{
              color: isSelected ? SELECTED_BORDER : IDLE_BORDER,
              weight: isSelected ? 3 : 2,
              fillColor: markerColor(alert),
              fillOpacity: 1,
            }}
            eventHandlers={{ click: handleClick }}
          >
            <Tooltip direction="top" offset={[0, -8]}>
              {station.sta_nm} · {station.parking_bike_tot_cnt}/{station.hold_cnt}대
            </Tooltip>
          </CircleMarker>
        );
      })}
      {showCounts &&
        visible
          .filter((station) => !occludedStationIds.has(station.sta_id))
          .map((station) => {
            const alert = alertsByStation.get(station.sta_id);
            const priority = priorityOf(station);
            return (
              <Marker
                key={`count-${station.sta_id}`}
                position={[station.lat, station.lon]}
                icon={countIcon(station.parking_bike_tot_cnt, markerRadius(alert) * scale)}
                // Leaflet Marker는 기본적으로 화면 y좌표가 낮을수록(아래쪽일수록)
                // 위로 오게 z-index를 매긴다. 원 쌓임 순서(zPriority)와 안 맞으면
                // 자기 원은 덮였는데 숫자만 남는 상황이 생기므로, 같은 우선순위를
                // 화면 y좌표보다 훨씬 큰 오프셋으로 줘서 그 기본 규칙을 덮어씌운다.
                zIndexOffset={Number.isFinite(priority) ? Math.round(priority * 1000) : 1_000_000}
                interactive={false}
              />
            );
          })}
    </>
  );
}

function MapResizer() {
  const map = useMapEvents({});
  useEffect(() => {
    const container = map.getContainer();
    const observer = new ResizeObserver(() => {
      map.invalidateSize();
    });
    observer.observe(container);
    return () => observer.disconnect();
  }, [map]);
  return null;
}

export function StationMap({
  stations,
  alerts,
  selectedStationId,
  onSelect,
  mapFilterMode,
  regionCenters,
  selectedRegion,
}: Props) {
  return (
    <MapContainer
      center={GANGNAM_CENTER}
      zoom={DEFAULT_ZOOM}
      minZoom={SEOUL_MIN_ZOOM}
      maxBounds={SEOUL_BOUNDS}
      maxBoundsViscosity={1.0}
      style={{ height: "100%", width: "100%" }}
      wheelDebounceTime={100}
    >
      <MapResizer />
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
        url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
      />
      <StationMarkers
        stations={stations}
        alerts={alerts}
        selectedStationId={selectedStationId}
        onSelect={onSelect}
        mapFilterMode={mapFilterMode}
        regionCenters={regionCenters}
        selectedRegion={selectedRegion}
      />
    </MapContainer>
  );
}
