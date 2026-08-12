import "leaflet/dist/leaflet.css";
import L from "leaflet";
import { useEffect, useRef, useState } from "react";
import { CircleMarker, MapContainer, Marker, TileLayer, Tooltip, useMapEvents } from "react-leaflet";
import type { ActionType, Alert, StationSummary } from "../api";

const GANGNAM_CENTER: [number, number] = [37.5172, 127.0473];
const DEFAULT_ZOOM = 13;
const BASE_ZOOM = DEFAULT_ZOOM; // 마커 크기의 기준 줌(이 레벨에서 MIN~MAX_RADIUS 그대로 나옴)
const COUNT_LABEL_MIN_ZOOM = 14; // 이 줌 레벨부터 재고 수를 라벨로 띄운다
const MIN_RADIUS = 9;
const MAX_RADIUS = 20;
const CLICK_PADDING = 10; // 시각적 마커보다 이만큼 더 넓게 클릭을 받는다
const SELECTED_BORDER = "#0b0b0b";
const IDLE_BORDER = "#fcfcfb";

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
  const t = Math.min(1, Math.max(0, (alert?.urgency_score ?? 0) / 100));
  return mixHex(DIVERGING_NEUTRAL, target, t);
}

function markerRadius(alert: Alert | undefined): number {
  const score = alert?.urgency_score ?? 0;
  return MIN_RADIUS + (score / 100) * (MAX_RADIUS - MIN_RADIUS);
}

// 재고 수 라벨의 배경색. 마커 자체(채우기 색)는 우선순위 방향을 보여주니까,
// 이 라벨은 "지금 이 순간 실제로 얼마나 비었거나 찼는지"를 따로 보여주는
// 용도로 둔다. 진짜 심한 경우(20% 이하/80% 이상)만 색을 칠하고, 평범한
// 범위는 그냥 흰 배경으로 둬서 정말 심각한 것만 눈에 띄게 한다.
function currentStatusColor(station: StationSummary): string | null {
  const ratio = station.parking_bike_tot_cnt / station.hold_cnt;
  if (ratio <= 0.2) return DIVERGING_RED;
  if (ratio >= 0.8) return DIVERGING_BLUE;
  return null;
}

// 라벨을 마커 위 고정 픽셀만큼 띄우면, 줌에 따라 마커가 커질 때 라벨이 커진
// 마커 안에 파묻힌다. 그 마커의 실제 반지름(markerRadius) 기준으로 띄우는
// 거리를 같이 계산해서 항상 마커 위 여백에 떠 있게 한다.
function countIcon(count: number, tintColor: string | null, markerRadiusPx: number) {
  const colorStyle = tintColor ? `background:${tintColor};color:#fff;border-color:${tintColor};` : "";
  const offset = markerRadiusPx + 12;
  return L.divIcon({
    className: "marker-count-label",
    html: `<span style="${colorStyle}transform:translate(-50%, -${offset}px);">${count}</span>`,
    iconSize: [0, 0],
  });
}

interface Props {
  stations: StationSummary[];
  alerts: Alert[];
  selectedStationId: number | null;
  onSelect: (stationId: number) => void;
}

function StationMarkers({ stations, alerts, selectedStationId, onSelect }: Props) {
  const [zoom, setZoom] = useState(DEFAULT_ZOOM);
  // zoomend가 아니라 zoom을 듣는다. zoom은 줌 애니메이션이 진행되는 동안
  // 프레임마다(줌 버튼처럼 순간 전환일 때도 포함) 계속 발생해서, 마커 크기가
  // 애니메이션 도중에도 실시간으로 따라 커지고 작아진다. zoomend만 들으면
  // 애니메이션이 다 끝난 뒤에야 한 번에 크기가 바뀌어 뚝뚝 끊겨 보인다.
  const map = useMapEvents({
    zoom: () => setZoom(map.getZoom()),
  });
  const scale = zoomScale(zoom);
  const showCounts = zoom >= COUNT_LABEL_MIN_ZOOM;
  const alertsByStation = new Map(alerts.map((alert) => [alert.sta_id, alert]));

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

  return (
    <>
      {/* 마커 자체보다 넓게 깔아 두는 투명 클릭 영역. 시각적 마커 밑에 먼저 그려야
          마커 위에서의 호버가 그대로 툴팁을 띄우고, 마커 밖 여백(CLICK_PADDING)만
          이 레이어가 받아서 대충 눌러도 선택되게 한다. */}
      {stations.map((station) => {
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
      {stations.map((station) => {
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
        stations.map((station) => {
          const alert = alertsByStation.get(station.sta_id);
          return (
            <Marker
              key={`count-${station.sta_id}`}
              position={[station.lat, station.lon]}
              icon={countIcon(station.parking_bike_tot_cnt, currentStatusColor(station), markerRadius(alert) * scale)}
              interactive={false}
            />
          );
        })}
    </>
  );
}

export function StationMap({ stations, alerts, selectedStationId, onSelect }: Props) {
  return (
    <MapContainer
      center={GANGNAM_CENTER}
      zoom={DEFAULT_ZOOM}
      style={{ height: "100%", width: "100%" }}
      wheelDebounceTime={100}
    >
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
        url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
      />
      <StationMarkers stations={stations} alerts={alerts} selectedStationId={selectedStationId} onSelect={onSelect} />
    </MapContainer>
  );
}
