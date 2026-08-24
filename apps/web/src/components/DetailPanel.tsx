import { useEffect, useRef, useState } from "react";
import { api } from "../api";
import type { CulturalEvent, StationDetail, WeatherPoint } from "../api";
import { formatIsoTime } from "../format";

export interface FocusedEvent {
  eventLat: number;
  eventLon: number;
  searchCenterLat: number;
  searchCenterLon: number;
  radiusKm: number;
}

interface StationPoint {
  lat: number;
  lon: number;
}

type Tab = "info" | "events" | "weather";

const TABS: { key: Tab; label: string }[] = [
  { key: "info", label: "대여소 정보" },
  { key: "events", label: "주변 행사" },
  { key: "weather", label: "주변 날씨" },
];
const DETAIL_POLL_INTERVAL_MS = 60_000;
const SKY_LABEL: Record<WeatherPoint["sky_condition_cd"], string> = {
  clear: "맑음",
  mostly_cloudy: "구름 많음",
  cloudy: "흐림",
};
const PRECIPITATION_LABEL: Record<WeatherPoint["precipitation_type_cd"], string> = {
  none: "없음",
  rain: "비",
  rain_snow: "비/눈",
  snow: "눈",
  shower: "소나기",
  raindrop: "빗방울",
  raindrop_snow_flurry: "빗방울/눈날림",
  snow_flurry: "눈날림",
};

interface Props {
  stationId: string | null;
  stationPoint: StationPoint | null;
  onFocusEvent: (event: FocusedEvent | null) => void;
}

function nullableMeasurement(value: number | null, suffix: string): string {
  return value === null ? "-" : `${value}${suffix}`;
}

export function DetailPanel({ stationId, stationPoint, onFocusEvent }: Props) {
  const [tab, setTab] = useState<Tab>("info");
  const [detail, setDetail] = useState<StationDetail | null>(null);
  const [detailError, setDetailError] = useState(false);
  const [events, setEvents] = useState<CulturalEvent[] | null>(null);
  const [eventsError, setEventsError] = useState(false);
  const [radiusKm, setRadiusKm] = useState<number | null>(null);
  const [weather, setWeather] = useState<WeatherPoint[] | null>(null);
  const [weatherError, setWeatherError] = useState(false);
  const [focusedEventId, setFocusedEventId] = useState<string | null>(null);
  const focusedEventIdRef = useRef(focusedEventId);
  focusedEventIdRef.current = focusedEventId;

  useEffect(() => {
    setTab("info");
    setDetail(null);
    setDetailError(false);
    setEvents(null);
    setEventsError(false);
    setRadiusKm(null);
    setWeather(null);
    setWeatherError(false);
    setFocusedEventId(null);
    onFocusEvent(null);
  }, [stationId, onFocusEvent]);

  useEffect(() => {
    if (tab !== "events") {
      setFocusedEventId(null);
      onFocusEvent(null);
    }
  }, [tab, onFocusEvent]);

  useEffect(() => {
    if (stationId === null) return;
    let cancelled = false;
    let requestGeneration = 0;
    function refresh() {
      const currentGeneration = ++requestGeneration;
      api
        .station(stationId as string)
        .then((data) => {
          if (!cancelled && currentGeneration === requestGeneration) {
            setDetail(data);
            setDetailError(false);
          }
        })
        .catch(() => {
          if (!cancelled && currentGeneration === requestGeneration) {
            setDetailError(true);
          }
        });
    }
    refresh();
    const timer = setInterval(refresh, DETAIL_POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [stationId]);

  useEffect(() => {
    if (stationId === null || tab !== "events") return;
    let cancelled = false;
    let requestGeneration = 0;
    setEvents(null);
    setEventsError(false);
    setRadiusKm(null);
    function refresh() {
      const currentGeneration = ++requestGeneration;
      api
        .events(stationId as string)
        .then((data) => {
          if (cancelled || currentGeneration !== requestGeneration) return;
          setEvents(data.events);
          setRadiusKm(data.radius_km);
          setEventsError(false);
          const focusedId = focusedEventIdRef.current;
          if (focusedId !== null && !data.events.some((event) => event.event_id === focusedId)) {
            setFocusedEventId(null);
            onFocusEvent(null);
          }
        })
        .catch(() => {
          if (!cancelled && currentGeneration === requestGeneration) {
            setEventsError(true);
          }
        });
    }
    refresh();
    const timer = setInterval(refresh, DETAIL_POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [stationId, tab, onFocusEvent]);

  useEffect(() => {
    if (stationId === null || tab !== "weather") return;
    let cancelled = false;
    let requestGeneration = 0;
    setWeather(null);
    setWeatherError(false);
    function refresh() {
      const currentGeneration = ++requestGeneration;
      api
        .weather(stationId as string)
        .then((data) => {
          if (!cancelled && currentGeneration === requestGeneration) {
            setWeather(data.points);
            setWeatherError(false);
          }
        })
        .catch(() => {
          if (!cancelled && currentGeneration === requestGeneration) {
            setWeatherError(true);
          }
        });
    }
    refresh();
    const timer = setInterval(refresh, DETAIL_POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [stationId, tab]);

  if (stationId === null) {
    return <p className="empty-state">지도나 우측 리스트에서 대여소를 선택하세요.</p>;
  }

  return (
    <div className="detail-panel-wrap">
      <div className="alert-tabs" role="tablist">
        {TABS.map((item) => (
          <button
            key={item.key}
            type="button"
            role="tab"
            aria-selected={tab === item.key}
            className={`alert-tab${tab === item.key ? " active" : ""}`}
            onClick={() => setTab(item.key)}
          >
            {item.label}
          </button>
        ))}
      </div>

      <div className="detail-panel-content">
        {tab === "info" ? (
          detailError && !detail ? (
            <p className="empty-state">대여소 정보를 불러오지 못했습니다.</p>
          ) : !detail ? (
            <p className="empty-state">불러오는 중...</p>
          ) : (
            <div className="station-info">
              {detailError && (
                <p className="data-refresh-warning" role="status">
                  상세 조회에 실패해 마지막 결과를 표시합니다.
                </p>
              )}
              <header className="station-info-header">
                <h3>{detail.sta_nm}</h3>
                <p>{detail.sta_addr}</p>
              </header>

              <div className="station-stock-card">
                <div
                  className="stock-donut"
                  role="img"
                  aria-label={`현재 자전거 ${detail.parking_bike_tot_cnt}대, 거치대 ${detail.hold_cnt}대`}
                >
                  <svg viewBox="0 0 42 42" aria-hidden="true">
                    <circle className="stock-donut-track" cx="21" cy="21" r="16" />
                    <circle
                      className="stock-donut-value"
                      cx="21"
                      cy="21"
                      r="16"
                      pathLength="100"
                      strokeDasharray={`${Math.min(100, Math.max(0, Math.round(detail.shared_rate * 100)))} 100`}
                    />
                  </svg>
                  <span className="stock-donut-number">
                    <strong>{detail.parking_bike_tot_cnt}</strong>
                    <small>/ {detail.hold_cnt}대</small>
                  </span>
                </div>
              </div>
            </div>
          )
        ) : tab === "events" ? (
          eventsError && events === null ? (
            <p className="empty-state">주변 행사 정보를 불러오지 못했습니다.</p>
          ) : events === null ? (
            <p className="empty-state">불러오는 중...</p>
          ) : events.length === 0 ? (
            <p className="empty-state">주변에 진행 중인 행사가 없습니다.</p>
          ) : (
            <div className="data-preserving-panel">
              {eventsError && (
                <p className="data-refresh-warning" role="status">
                  행사 조회에 실패해 마지막 결과를 표시합니다.
                </p>
              )}
              <ul className="event-list">
                {events.map((event) => {
                  const isFocused = focusedEventId === event.event_id;
                  return (
                    <li key={event.event_id}>
                      <button
                        type="button"
                        className={`event-item${isFocused ? " selected" : ""}`}
                        onClick={() => {
                          if (radiusKm === null || stationPoint === null) return;
                          if (isFocused) {
                            setFocusedEventId(null);
                            onFocusEvent(null);
                          } else {
                            setFocusedEventId(event.event_id);
                            onFocusEvent({
                              eventLat: event.lat,
                              eventLon: event.lon,
                              searchCenterLat: stationPoint.lat,
                              searchCenterLon: stationPoint.lon,
                              radiusKm,
                            });
                          }
                        }}
                      >
                        <span className="event-item-title">{event.title}</span>
                        <span className="event-item-meta">
                          {[event.place, `${event.start_date} ~ ${event.end_date}`, `${event.distance_km}km`]
                            .filter(Boolean)
                            .join(" · ")}
                        </span>
                      </button>
                    </li>
                  );
                })}
              </ul>
            </div>
          )
        ) : weatherError && weather === null ? (
          <p className="empty-state">주변 날씨 정보를 불러오지 못했습니다.</p>
        ) : weather === null ? (
          <p className="empty-state">불러오는 중...</p>
        ) : (
          <div className="data-preserving-panel">
            {weatherError && (
              <p className="data-refresh-warning" role="status">
                날씨 조회에 실패해 마지막 결과를 표시합니다.
              </p>
            )}
            <ul className="weather-list">
              {weather.map((point) => (
                <li key={point.forecast_dttm} className="weather-item">
                  <span className="weather-item-time">
                    {formatIsoTime(point.forecast_dttm, { month: "numeric", day: "numeric", hour: "2-digit", minute: "2-digit" })}
                  </span>
                  <strong>{point.temperature}℃</strong>
                  <span>{SKY_LABEL[point.sky_condition_cd]}</span>
                  <span>강수 {PRECIPITATION_LABEL[point.precipitation_type_cd]}</span>
                  <span>확률 {nullableMeasurement(point.precipitation_prob, "%")}</span>
                  <span>강수량 {nullableMeasurement(point.precipitation_amount, "mm")}</span>
                  <span>습도 {nullableMeasurement(point.humidity, "%")}</span>
                  <span>풍속 {nullableMeasurement(point.wind_speed, "m/s")}</span>
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </div>
  );
}
