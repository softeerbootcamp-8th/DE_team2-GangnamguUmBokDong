import { useEffect, useState } from "react";
import { api } from "../api";
import type { CulturalEvent, StationDetail } from "../api";
import { formatIsoTime } from "../format";

export interface FocusedEvent {
  lat: number;
  lon: number;
  radiusKm: number;
}

type Tab = "info" | "events" | "weather";

const TABS: { key: Tab; label: string }[] = [
  { key: "info", label: "대여소 정보" },
  { key: "events", label: "주변 행사" },
  { key: "weather", label: "주변 날씨" },
];

interface Props {
  stationId: string | null;
  reasons: string[];
  onFocusEvent: (event: FocusedEvent | null) => void;
}

export function DetailPanel({ stationId, reasons, onFocusEvent }: Props) {
  const [tab, setTab] = useState<Tab>("info");
  const [detail, setDetail] = useState<StationDetail | null>(null);
  const [events, setEvents] = useState<CulturalEvent[] | null>(null);
  const [eventsError, setEventsError] = useState(false);
  const [radiusKm, setRadiusKm] = useState<number | null>(null);
  const [focusedEventId, setFocusedEventId] = useState<string | null>(null);

  // 대여소를 바꾸면 이전 대여소에서 골라둔 탭이 그대로 유지될 이유가 없다.
  // 탭이 "주변 행사"에서 벗어나면(정보/날씨로 이동, 혹은 여기서 info로
  // 리셋되는 경우 포함) 아래 effect가 지도에 띄운 행사 포커스도 같이 지운다.
  useEffect(() => {
    setTab("info");
  }, [stationId]);

  // "주변 행사" 탭을 벗어나면 포커싱한 행사가 더 이상 의미 없으니 지도 표시를
  // 지우고, 대여소 포커싱으로 돌아가게 한다(StationMap.tsx가 focusedEvent가
  // null이 되면 다시 선택된 대여소로 지도를 옮긴다).
  useEffect(() => {
    if (tab !== "events") {
      setFocusedEventId(null);
      onFocusEvent(null);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab]);

  useEffect(() => {
    if (stationId === null) {
      setDetail(null);
      return;
    }
    let cancelled = false;
    api.station(stationId).then((data) => {
      if (!cancelled) setDetail(data);
    });
    return () => {
      cancelled = true;
    };
  }, [stationId]);

  // 행사 목록은 정보 탭보다 무거운 조회라, 실제로 행사 탭을 열었을 때만 가져온다.
  useEffect(() => {
    if (stationId === null || tab !== "events") return;
    let cancelled = false;
    setEvents(null);
    setEventsError(false);
    api
      .events(stationId)
      .then((data) => {
        if (!cancelled) {
          setEvents(data.events);
          setRadiusKm(data.radius_km);
        }
      })
      .catch(() => {
        if (!cancelled) setEventsError(true);
      });
    return () => {
      cancelled = true;
    };
  }, [stationId, tab]);

  if (stationId === null) {
    return <p className="empty-state">지도나 우측 리스트에서 대여소를 선택하세요.</p>;
  }

  return (
    <div className="detail-panel-wrap">
      <div className="alert-tabs" role="tablist">
        {TABS.map((t) => (
          <button
            key={t.key}
            type="button"
            role="tab"
            aria-selected={tab === t.key}
            className={`alert-tab${tab === t.key ? " active" : ""}`}
            onClick={() => setTab(t.key)}
          >
            {t.label}
          </button>
        ))}
      </div>

      {tab === "info" ? (
        !detail ? (
          <p className="empty-state">불러오는 중...</p>
        ) : (
          <dl className="detail-grid">
            <dt>대여소명</dt>
            <dd>{detail.sta_nm}</dd>
            <dt>주소</dt>
            <dd>{detail.sta_addr}</dd>
            <dt>현재 자전거 수</dt>
            <dd>
              {detail.parking_bike_tot_cnt} / {detail.hold_cnt}대 ({Math.round(detail.shared_rate * 100)}%)
            </dd>
            <dt>갱신 시각</dt>
            <dd>{formatIsoTime(detail.base_dttm)}</dd>
            {reasons.length > 0 && (
              <>
                <dt>수요 영향 사유</dt>
                <dd>
                  <ul className="reason-list">
                    {reasons.map((reason) => (
                      <li key={reason}>{reason}</li>
                    ))}
                  </ul>
                </dd>
              </>
            )}
          </dl>
        )
      ) : tab === "events" ? (
        eventsError ? (
          <p className="empty-state">주변 행사 정보를 불러오지 못했습니다.</p>
        ) : events === null ? (
          <p className="empty-state">불러오는 중...</p>
        ) : events.length === 0 ? (
          <p className="empty-state">주변에 진행 중인 행사가 없습니다.</p>
        ) : (
          <ul className="event-list">
            {events.map((event) => {
              const isFocused = focusedEventId === event.event_id;
              return (
                <li key={event.event_id}>
                  <button
                    type="button"
                    className={`event-item${isFocused ? " selected" : ""}`}
                    onClick={() => {
                      if (radiusKm === null) return;
                      if (isFocused) {
                        setFocusedEventId(null);
                        onFocusEvent(null);
                      } else {
                        setFocusedEventId(event.event_id);
                        onFocusEvent({ lat: event.lat, lon: event.lon, radiusKm });
                      }
                    }}
                  >
                    <span className="event-item-title">{event.title}</span>
                    <span className="event-item-meta">
                      {[event.place, [event.start_date, event.end_date].filter(Boolean).join(" ~ "), `${event.distance_km}km`]
                        .filter(Boolean)
                        .join(" · ")}
                    </span>
                  </button>
                </li>
              );
            })}
          </ul>
        )
      ) : (
        // 날씨는 격자-구 매핑 정확도 문제(#99)가 팀 논의로 결론 나야 데이터 형태가
        // 정해져서 아직 못 붙였다. 탭 자리만 미리 만들어둔다.
        <p className="empty-state">주변 날씨는 준비 중입니다.</p>
      )}
    </div>
  );
}
