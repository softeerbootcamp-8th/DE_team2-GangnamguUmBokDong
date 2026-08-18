import { useEffect, useState } from "react";
import { api } from "../api";
import type { CulturalEvent, StationDetail } from "../api";
import { formatIsoTime } from "../format";

type Tab = "info" | "events";

const TABS: { key: Tab; label: string }[] = [
  { key: "info", label: "대여소 정보" },
  { key: "events", label: "행사" },
];

interface Props {
  stationId: string | null;
  reasons: string[];
}

export function DetailPanel({ stationId, reasons }: Props) {
  const [tab, setTab] = useState<Tab>("info");
  const [detail, setDetail] = useState<StationDetail | null>(null);
  const [events, setEvents] = useState<CulturalEvent[] | null>(null);

  // 대여소를 바꾸면 이전 대여소에서 골라둔 탭이 그대로 유지될 이유가 없다.
  useEffect(() => {
    setTab("info");
  }, [stationId]);

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
    api.events(stationId).then((data) => {
      if (!cancelled) setEvents(data);
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
      ) : events === null ? (
        <p className="empty-state">불러오는 중...</p>
      ) : events.length === 0 ? (
        <p className="empty-state">주변에 진행 중인 행사가 없습니다.</p>
      ) : (
        <ul className="event-list">
          {events.map((event) => (
            <li key={event.event_id} className="event-item">
              <span className="event-item-title">{event.title}</span>
              <span className="event-item-meta">
                {[event.place, [event.start_date, event.end_date].filter(Boolean).join(" ~ "), `${event.distance_km}km`]
                  .filter(Boolean)
                  .join(" · ")}
              </span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
