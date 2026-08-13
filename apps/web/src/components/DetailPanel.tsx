import { useEffect, useState } from "react";
import { api } from "../api";
import type { StationDetail } from "../api";
import { formatIsoTime } from "../format";

interface Props {
  stationId: number | null;
  reasons: string[];
}

export function DetailPanel({ stationId, reasons }: Props) {
  const [detail, setDetail] = useState<StationDetail | null>(null);

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

  if (stationId === null) {
    return <p className="empty-state">대여소를 선택하면 상세 정보가 표시됩니다.</p>;
  }

  if (!detail) {
    return <p className="empty-state">불러오는 중...</p>;
  }

  return (
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
  );
}
