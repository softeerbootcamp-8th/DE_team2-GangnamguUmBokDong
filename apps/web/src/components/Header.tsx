import { useEffect, useState } from "react";
import { api } from "../api";
import type { DispatchCenter } from "../api";
import { formatClock, formatIsoTime } from "../format";
import { RegionTabs } from "./RegionTabs";
import logo from "../../assets/ubd_logo.png";

const STATUS_POLL_INTERVAL_MS = 30_000;

interface Props {
  regions: DispatchCenter[];
  selectedRegion: string;
  stationsUpdatedAt: Date | null;
  onRegionChange: (region: string) => void;
}

interface HeaderTimeProps {
  id: string;
  label: string;
  value: string;
  description: string;
  error?: boolean;
}

function HeaderTime({ id, label, value, description, error = false }: HeaderTimeProps) {
  return (
    <span className={`header-time${error ? " status-error" : ""}`}>
      <span>{label} {value}</span>
      <span className="header-time-help">
        <button
          type="button"
          className="header-time-info"
          aria-label={`${label} 설명`}
          aria-describedby={`${id}-tooltip`}
        >
          i
        </button>
        <span id={`${id}-tooltip`} className="header-time-tooltip" role="tooltip">
          {description}
        </span>
      </span>
    </span>
  );
}

export function Header({ regions, selectedRegion, stationsUpdatedAt, onRegionChange }: Props) {
  const [now, setNow] = useState(new Date());
  const [predictedAt, setPredictedAt] = useState<string | null>(null);
  const [statusError, setStatusError] = useState(false);

  useEffect(() => {
    const clock = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(clock);
  }, []);

  useEffect(() => {
    let cancelled = false;
    let requestGeneration = 0;
    function refresh() {
      const currentGeneration = ++requestGeneration;
      api
        .status()
        .then((data) => {
          if (!cancelled && currentGeneration === requestGeneration) {
            setPredictedAt(data.base_dttm);
            setStatusError(false);
          }
        })
        .catch(() => {
          if (!cancelled && currentGeneration === requestGeneration) {
            setPredictedAt(null);
            setStatusError(true);
          }
        });
    }
    refresh();
    const timer = setInterval(refresh, STATUS_POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, []);

  return (
    <header className="app-header">
      <span className="app-header-title">
        <img src={logo} alt="" className="app-header-logo" />
        <h1>서울특별시 따릉이 재배치 대시보드</h1>
        <RegionTabs
          regions={regions}
          selectedRegion={selectedRegion}
          onChange={onRegionChange}
        />
      </span>
      <div className="app-header-times">
        <HeaderTime
          id="current-time"
          label="현재 시각"
          value={formatClock(now)}
          description="이 브라우저 기기의 현재 시각입니다."
        />
        <HeaderTime
          id="station-query-time"
          label="조회 시각"
          value={stationsUpdatedAt ? formatClock(stationsUpdatedAt) : "-"}
          description="현재 화면의 대여소 정보를 API에서 성공적으로 조회한 시각입니다. 조회에 실패하면 -로 표시됩니다."
        />
        <HeaderTime
          id="forecast-base-time"
          label="기준 시각"
          value={statusError ? "갱신 실패" : predictedAt ? formatIsoTime(predictedAt, { hour: "2-digit", minute: "2-digit" }) : "-"}
          description="전체 수요예측이 공통으로 사용하는 데이터 기준 시각입니다. 최근 10분 내의 일관된 예측만 표시됩니다."
          error={statusError}
        />
      </div>
    </header>
  );
}
