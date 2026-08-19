import { useEffect, useState } from "react";
import { api } from "../api";
import { formatClock, formatIsoTime } from "../format";
import logo from "../../assets/logo_transparent.png";

const STATUS_POLL_INTERVAL_MS = 30_000;

export function Header() {
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
      </span>
      <div className="app-header-times">
        <span>현재 시각 {formatClock(now)}</span>
        <span className={statusError ? "status-error" : undefined}>
          예측 시각 {statusError ? "갱신 실패" : predictedAt ? formatIsoTime(predictedAt, { hour: "2-digit", minute: "2-digit" }) : "-"}
        </span>
      </div>
    </header>
  );
}
