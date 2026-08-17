import { useEffect, useState } from "react";
import { api } from "../api";
import { formatClock, formatIsoTime } from "../format";

const STATUS_POLL_INTERVAL_MS = 30_000;

export function Header() {
  const [now, setNow] = useState(new Date());
  const [predictedAt, setPredictedAt] = useState<string | null>(null);

  useEffect(() => {
    const clock = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(clock);
  }, []);

  useEffect(() => {
    let cancelled = false;
    function refresh() {
      api.status().then((data) => {
        if (!cancelled) setPredictedAt(data.base_dttm);
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
      <h1>서울시 따릉이 재배치 대시보드</h1>
      <div className="app-header-times">
        <span>현재 시각 {formatClock(now)}</span>
        <span>예측 시각 {predictedAt ? formatIsoTime(predictedAt, { hour: "2-digit", minute: "2-digit" }) : "-"}</span>
      </div>
    </header>
  );
}
