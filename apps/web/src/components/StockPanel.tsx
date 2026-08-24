import type { ForecastResponse, StationSummary } from "../api";
import { forecastUnavailableMessage } from "./ForecastPanel";
import { StockChart } from "./StockChart";

interface Props {
  station: StationSummary | null;
  forecast: ForecastResponse | null;
  error: Error | null;
}

export function StockPanel({ station, forecast, error }: Props) {
  if (!station) {
    return <p className="empty-state">지도나 우측 리스트에서 대여소를 선택하세요.</p>;
  }

  if (error && !forecast) {
    return <p className="empty-state">{forecastUnavailableMessage(error)}</p>;
  }

  if (!forecast) {
    return <p className="empty-state">예측 데이터를 불러오는 중...</p>;
  }

  return (
    <div className="chart-with-status">
      {error && (
        <p className="data-refresh-warning" role="status">
          예측 조회에 실패해 마지막 결과를 표시합니다.
        </p>
      )}
      <StockChart station={station} baseDttm={forecast.base_dttm} points={forecast.points} />
    </div>
  );
}
