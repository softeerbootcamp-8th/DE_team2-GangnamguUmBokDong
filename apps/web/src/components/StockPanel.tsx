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

  if (error) {
    return <p className="empty-state">{forecastUnavailableMessage(error)}</p>;
  }

  if (!forecast) {
    return <p className="empty-state">예측 데이터를 불러오는 중...</p>;
  }

  return <StockChart station={station} baseDttm={forecast.base_dttm} points={forecast.points} />;
}
