import type { ForecastResponse, StationSummary } from "../api";
import { ForecastChart } from "./ForecastChart";

interface Props {
  station: StationSummary | null;
  forecast: ForecastResponse | null;
}

export function ForecastPanel({ station, forecast }: Props) {
  if (!station) {
    return <p className="empty-state">지도나 우측 리스트에서 대여소를 선택하세요.</p>;
  }

  if (!forecast) {
    return <p className="empty-state">예측 데이터를 불러오는 중...</p>;
  }

  return <ForecastChart points={forecast.points} />;
}
