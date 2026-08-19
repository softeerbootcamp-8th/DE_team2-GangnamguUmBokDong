import { ApiError } from "../api";
import type { ForecastResponse, StationSummary } from "../api";
import { ForecastChart } from "./ForecastChart";

interface Props {
  station: StationSummary | null;
  forecast: ForecastResponse | null;
  error: Error | null;
}

export function forecastUnavailableMessage(error: Error): string {
  if (error instanceof ApiError && error.status === 404) return "이 대여소는 예측을 지원하지 않습니다.";
  if (error instanceof ApiError && error.status === 503) return "예측 데이터를 갱신 중입니다.";
  return "예측 데이터를 불러오지 못했습니다.";
}

export function ForecastPanel({ station, forecast, error }: Props) {
  if (!station) {
    return <p className="empty-state">지도나 우측 리스트에서 대여소를 선택하세요.</p>;
  }

  if (error) {
    return <p className="empty-state">{forecastUnavailableMessage(error)}</p>;
  }

  if (!forecast) {
    return <p className="empty-state">예측 데이터를 불러오는 중...</p>;
  }

  return <ForecastChart points={forecast.points} />;
}
