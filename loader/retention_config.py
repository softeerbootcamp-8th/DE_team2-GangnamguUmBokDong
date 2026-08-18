"""RDS 만료 행 정리(retention)에 쓰이는 유예기간. 만료됐다고 바로 지우면 "그때
예측·예보가 실제와 얼마나 맞았는지" 사후 비교·분석할 데이터가 안 남으므로, 이
유예기간이 지난 뒤에만 삭제한다(칼같이 now() 기준 즉시 삭제하지 않음)."""

from datetime import timedelta

RETENTION_GRACE: dict[str, timedelta] = {
    "weather_forecast": timedelta(hours=2),
    "forecast_points": timedelta(hours=2),
    "cultural_events": timedelta(days=3),
}

# end_date는 DATE 컬럼이라(TIMESTAMPTZ가 아님) datetime이 아니라 date로 비교해야
# 타입 캐스팅이 모호해지지 않는다.
DATE_TYPED_EXPIRE_TABLES = {"cultural_events"}
