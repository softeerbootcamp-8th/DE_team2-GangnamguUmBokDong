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


def grace_for(target_table: str) -> timedelta:
    """target_table의 유예기간을 반환한다. tables.yaml에 expire_col만 추가하고
    여기에 유예기간을 안 적으면 적재 도중 KeyError로 죽으므로, 원인을 짚어주는
    에러로 바꾼다(실제 방어는 config._validate_retention_config가 임포트 시점에 한다)."""
    try:
        return RETENTION_GRACE[target_table]
    except KeyError:
        raise KeyError(
            f"{target_table}에 expire_col이 선언됐지만 RETENTION_GRACE에 유예기간이 없다. "
            f"loader/retention_config.py에 '{target_table}' 항목을 추가하라."
        ) from None
