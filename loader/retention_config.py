"""파생 예측의 RDS 만료 행 정리(retention) 유예기간을 정의한다."""

from datetime import timedelta

RETENTION_GRACE: dict[str, timedelta] = {
    "forecast_points": timedelta(hours=2),
}

# 원천 weather/event projection은 publication publisher가 원자 reconcile하므로, 이
# 레거시 loader에는 DATE 기반 retention 대상이 남아 있지 않다.
DATE_TYPED_EXPIRE_TABLES: frozenset[str] = frozenset()


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
