"""백테스트 production release 정책의 단일 식별 계약을 정의한다."""

from __future__ import annotations

from datetime import date
from typing import Any

from gold.rebalance_policy import DEFAULT_REBALANCE_POLICY
from gold.rebalance_route import (
    MAX_STOPS_PER_ROUTE,
    PICKUP_DISPATCH_ASSUMED_SPEED_KMH,
    PICKUP_DISPATCH_SERVICE_MINUTES_PER_STOP,
)

PRODUCTION_POLICY_NAME = "production_route_v4"
PRODUCTION_TARGET_DATES = tuple(date(2025, month, 17) for month in range(3, 13))
PRODUCTION_CENTER_ID = "hangnyeoul"
PRODUCTION_START_HOUR = 6
PRODUCTION_FLEET_SIZE = 3
PRODUCTION_EVALUATION_MINUTES = (60, 120, 180)
PRODUCTION_TICK_MINUTES = 5
PRODUCTION_TRUCK_CAPACITY = 20
PRODUCTION_SPEED_KMH = PICKUP_DISPATCH_ASSUMED_SPEED_KMH
PRODUCTION_SERVICE_MINUTES_PER_STOP = PICKUP_DISPATCH_SERVICE_MINUTES_PER_STOP
PRODUCTION_APPROVAL_DELAY_MINUTES = 0
PRODUCTION_WEATHER_PUBLICATION_LAG_MINUTES = 60
PRODUCTION_POPULATION_LOOKBACK_WEEKS = 4
PRODUCTION_OPERATOR_TIMING_SCENARIOS = (
    "interval_start",
    "interval_midpoint",
    "interval_end",
)
PRODUCTION_MODEL_BUNDLE_SHA256 = (
    "c677e8e192caef85adc7293a26019ea28681199c75bc085ae86702d300bb0afb"
)
PRODUCTION_WEATHER_SHA256 = (
    "fafbedd1157e933fe38c1d0357f1191a48c30ce6ecb5bd2fda5d4c0add9004a5"
)
PRODUCTION_INPUT_PROVENANCE_SHA256_BY_DATE = {
    "2025-03-17": "d32e89245e11728cecef3333644492fae32637c6af8522cd9c8ca1ec433d6ba4",
    "2025-04-17": "5249d04d9a5ac616709494111765aa4c486fa73f05c8f64988cb16006c99d4ab",
    "2025-05-17": "dff232ccd391edfe4490eed990e98144666dcac657592aeb2bcf2d1a14839ae0",
    "2025-06-17": "19b2d0fd4a763063f5fa086bdc3d28a0bd905bcc1dcf2cef38b26646a37730f6",
    "2025-07-17": "d9f5a37351628ffb9fa723211cb3cbcec02b18eea04ce97cdadf57f620b29868",
    "2025-08-17": "92732889294d82ae3862e27a417acd9a6012fdfb5eb67062359ec8dfdb7f4a23",
    "2025-09-17": "8ec6e100e7fb3e8248dde909f3195837bb613a9cdeefde9e71310898ca541343",
    "2025-10-17": "51e781d367a26844dc6008b5e593eacde18e2ffc06f553cb96979c8a6b502c74",
    "2025-11-17": "74864cccee867ca3cad9c87f91af4711a9b076ad4b072db3d1af13377fa87463",
    "2025-12-17": "a52cc0ab62fab047892d1c8e48537c08793d1290eb6fb04f64712f3c82f8cbf5",
}
PRODUCTION_STATION_SURFACE_SHA256_BY_DATE = {
    "2025-03-17": "17c2c5b98728d932bb6460db39c6ca2189629d773a3d86c7eb5bc918282a74e4",
    "2025-04-17": "17c2c5b98728d932bb6460db39c6ca2189629d773a3d86c7eb5bc918282a74e4",
    "2025-05-17": "394df2124f4395f7bc772ac82895fb3ecc6f4e81865fa0a8f659f29d32485f19",
    "2025-06-17": "fb34af1b0ea10c67030fced70af9db5016649548562596591be8b8873213e843",
    "2025-07-17": "fb34af1b0ea10c67030fced70af9db5016649548562596591be8b8873213e843",
    "2025-08-17": "30e75ad4bb0d334609c78ca303a88a058cc3b8446e9271adf45ae7d8a9ae6ef2",
    "2025-09-17": "8f5b8ef0cb6ffb8f85f48ab035c56f8ed2715506ccc7f237bbc07378790410a4",
    "2025-10-17": "8f5b8ef0cb6ffb8f85f48ab035c56f8ed2715506ccc7f237bbc07378790410a4",
    "2025-11-17": "88a6edbf7222a241af6eb324fffffa72f91ab554a28bfc962f62096a97f0118b",
    "2025-12-17": "3d8a49f5cb8ec13199047f4007e21b51bd70698c0d761d05461861e193057932",
}
PRODUCTION_STATION_COUNT_BY_DATE = {
    "2025-03-17": 262,
    "2025-04-17": 262,
    "2025-05-17": 263,
    "2025-06-17": 264,
    "2025-07-17": 264,
    "2025-08-17": 265,
    "2025-09-17": 266,
    "2025-10-17": 266,
    "2025-11-17": 267,
    "2025-12-17": 267,
}


def production_policy_configuration() -> dict[str, Any]:
    """배포 정책과 경로 작업 상한을 포함한 exact audit 설정을 반환한다."""
    return {
        **DEFAULT_REBALANCE_POLICY.audit_document(),
        "max_stops_per_route": MAX_STOPS_PER_ROUTE,
    }


def production_evidence_scope() -> dict[str, Any]:
    """Production release에 허용된 데이터·운영·모델 근거 범위를 반환한다."""
    return {
        "target_dates": [target.isoformat() for target in PRODUCTION_TARGET_DATES],
        "result_count": len(PRODUCTION_TARGET_DATES),
        "center_id": PRODUCTION_CENTER_ID,
        "start_hour": PRODUCTION_START_HOUR,
        "evaluation_minutes": list(PRODUCTION_EVALUATION_MINUTES),
        "operation_contract": {
            "tick_minutes": PRODUCTION_TICK_MINUTES,
            "fleet_size": PRODUCTION_FLEET_SIZE,
            "truck_capacity": PRODUCTION_TRUCK_CAPACITY,
            "speed_kmh": PRODUCTION_SPEED_KMH,
            "service_minutes_per_stop": PRODUCTION_SERVICE_MINUTES_PER_STOP,
            "approval_delay_minutes": PRODUCTION_APPROVAL_DELAY_MINUTES,
            "weather_publication_lag_minutes": (
                PRODUCTION_WEATHER_PUBLICATION_LAG_MINUTES
            ),
            "population_lookback_weeks": PRODUCTION_POPULATION_LOOKBACK_WEEKS,
            "operator_timing_scenarios": list(
                PRODUCTION_OPERATOR_TIMING_SCENARIOS
            ),
        },
        "model_bundle_sha256": PRODUCTION_MODEL_BUNDLE_SHA256,
        "weather_csv_sha256": PRODUCTION_WEATHER_SHA256,
        "input_provenance_sha256_by_date": dict(
            PRODUCTION_INPUT_PROVENANCE_SHA256_BY_DATE
        ),
        "station_surface_sha256_by_date": dict(
            PRODUCTION_STATION_SURFACE_SHA256_BY_DATE
        ),
        "station_count_by_date": dict(PRODUCTION_STATION_COUNT_BY_DATE),
    }
