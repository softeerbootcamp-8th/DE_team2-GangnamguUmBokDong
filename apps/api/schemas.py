"""대시보드 API의 외부 응답 schema를 정의한다."""

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel

ActionType = Literal["supply_needed", "retrieval_needed", "normal"]


class StationSummary(BaseModel):
    """지도와 목록에 제공하는 active station 요약이다."""

    sta_id: str
    sta_nm: str
    lat: float
    lon: float
    hold_cnt: int
    parking_bike_tot_cnt: int
    shared_rate: float
    region: str
    base_dttm: datetime


class StationDetail(StationSummary):
    """주소를 추가한 station 상세 응답이다."""

    sta_addr: str


class ForecastPoint(BaseModel):
    """한 시간 구간의 수요와 누적 예측 재고를 나타낸다."""

    predicted_dttm: datetime
    predicted_rent_cnt: int
    predicted_return_cnt: int
    predicted_bikes: int
    action_type: ActionType


class ForecastResponse(BaseModel):
    """station별 미래 12시간 수요예측 응답이다."""

    sta_id: str
    base_dttm: datetime
    points: list[ForecastPoint]


class Alert(BaseModel):
    """station별 최신 재배치 판단 응답이다."""

    sta_id: str
    sta_nm: str
    action_type: ActionType
    urgency_score: float
    minutes_until_critical: int
    region: str
    base_dttm: datetime
    data_status: Literal["fresh", "stale"]
    age_minutes: float


class StatusResponse(BaseModel):
    """공통 demand publication 기준 시각 응답이다."""

    base_dttm: datetime


class DispatchCenter(BaseModel):
    """active dispatch center의 화면 표시값이다."""

    region: str
    lat: float
    lon: float


RouteStatus = Literal["proposed", "dispatched", "completed", "cancelled"]
RouteAction = Literal["pickup", "dropoff"]


class RouteStop(BaseModel):
    """route의 연속된 방문 순서 하나를 나타낸다."""

    visit_order: int
    sta_id: str
    sta_nm: str
    lat: float
    lon: float
    action: RouteAction
    bike_cnt: int


class Route(BaseModel):
    """header와 stop이 같은 snapshot인 route aggregate다."""

    route_id: str
    region: str
    status: RouteStatus
    proposed_at: datetime
    dispatched_at: datetime | None
    completed_at: datetime | None
    cancelled_at: datetime | None
    dismissed_at: datetime | None
    restored_from_route_id: str | None
    stops: list[RouteStop]


class CulturalEvent(BaseModel):
    """station 주변에 표시하는 현재·예정 행사다."""

    event_id: str
    title: str
    place: str | None
    start_date: date
    end_date: date
    lat: float
    lon: float
    distance_km: float


class EventsResponse(BaseModel):
    """인근 행사 목록과 실제 검색 반경을 함께 반환한다."""

    radius_km: float
    events: list[CulturalEvent]


SkyCondition = Literal["clear", "mostly_cloudy", "cloudy"]
PrecipitationType = Literal[
    "none",
    "rain",
    "rain_snow",
    "snow",
    "shower",
    "raindrop",
    "raindrop_snow_flurry",
    "snow_flurry",
]


class WeatherPoint(BaseModel):
    """한 정시의 선택 완료된 날씨 예보를 나타낸다."""

    forecast_dttm: datetime
    temperature: float
    sky_condition_cd: SkyCondition
    precipitation_type_cd: PrecipitationType
    precipitation_prob: float | None
    precipitation_amount: float | None
    humidity: float | None
    wind_speed: float | None


class WeatherResponse(BaseModel):
    """station별 미래 12개 정시 날씨 응답이다."""

    sta_id: str
    points: list[WeatherPoint]
