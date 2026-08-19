from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel

ActionType = Literal["supply_needed", "retrieval_needed", "normal"]


class StationSummary(BaseModel):
    sta_id: str
    sta_nm: str
    gu: str
    lat: float
    lon: float
    hold_cnt: int
    parking_bike_tot_cnt: int
    shared_rate: float
    region: str
    base_dttm: datetime


class StationDetail(StationSummary):
    sta_addr: str


class ForecastPoint(BaseModel):
    predicted_dttm: datetime
    predicted_rent_cnt: int
    predicted_return_cnt: int
    predicted_bikes: int
    action_type: ActionType


class ForecastResponse(BaseModel):
    sta_id: str
    base_dttm: datetime
    points: list[ForecastPoint]
    reasons: list[str]


class Alert(BaseModel):
    sta_id: str
    sta_nm: str
    action_type: ActionType
    urgency_score: float
    minutes_until_critical: int
    region: str


class StatusResponse(BaseModel):
    base_dttm: datetime


class DispatchCenter(BaseModel):
    region: str
    lat: float
    lon: float


RouteStatus = Literal["proposed", "dispatched", "completed", "cancelled"]
RouteAction = Literal["pickup", "dropoff"]


class RouteStop(BaseModel):
    visit_order: int
    sta_id: str
    sta_nm: str
    lat: float
    lon: float
    action: RouteAction
    bike_cnt: int


class Route(BaseModel):
    route_id: str
    region: str
    status: RouteStatus
    proposed_at: datetime
    dispatched_at: datetime | None
    completed_at: datetime | None
    stops: list[RouteStop]


class CulturalEvent(BaseModel):
    event_id: str
    title: str
    category: str | None
    place: str | None
    start_date: date | None
    end_date: date | None
    is_free: str | None
    lat: float
    lon: float
    distance_km: float


class EventsResponse(BaseModel):
    # 프론트가 "적용 면적"(검색 반경) 원을 지도에 그릴 때 이 값을 그대로 쓴다 —
    # queries.NEARBY_EVENT_RADIUS_KM을 프론트에 따로 하드코딩해서 값이 어긋나는
    # 일이 없게, 응답에 실어 보낸다.
    radius_km: float
    events: list[CulturalEvent]
