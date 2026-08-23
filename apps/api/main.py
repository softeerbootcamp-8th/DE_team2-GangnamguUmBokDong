"""Gold PostGIS 대시보드 API endpoint를 제공한다."""

from typing import Literal
from uuid import UUID

import queries
from core.db import fetch_one
from core.forecast import enrich_forecast_points
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from schemas import (
    Alert,
    DispatchCenter,
    EventsResponse,
    ForecastResponse,
    Route,
    StationDetail,
    StationSummary,
    StatusResponse,
    WeatherResponse,
)

RouteStatusFilter = Literal["proposed", "dispatched", "completed", "cancelled"]

app = FastAPI(title="GangnamguUmBokDong API")

# 개발 단계 설정. 실배포 시 지통실 프론트 도메인으로 좁혀야 한다.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.get("/healthz")
def healthz() -> dict:
    """프로세스 생존만 알린다.

    DB를 일부러 조회하지 않는다 — 컨테이너 healthcheck와 리버스 프록시의 업스트림
    판정에 쓰이므로, RDS 순단이나 파이프라인 지연이 프로세스 재시작으로 번지면 안 된다.
    데이터가 준비됐는지는 `/status`, DB 연결 여부는 `/readyz`가 따로 답한다.
    """
    return {"status": "ok"}


@app.get("/readyz")
def readyz() -> dict:
    """DB에 실제로 연결되는지까지 확인한다.

    배포 직후 `DATABASE_URL`과 RDS 접근이 되는지 보는 용도다. 파이프라인이 아직
    한 번도 안 돌아 데이터가 비어 있어도 여기서는 200이 나온다 — 신선도 판정은
    `/status`의 몫이라 의도적으로 분리했다.

    raises:
        HTTPException: DB에 연결할 수 없을 때 503
    """
    try:
        fetch_one("SELECT 1 AS ok")
    except Exception as exc:
        # 연결 실패 원인(자격증명·네트워크·DATABASE_URL 누락)과 무관하게 not ready다.
        raise HTTPException(status_code=503, detail="database_unavailable") from exc
    return {"status": "ready"}


def _shared_rate(row: dict) -> float:
    """현재 거치율을 재고와 정원으로 계산한다."""
    return round(row["parking_bike_tot_cnt"] / row["hold_cnt"], 2)


@app.get("/stations", response_model=list[StationSummary])
def list_stations() -> list[dict]:
    """active station과 같은 anchor의 신선한 현재 재고를 반환한다."""
    now = queries.now_utc()
    return [
        {**row, "shared_rate": _shared_rate(row)} for row in queries.fetch_stations(now)
    ]


@app.get("/stations/{sta_id}", response_model=StationDetail)
def get_station(sta_id: str) -> dict:
    """신선한 현재 재고가 있는 active station 상세를 반환한다."""
    row = queries.fetch_station(sta_id, queries.now_utc())
    if row is None:
        raise HTTPException(status_code=404, detail=f"station {sta_id} not found")
    return {**row, "shared_rate": _shared_rate(row)}


@app.get("/stations/{sta_id}/forecast", response_model=ForecastResponse)
def get_forecast(sta_id: str) -> dict:
    """같은 anchor의 재고로 미래 12시간 수요와 예측 재고를 반환한다."""
    result = queries.fetch_forecast(sta_id, queries.now_utc())
    if result.state is queries.ForecastState.STATION_NOT_FOUND:
        raise HTTPException(status_code=404, detail=f"station {sta_id} not found")
    if result.state is queries.ForecastState.FORECAST_NOT_AVAILABLE:
        raise HTTPException(status_code=404, detail="forecast_not_available")
    if result.state is queries.ForecastState.FORECAST_NOT_READY:
        raise HTTPException(status_code=503, detail="forecast_not_ready")
    if result.state is queries.ForecastState.STOCK_NOT_ALIGNED:
        raise HTTPException(status_code=503, detail="stock_forecast_not_aligned")

    if result.station is None or result.base_dttm is None:
        raise RuntimeError("ready forecast 결과에 station 또는 base_dttm이 없습니다.")
    points = enrich_forecast_points(
        result.station["parking_bike_tot_cnt"],
        result.station["hold_cnt"],
        list(result.points),
    )
    return {
        "sta_id": sta_id,
        "base_dttm": result.base_dttm,
        "points": points,
    }


@app.get("/stations/{sta_id}/events", response_model=EventsResponse)
def get_station_events(sta_id: str) -> dict:
    """active station Point 주변의 신선한 현재·예정 행사를 반환한다."""
    events = queries.fetch_nearby_events(sta_id, queries.now_utc())
    if events is None:
        raise HTTPException(status_code=404, detail=f"station {sta_id} not found")
    return {"radius_km": queries.NEARBY_EVENT_RADIUS_KM, "events": events}


@app.get("/stations/{sta_id}/weather", response_model=WeatherResponse)
def get_station_weather(
    sta_id: str,
    hours: int = Query(default=12, ge=12, le=12),
) -> dict:
    """active station 격자의 fresh한 미래 12개 정시 날씨를 반환한다."""
    result = queries.fetch_weather(sta_id, queries.now_utc(), hours)
    if result.state is queries.WeatherState.STATION_NOT_FOUND:
        raise HTTPException(status_code=404, detail=f"station {sta_id} not found")
    if result.state is queries.WeatherState.WEATHER_NOT_READY:
        raise HTTPException(status_code=503, detail="weather_not_ready")
    return {"sta_id": sta_id, "points": list(result.points)}


@app.get("/regions", response_model=list[DispatchCenter])
def list_regions() -> list[dict]:
    """Gold에 게시된 active dispatch center 목록을 반환한다."""
    return queries.fetch_regions()


@app.get("/status", response_model=StatusResponse)
def get_status() -> dict:
    """fresh한 공통 demand publication 기준 시각을 반환한다."""
    base_dttm = queries.fetch_status_base_dttm(queries.now_utc())
    if base_dttm is None:
        raise HTTPException(status_code=503, detail="forecast_not_ready")
    return {"base_dttm": base_dttm}


@app.get("/alerts", response_model=list[Alert])
def list_alerts() -> list[dict]:
    """같은 anchor와 correction 순서를 만족하는 긴급도 목록을 반환한다."""
    return queries.fetch_alerts(queries.now_utc())


@app.get("/routes", response_model=list[Route])
def list_routes(
    region: str | None = None,
    status: RouteStatusFilter | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[dict]:
    """필터와 bounded pagination을 적용한 route aggregate 목록을 반환한다."""
    return queries.fetch_routes(region, status, limit, offset)


@app.get("/routes/{route_id}", response_model=Route)
def get_route(route_id: UUID) -> dict:
    """UUID route header와 stop을 같은 snapshot으로 반환한다."""
    route = queries.fetch_route(route_id)
    if route is None:
        raise HTTPException(status_code=404, detail=f"route {route_id} not found")
    return route


def _route_transition_response(
    route_id: UUID,
    result: dict | queries.RouteTransitionResult,
    expected_status: str,
) -> dict:
    """DB 독립 route 전이 결과를 HTTP 오류 또는 응답으로 변환한다."""
    if result is queries.RouteTransitionResult.NOT_FOUND:
        raise HTTPException(status_code=404, detail=f"route {route_id} not found")
    if result is queries.RouteTransitionResult.WRONG_STATUS:
        raise HTTPException(
            status_code=409,
            detail=f"route {route_id} is not in {expected_status} status",
        )
    if result is queries.RouteTransitionResult.STATION_CONFLICT:
        raise HTTPException(status_code=409, detail="route_station_conflict")
    if result is queries.RouteTransitionResult.CONSTRAINT_CONFLICT:
        raise HTTPException(status_code=409, detail="route_transition_conflict")
    return result


@app.post("/routes/{route_id}/dispatch", response_model=Route)
def dispatch_route(route_id: UUID) -> dict:
    """route를 proposed에서 dispatched로 guarded 전이한다."""
    result = queries.dispatch_route(route_id, queries.now_utc())
    return _route_transition_response(route_id, result, "proposed")


@app.post("/routes/{route_id}/complete", response_model=Route)
def complete_route(route_id: UUID) -> dict:
    """route를 dispatched에서 completed로 guarded 전이한다."""
    result = queries.complete_route(route_id, queries.now_utc())
    return _route_transition_response(route_id, result, "dispatched")


@app.post("/routes/{route_id}/cancel", response_model=Route)
def cancel_route(route_id: UUID) -> dict:
    """route를 dispatched에서 cancelled로 guarded 전이한다."""
    result = queries.cancel_route(route_id, queries.now_utc())
    return _route_transition_response(route_id, result, "dispatched")
