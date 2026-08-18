from core.forecast import enrich_forecast_points
from core.regions import DISPATCH_CENTERS, nearest_region
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

import queries
from schemas import (
    Alert,
    DispatchCenter,
    ForecastResponse,
    Route,
    StationDetail,
    StationSummary,
    StatusResponse,
)

app = FastAPI(title="GangnamguUmBokDong API")

# 개발 단계 설정. 실배포 시 지통실 프론트 도메인으로 좁혀야 한다.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


def _shared_rate(row: dict) -> float:
    """현재 거치율(재고/정원)을 계산한다."""
    return round(row["parking_bike_tot_cnt"] / row["hold_cnt"], 2)


@app.get("/stations", response_model=list[StationSummary])
def list_stations() -> list[dict]:
    """전체 대여소의 마스터 정보 + 현재 재고를 반환한다."""
    return [
        {**row, "shared_rate": _shared_rate(row), "region": nearest_region(row["lat"], row["lon"])}
        for row in queries.fetch_stations()
    ]


@app.get("/stations/{sta_id}", response_model=StationDetail)
def get_station(sta_id: str) -> dict:
    """대여소 하나의 상세 정보를 반환한다. 없으면 404."""
    row = queries.fetch_station(sta_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"station {sta_id} not found")
    return {**row, "shared_rate": _shared_rate(row), "region": nearest_region(row["lat"], row["lon"])}


@app.get("/stations/{sta_id}/forecast", response_model=ForecastResponse)
def get_forecast(sta_id: str) -> dict:
    """대여소의 대여·반납 예측과 예측 재고를 시간순으로 반환한다. 없으면 404."""
    station = queries.fetch_station(sta_id)
    if station is None:
        raise HTTPException(status_code=404, detail=f"station {sta_id} not found")

    now = queries.now_utc()
    raw_points = queries.fetch_forecast_points(sta_id, now)
    points = enrich_forecast_points(station["parking_bike_tot_cnt"], station["hold_cnt"], raw_points)
    return {
        "sta_id": sta_id,
        "base_dttm": queries.fetch_batch_run_at(now),
        "points": points,
        # 예측 변동 사유(문화행사·날씨 등 텍스트 설명)를 만들어내는 파이프라인은
        # 아직 없다. 생기면 여기서 채운다.
        "reasons": [],
    }


@app.get("/regions", response_model=list[DispatchCenter])
def list_regions() -> list[dict]:
    """지역센터(권역) 목록과 좌표를 반환한다. 프론트가 권역 경계(보로노이)를
    그리려면 대여소 배정에 쓰인 것과 같은 좌표를 알아야 하므로, 여기 하나
    (core.regions)만 출처로 둔다."""
    return [{"region": name, "lat": lat, "lon": lon} for name, lat, lon in DISPATCH_CENTERS]


@app.get("/status", response_model=StatusResponse)
def get_status() -> dict:
    """가장 최근 예측 배치 기준 시각을 반환한다."""
    return {"base_dttm": queries.fetch_batch_run_at(queries.now_utc())}


@app.get("/alerts", response_model=list[Alert])
def list_alerts() -> list[dict]:
    """전체 대여소의 재배치 우선순위 알림을 urgency_score 내림차순으로 반환한다.

    urgency_score는 더 이상 요청마다 계산하지 않는다 — 5분 배치(rebalance/urgency.py)가
    미리 계산해 station_urgency 테이블에 적재해두고, 여기서는 그 결과만 조회한다.
    """
    alerts = queries.fetch_alerts()
    return [
        {
            "sta_id": row["sta_id"],
            "sta_nm": row["sta_nm"],
            "action_type": row["action_type"],
            "urgency_score": row["urgency_score"],
            "minutes_until_critical": row["minutes_until_critical"],
            "region": nearest_region(row["lat"], row["lon"]),
        }
        for row in alerts
    ]


@app.get("/routes", response_model=list[Route])
def list_routes(region: str | None = None, status: str | None = None) -> list[dict]:
    """재배치 라우트 목록을 스톱과 함께 반환한다. region/status로 필터링 가능."""
    return queries.fetch_routes(region, status)


@app.get("/routes/{route_id}", response_model=Route)
def get_route(route_id: str) -> dict:
    """라우트 하나를 스톱과 함께 반환한다. 없으면 404."""
    route = queries.fetch_route(route_id)
    if route is None:
        raise HTTPException(status_code=404, detail=f"route {route_id} not found")
    return route


@app.post("/routes/{route_id}/dispatch", response_model=Route)
def dispatch_route(route_id: str) -> dict:
    """운영자가 라우트 실행을 선택했을 때 proposed -> dispatched로 전이한다.
    없으면 404, proposed 상태가 아니면 409."""
    result = queries.dispatch_route(route_id, queries.now_utc())
    if result == "not_found":
        raise HTTPException(status_code=404, detail=f"route {route_id} not found")
    if result == "wrong_status":
        raise HTTPException(status_code=409, detail=f"route {route_id} is not in proposed status")
    return queries.fetch_route(route_id)


@app.post("/routes/{route_id}/complete", response_model=Route)
def complete_route(route_id: str) -> dict:
    """운영자가 실행 완료를 표시했을 때 dispatched -> completed로 전이한다.
    없으면 404, dispatched 상태가 아니면 409."""
    result = queries.complete_route(route_id, queries.now_utc())
    if result == "not_found":
        raise HTTPException(status_code=404, detail=f"route {route_id} not found")
    if result == "wrong_status":
        raise HTTPException(status_code=409, detail=f"route {route_id} is not in dispatched status")
    return queries.fetch_route(route_id)
