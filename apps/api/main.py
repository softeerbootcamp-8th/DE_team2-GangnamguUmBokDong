import queries
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from schemas import (
    Alert,
    ForecastResponse,
    StationDetail,
    StationSummary,
    StatusResponse,
)
from scoring import enrich_forecast_points, urgency_score

app = FastAPI(title="GangnamguUmBokDong API")

# 개발 단계 설정. 실배포 시 지통실 프론트 도메인으로 좁혀야 한다.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def _shared_rate(row: dict) -> float:
    """현재 거치율(재고/정원)을 계산한다."""
    return round(row["parking_bike_tot_cnt"] / row["hold_cnt"], 2)


@app.get("/stations", response_model=list[StationSummary])
def list_stations() -> list[dict]:
    """전체 대여소의 마스터 정보 + 현재 재고를 반환한다."""
    return [{**row, "shared_rate": _shared_rate(row)} for row in queries.fetch_stations()]


@app.get("/stations/{sta_id}", response_model=StationDetail)
def get_station(sta_id: int) -> dict:
    """대여소 하나의 상세 정보를 반환한다. 없으면 404."""
    row = queries.fetch_station(sta_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"station {sta_id} not found")
    return {**row, "shared_rate": _shared_rate(row)}


@app.get("/stations/{sta_id}/forecast", response_model=ForecastResponse)
def get_forecast(sta_id: int) -> dict:
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


@app.get("/status", response_model=StatusResponse)
def get_status() -> dict:
    """가장 최근 예측 배치 기준 시각을 반환한다."""
    return {"base_dttm": queries.fetch_batch_run_at(queries.now_utc())}


@app.get("/alerts", response_model=list[Alert])
def list_alerts() -> list[dict]:
    """전체 대여소의 재배치 우선순위 알림을 urgency_score 내림차순으로 반환한다.

    대여소마다 재고 이력·예측치를 따로 조회하면 대여소 수만큼 쿼리가 늘어나므로
    (N+1), 두 데이터 다 전체 대여소를 대상으로 한 번씩만 조회해 sta_id별로 나눠 쓴다.
    """
    now = queries.now_utc()
    stations = queries.fetch_stations()
    sta_ids = [station["sta_id"] for station in stations]
    stock_history_by_station = queries.fetch_all_stock_history(sta_ids, now)
    raw_points_by_station = queries.fetch_all_forecast_points(sta_ids, now)

    alerts = []
    for station in stations:
        current = station["parking_bike_tot_cnt"]
        hold_cnt = station["hold_cnt"]
        stock_history = stock_history_by_station[station["sta_id"]]
        raw_points = raw_points_by_station[station["sta_id"]]
        points = enrich_forecast_points(current, hold_cnt, raw_points)

        score, minutes, action_type = urgency_score(current, hold_cnt, stock_history, points, now)
        alerts.append(
            {
                "sta_id": station["sta_id"],
                "sta_nm": station["sta_nm"],
                "action_type": action_type,
                "urgency_score": score,
                "minutes_until_critical": minutes,
            }
        )
    return sorted(alerts, key=lambda a: a["urgency_score"], reverse=True)
