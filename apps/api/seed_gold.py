"""로컬 개발용 대여소 10곳 + 재고 이력 + 예측치를 골드 테이블에 채운다.

collector→gold를 채우는 실제 ETL이 아직 없어서, apps/api를 로컬에서 켜보려면
이 스크립트로 더미 데이터를 직접 넣어야 한다. 실행: `uv run python seed_gold.py`
(ops/postgres/init의 자동 실행 스크립트와는 별개로, 원할 때만 수동으로 돌린다.)
"""

from datetime import timedelta

from core.db import get_connection

from queries import now_utc

# 대여소 마스터 + 재고 이력(stock_history: (몇 분 전, 그때 재고))
# + 예측 원본치(demand_profile: 1~12시간 뒤 (대여량, 반납량)).
# 기존 프로토타입(GangnamguUmBokDong/client/backend/mock_data.py)의 시나리오를 그대로 옮겼다.
STATIONS = [
    {
        "sta_id": 101,
        "sta_nm": "강남역 2번 출구",
        "gu": "강남구",
        "sta_addr": "서울 강남구 강남대로 396",
        "lat": 37.4979,
        "lon": 127.0276,
        "hold_cnt": 20,
        "stock_history": [(20, 0), (15, 0), (10, 0), (5, 0), (0, 0)],
        "demand_profile": [(14, 1)] + [(2, 2)] * 11,
    },
    {
        "sta_id": 102,
        "sta_nm": "역삼동 GS타워 앞",
        "gu": "강남구",
        "sta_addr": "서울 강남구 논현로 508",
        "lat": 37.5006,
        "lon": 127.0365,
        "hold_cnt": 20,
        "stock_history": [(20, 0), (15, 0), (10, 0), (5, 0), (0, 0)],
        "demand_profile": [(2, 16)] + [(2, 2)] * 11,
    },
    {
        "sta_id": 103,
        "sta_nm": "삼성역 1번 출구",
        "gu": "강남구",
        "sta_addr": "서울 강남구 영동대로 513",
        "lat": 37.5089,
        "lon": 127.0632,
        "hold_cnt": 25,
        "stock_history": [(20, 0), (15, 0), (10, 0), (5, 0), (0, 0)],
        "demand_profile": [(2, 1)] + [(2, 2)] * 11,
    },
    {
        "sta_id": 104,
        "sta_nm": "청담사거리",
        "gu": "강남구",
        "sta_addr": "서울 강남구 압구정로 411",
        "lat": 37.5251,
        "lon": 127.0473,
        "hold_cnt": 20,
        "stock_history": [(20, 15), (15, 14), (10, 12), (5, 11), (0, 10)],
        "demand_profile": [(8, 2)] + [(3, 2)] * 11,
    },
    {
        "sta_id": 105,
        "sta_nm": "신논현역 6번 출구",
        "gu": "강남구",
        "sta_addr": "서울 강남구 강남대로 476",
        "lat": 37.5045,
        "lon": 127.0246,
        "hold_cnt": 20,
        "stock_history": [(20, 15), (15, 15), (10, 15), (5, 15), (0, 15)],
        "demand_profile": [(5, 3), (35, 2)] + [(2, 2)] * 10,
    },
    {
        "sta_id": 106,
        "sta_nm": "논현동 학동사거리",
        "gu": "강남구",
        "sta_addr": "서울 강남구 학동로 401",
        "lat": 37.5115,
        "lon": 127.0286,
        "hold_cnt": 20,
        "stock_history": [(20, 15), (15, 15), (10, 15), (5, 15), (0, 15)],
        "demand_profile": [(2, 1)] * 7 + [(11, 1)] + [(2, 2)] * 4,
    },
    {
        "sta_id": 107,
        "sta_nm": "도곡동 타워팰리스 사거리",
        "gu": "강남구",
        "sta_addr": "서울 강남구 도곡로 401",
        "lat": 37.4907,
        "lon": 127.0403,
        "hold_cnt": 20,
        "stock_history": [(20, 12), (15, 12), (10, 12), (5, 12), (0, 12)],
        "demand_profile": [(2, 2)] * 12,
    },
    {
        "sta_id": 108,
        "sta_nm": "대치동 은마사거리",
        "gu": "강남구",
        "sta_addr": "서울 강남구 삼성로 212",
        "lat": 37.4945,
        "lon": 127.0557,
        "hold_cnt": 20,
        "stock_history": [(20, 20), (15, 22), (10, 24), (5, 25), (0, 26)],
        "demand_profile": [(1, 3)] + [(2, 2)] * 11,
    },
    {
        "sta_id": 109,
        "sta_nm": "압구정로데오역",
        "gu": "강남구",
        "sta_addr": "서울 강남구 언주로 861",
        "lat": 37.5274,
        "lon": 127.0403,
        "hold_cnt": 20,
        "stock_history": [(20, 12), (15, 12), (10, 12), (5, 12), (0, 12)],
        "demand_profile": [(2, 3), (1, 4), (1, 8)] + [(2, 2)] * 9,
    },
    {
        "sta_id": 110,
        "sta_nm": "코엑스 대성홀 앞",
        "gu": "강남구",
        "sta_addr": "서울 강남구 영동대로 513",
        "lat": 37.5115,
        "lon": 127.0605,
        "hold_cnt": 15,
        "stock_history": [(20, 15), (15, 20), (10, 24), (5, 27), (0, 30)],
        "demand_profile": [(1, 3)] + [(2, 2)] * 11,
    },
]


def seed() -> None:
    """STATIONS를 골드 테이블에 채운다. 몇 번을 다시 실행해도 결과가 같도록,
    대상 대여소의 기존 재고 이력·예측치를 먼저 지우고 새로 넣는다."""
    now = now_utc()
    sta_ids = [station["sta_id"] for station in STATIONS]

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM forecast_points WHERE sta_id = ANY(%s)", (sta_ids,))
        cur.execute("DELETE FROM station_stock WHERE sta_id = ANY(%s)", (sta_ids,))

        for station in STATIONS:
            cur.execute(
                """
                INSERT INTO stations (sta_id, sta_nm, gu, sta_addr, lat, lon, hold_cnt)
                VALUES (%(sta_id)s, %(sta_nm)s, %(gu)s, %(sta_addr)s, %(lat)s, %(lon)s, %(hold_cnt)s)
                ON CONFLICT (sta_id) DO UPDATE SET
                    sta_nm = EXCLUDED.sta_nm,
                    gu = EXCLUDED.gu,
                    sta_addr = EXCLUDED.sta_addr,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon,
                    hold_cnt = EXCLUDED.hold_cnt
                """,
                station,
            )

            for minutes_ago, parking_bike_tot_cnt in station["stock_history"]:
                cur.execute(
                    """
                    INSERT INTO station_stock (sta_id, observed_at, parking_bike_tot_cnt)
                    VALUES (%s, %s, %s)
                    """,
                    (station["sta_id"], now - timedelta(minutes=minutes_ago), parking_bike_tot_cnt),
                )

            for hour, (predicted_rent_cnt, predicted_return_cnt) in enumerate(station["demand_profile"], start=1):
                cur.execute(
                    """
                    INSERT INTO forecast_points
                        (sta_id, predicted_dttm, predicted_rent_cnt, predicted_return_cnt, batch_run_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (sta_id, predicted_dttm) DO UPDATE SET
                        predicted_rent_cnt = EXCLUDED.predicted_rent_cnt,
                        predicted_return_cnt = EXCLUDED.predicted_return_cnt,
                        batch_run_at = EXCLUDED.batch_run_at
                    """,
                    (station["sta_id"], now + timedelta(hours=hour), predicted_rent_cnt, predicted_return_cnt, now),
                )

    print(f"seeded {len(STATIONS)} stations")


if __name__ == "__main__":
    seed()
