"""로컬 개발용 대여소 2,746곳 + 재고 이력 + 예측치를 골드 테이블에 채운다.

collector→gold를 채우는 실제 ETL이 아직 없어서, apps/api를 로컬에서 켜보려면
이 스크립트로 더미 데이터를 직접 넣어야 한다. 실행: `uv run python seed_gold.py`
(ops/postgres/init의 자동 실행 스크립트와는 별개로, 원할 때만 수동으로 돌린다.)

`seed_data/stations_seoul.json`은 de-project 레포의 bike_station_hist 컬렉터가
2026-08-13 00~14시에 실제로 수집한 서울 전역 재고 스냅샷(로컬 MinIO bronze/silver)
에서 만들었다. 큐레이션 없이 구/주소 매핑에 성공한 전체 2,746곳을 그대로 썼다
(임의로 골라서 특정 패턴에 편향되는 걸 피하기 위해).

이 시드의 "현재 시각"을 관측 구간 중간인 05시로 잡고, 00~05시(6개 관측)는 그대로
stock_history로, 05~14시(9개 관측)는 demand_profile로 썼다 — 즉 미래 예측치도
모델이 만든 값이 아니라 **실제로 그 이후 시간에 관측된 진짜 재고 변화**다.
predicted_rent_cnt/predicted_return_cnt는 실측값 자체가 아니라, `enrich_forecast_points`
(현재재고 + 반납 - 대여를 누적하는 방식)를 거꾸로 풀어서 "이 값을 넣으면 정확히
그 시각의 실측 재고가 나온다"로 역산한 값이다. 대여/반납 각각의 실제 건수는 알 수
없어서(재고 순증감만 앎), 한 시간 안에 순감소면 전부 대여로, 순증가면 전부 반납으로
몰아넣는 단순화를 했다 — 재고 곡선 자체는 실측과 동일하지만, 대여/반납 개별 값은
근사치다.
"""

import json
from datetime import timedelta
from pathlib import Path

from core.db import get_connection

from queries import now_utc

STATIONS = json.loads((Path(__file__).parent / "seed_data" / "stations_seoul.json").read_text(encoding="utf-8"))


def seed() -> None:
    """STATIONS를 골드 테이블에 채운다. 몇 번을 다시 실행해도 결과가 같도록,
    대상 대여소의 기존 재고 이력·예측치를 먼저 지우고 새로 넣는다."""
    now = now_utc()
    sta_ids = [station["sta_id"] for station in STATIONS]

    stock_rows = []
    forecast_rows = []
    for station in STATIONS:
        for minutes_ago, parking_bike_tot_cnt in station["stock_history"]:
            stock_rows.append((station["sta_id"], now - timedelta(minutes=minutes_ago), parking_bike_tot_cnt))

        for hour, (predicted_rent_cnt, predicted_return_cnt) in enumerate(station["demand_profile"], start=1):
            forecast_rows.append(
                (station["sta_id"], now + timedelta(hours=hour), predicted_rent_cnt, predicted_return_cnt, now)
            )

    with get_connection() as conn, conn.cursor() as cur:
        # 이전에 다른 STATIONS 구성으로 시드를 돌린 적이 있으면, 지금 목록에 없는
        # sta_id가 stations 테이블에 잔여물로 남아있을 수 있다(예: 예전 33곳짜리
        # 시드의 흔적). 그런 것도 같이 지운다.
        cur.execute("DELETE FROM forecast_points WHERE sta_id != ALL(%s)", (sta_ids,))
        cur.execute("DELETE FROM station_stock WHERE sta_id != ALL(%s)", (sta_ids,))
        cur.execute("DELETE FROM stations WHERE sta_id != ALL(%s)", (sta_ids,))

        cur.execute("DELETE FROM forecast_points WHERE sta_id = ANY(%s)", (sta_ids,))
        cur.execute("DELETE FROM station_stock WHERE sta_id = ANY(%s)", (sta_ids,))

        cur.executemany(
            """
            INSERT INTO stations (sta_id, sta_nm, gu, sta_addr, lat, lon, hold_cnt)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sta_id) DO UPDATE SET
                sta_nm = EXCLUDED.sta_nm,
                gu = EXCLUDED.gu,
                sta_addr = EXCLUDED.sta_addr,
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                hold_cnt = EXCLUDED.hold_cnt
            """,
            [
                (s["sta_id"], s["sta_nm"], s["gu"], s["sta_addr"], s["lat"], s["lon"], s["hold_cnt"])
                for s in STATIONS
            ],
        )

        cur.executemany(
            "INSERT INTO station_stock (sta_id, observed_at, parking_bike_tot_cnt) VALUES (%s, %s, %s)",
            stock_rows,
        )

        cur.executemany(
            """
            INSERT INTO forecast_points
                (sta_id, predicted_dttm, predicted_rent_cnt, predicted_return_cnt, batch_run_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (sta_id, predicted_dttm) DO UPDATE SET
                predicted_rent_cnt = EXCLUDED.predicted_rent_cnt,
                predicted_return_cnt = EXCLUDED.predicted_return_cnt,
                batch_run_at = EXCLUDED.batch_run_at
            """,
            forecast_rows,
        )

    print(f"seeded {len(STATIONS)} stations")


if __name__ == "__main__":
    seed()
