# libs/core

모든 서비스 및 파이프라인 모듈(`loader`, `normalizer`, `nowcaster`, `collector`, `ml` 등)에서 공통으로 사용하는 **PostgreSQL 데이터베이스 및 S3/MinIO 제네릭 입출력 공용 라이브러리**입니다. 여러 서비스가 똑같이 필요로 하는 소규모 도메인 유틸(권역 배정, urgency_score 계산에 쓰이는 파생값·튜닝 상수)도 여기 둡니다.

---

## 1. 모듈 구성 및 파일별 역할

| 파일 | 역할 | 주요 제공 인터페이스 |
|---|---|---|
| `db.py` | PostgreSQL(psycopg3) 연결 관리 및 기본 쿼리 실행 | `get_connection()`, `execute()`, `fetch_all()`, `fetch_one()` |
| `upsert.py` | PostgreSQL 대량 Upsert (`INSERT ON CONFLICT DO UPDATE`) 쿼리 실행 | `upsert(conn, table, rows, conflict_cols, update_cols)` |
| `s3.py` | S3 / MinIO 제네릭 I/O (Parquet, JSON, Bytes) 및 배치/스레드 병렬 처리 | `get_object_bytes`, `read_parquet`, `write_parquet`, `read_json`, `write_json`, `list_keys`, `delete_objects` |
| `regions.py` | 대여소를 11개 지역센터 중 최근접으로 배정(apps/api, rebalance 공유) | `DISPATCH_CENTERS`, `nearest_region()` |
| `forecast.py` | 예측 원본치를 누적해 예측 재고·action_type을 계산(apps/api, rebalance 공유) | `enrich_forecast_points()` |
| `scoring_config.py` | urgency_score/enrich_forecast_points 계산에 쓰이는 튜닝 상수 | `SUPPLY_LOW_STOCK_RATIO`, `SEVERITY_SCALE` 등 |

---

## 2. 주요 기능 및 사용 가이드

### ① PostgreSQL 연결 및 쿼리 (`db.py`)
- `DATABASE_URL` 환경변수를 기반으로 연결을 생성합니다.
- `with get_connection() as conn:` 컨텍스트 매니저를 지원하여 트랜잭션 정상 종료 시 자동 커밋 및 예외 시 자동 롤백을 보장합니다.

```python
from core.db import fetch_all, get_connection

# 1. 단일/다중 행 조회 (dict_row 기반 dict 반환)
rows = fetch_all("SELECT * FROM stations WHERE gu = %(gu)s", {"gu": "강남구"})

# 2. 커스텀 트랜잭션 관리
with get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("UPDATE config SET value = %s WHERE key = %s", ("active", "status"))
    conn.commit()
```

### ② 대량 Upsert 실행기 (`upsert.py`)
- `psycopg3`의 `cur.executemany()`를 활용하여 다수의 레코드 딕셔너리를 PostgreSQL의 `ON CONFLICT (...) DO UPDATE SET ...` 구문으로 고속 일괄 적재합니다.
- `update_cols`가 비어있는 경우 안전하게 `DO NOTHING`으로 동작합니다.

```python
from core.upsert import upsert

rows = [
    {"sta_id": "101", "observed_at": "2026-08-16 14:05:00", "parking_bike_tot_cnt": 15},
    {"sta_id": "102", "observed_at": "2026-08-16 14:05:00", "parking_bike_tot_cnt": 8},
]

# (sta_id, observed_at) 복합키 충돌 시 parking_bike_tot_cnt 갱신
upsert(
    conn=conn,
    table="station_stock",
    rows=rows,
    conflict_cols=["sta_id", "observed_at"],
    update_cols=["parking_bike_tot_cnt"],
)
```

### ③ S3 / MinIO 제네릭 I/O (`s3.py`)
- **환경변수**: `S3_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_BUCKET`
- **Parquet 지원**: 단일 파일뿐만 아니라 S3 상의 파티션 디렉터리(`prefix/`) 하위 다중 파트 파일들을 `ThreadPoolExecutor`로 병렬 로딩하여 단일 DataFrame / Table로 결합합니다.
- **배치 삭제**: `delete_objects` 호출 시 AWS S3 한도(1,000건)에 맞추어 자동 청크 분할 삭제를 수행합니다.

```python
from core.s3 import read_parquet, write_parquet

# 1. Parquet 읽기 (Pandas DataFrame 반환)
df = read_parquet("silver/bike_station_realtime/dt=2026-08-16/hh=14/1405.parquet")

# 2. Parquet 저장
write_parquet(df, "silver/processed_data/output.parquet")
```
