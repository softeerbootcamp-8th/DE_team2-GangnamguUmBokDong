# inference — 실행 방법

학습이 끝난 모델(S3 아카이브 또는 챔피언, `training/README.md` 참고)로 예측을
실행하는 세 가지 경로를 제공한다.

| 경로 | 언제 쓰나 | 입력 |
|---|---|---|
| **배치 조회** (`predict_common.py` + `predict_{rental,return}_demand.py`) | 이미 만들어둔 feature 테이블에서 특정 station/기간을 조회 (백테스트, 결과 확인 전용) | station_id, 날짜/시간 범위 |
| **단일/다중 시점 예측** (`predict_single.py`) | 날짜/시각/horizon/날씨/(선택)인구 값을 직접 넣어서 예측·진단하는 계산 엔진 — authority는 아니며, 아래 운영 게시가 내부적으로 이 모듈의 `predict_demand_multi_hour_all_stations()`를 호출한다 | station_id(또는 전체), date, hour, minute, horizon, 날씨(선택), 인구(선택) |
| **운영 게시** (`publication_cli.py` → `publication.py`) | Airflow `realtime_tick*` 계열 DAG(합쳐서 5분마다)가 호출하는 **실제 운영 진입점(정식 authority)** — serving plan이 pin한 입력으로 위 계산 엔진을 돌리고, 검증된 결과를 immutable snapshot으로 게시한다 | serving plan URI + SHA-256 |

설계 배경은 [DESIGN.md](../../docs/ml/inference/DESIGN.md) 참고.

## 세팅

```bash
cd ml/inference
uv sync   # pyproject.toml/uv.lock 기준 .venv 생성 — pandas/numpy/boto3/holidays + ml_core·core(editable) 포함
```

`training`이 먼저 모델을 학습·승격해둬야 한다([training/README.md](../training/README.md)).
로컬 개발은 `.env`의 S3 자격증명으로 MinIO(`make up`)를 거친다.

추론은 챔피언 booster를 로드하기 전에 모델 옆 effective profile의 서빙 피처
계약과 현재 `common_config`를 비교한다. rolling/window/embargo, target horizon,
grid, horizon 수가 다르거나 profile 아티팩트가 없으면 잘못된 의미의 피처로
조용히 예측하지 않고 즉시 실패한다.

**단일 시점 예측을 쓰려면 추가로 fallback 프로필 2개를 한 번 만들어야 한다**
(`feature_engine`이 만든 병합 테이블/생활인구 테이블이 먼저 S3에 있어야 함):

```bash
cd ml
./inference/.venv/bin/python -m inference.build_station_profile      # -> station_hourly_profile.parquet: 정류소×minute×dow×월별 대여/반납 평균/표준편차(lag fallback)
./inference/.venv/bin/python -m inference.build_population_profile   # -> population_hourly_profile.parquet: 격자×hour×dow별 평균 인구(인구 fallback, 월은 안 나눔 — 계절 변동이 작아서)
```

station profile은 활성 모델의 `FEATURE_ENGINEERING_OUTPUT_DIR` 아래에 저장되므로
서로 다른 grid의 모델이 fallback을 덮어쓰지 않는다. 추론 시 profile의 minute
간격이 현재 모델 `GRID_TICK_MINUTES`와 다르거나 logical key가 중복되어 있으면
잘못된 값을 조용히 쓰지 않고 즉시 실패한다. 모델 grid를 바꾼 경우 같은 프로필로
`build_station_profile`도 다시 실행해야 한다.

## 배치 조회 CLI (백테스트 전용)

```bash
# 특정 정류소 + 특정 기간
./inference/.venv/bin/python -m inference.predict_rental_demand --station-id ST-2000 --start-date 2025-06-01 --end-date 2025-06-07

# 반납 모델도 옵션은 동일 (exposure 없음)
./inference/.venv/bin/python -m inference.predict_return_demand --station-id ST-2000 --start-date 2025-06-01 --end-date 2025-06-01
```

이미 만들어진 multi-horizon feature 테이블에서 읽어 채점만 하는 조회용
CLI다 — 실제 서비스가 부르는 진입점이 아니다(그건 아래 `predict_single.py`).

## 단일/다중 시점 예측 (`predict_single.py`, 실제 진입점)

```python
from inference.predict_single import predict_rental_demand, predict_return_demand

predict_rental_demand(
    station_id="ST-2000", date="2025-06-01", hour=8, minute=0, horizon=1,
    temp=22.5, precip=0.0,          # 생략하면 관측/예보 Silver에서 자동 조회
    population=3200,                # 없으면 생략 가능 — 격자 평소 인구로 대체됨
    stockout=False,                 # 없으면 생략 가능 — 실시간 재고에서 자동 조회
)
# -> {'station_id': 'ST-2000', 'date': '2025-06-01', 'hour': 8, 'minute': 0, 'horizon': 1,
#     'pred_mean': ..., 'pred_p10': ..., 'pred_p50': ..., 'pred_p90': ...,
#     'lag_fallback_used': [], 'lag_data_freshness': 1.0,
#     'population_source': 'provided', 'stockout_source': 'provided'}
```

`minute`은 모델 학습 grid와 별도인 운영 계약 `SERVING_TICK_MINUTES=5`의 배수만
유효하다. 모델 feature/target grid가 기본 20분이거나 비교용 5~60분 설정이어도
00/05/10/.../55분에 호출할 수 있고, `minute=7`처럼 운영 주기에 없는 값은
거부한다. 실시간 point-in-time 피처는 요청한 5분 시각을 그대로 사용한다. 다만
실시간 lag가 없어 station profile로 fallback할 때만 같은 날의 직전 모델 anchor로
내림한다(예: 20분 grid에서 17:05/10/15 -> 17:00).
`horizon`(1~`HORIZON_COUNT`, 기본 12)은
"몇 시간 뒤를 물을지"를 그대로 모델 feature로 넘긴다(아래 "여러 horizon
한 번에" 참고) — `predict_return_demand()`는 시그니처가 같지만 `stockout`이
없다(반납은 거치대 상태와 무관하게 항상 성공해서 exposure 보정이 필요 없음).

CLI로도 바로 확인 가능(값을 생략하면 실시간 Silver에서 자동 조회):

```bash
./inference/.venv/bin/python -m inference.predict_single \
  --station-id ST-2000 --date 2025-06-01 --hour 8 --minute 0 --horizon 1
```

날짜/시각/horizon/날씨/인구만 받는 이유(lag/rolling은 내부에서 자동 조회),
2단계 fallback(실시간 히스토리 없으면 → 정류소/격자 평소 패턴), 실시간
데이터 결측·지연 대응, 관측 vs 예보 날씨 분기는 [DESIGN.md](../../docs/ml/inference/DESIGN.md)에
자세히 있다.

## 여러 horizon 한 번에 (재귀 아님 — horizon이 feature)

```python
from inference.predict_single import predict_demand_multi_hour

predict_demand_multi_hour(
    station_id="ST-2000", date="2025-06-01", hour=8,     # "지금"(anchor_ts=T0)
    temp=22.5, precip=0.0,        # 스칼라(전체 horizon 재사용) 또는 길이 n_hours 배열(horizon별 예보)
    population=3200,
    n_hours=12,   # horizon=1..12를 한 번에
)
# -> [{'station_id': ..., 'date': ..., 'hour': ..., 'minute': ..., 'horizon': 1,
#      'rental': {'pred_mean': ..., ..., 'lag_fallback_used': [...], 'lag_data_freshness': ...},
#      'return': {'pred_mean': ...}, 'population_source': ..., 'stockout_source': ...}, ...] (길이 12)
```

CLI는 `--n-hours`만 추가하면 된다: `./inference/.venv/bin/python -m inference.predict_single ... --n-hours 12`.

**재귀적으로 이전 예측값을 다음 스텝 입력에 먹이지 않는다.** lag(직전 실적)는
"지금"(anchor_ts) 기준으로 딱 한 번만 계산하고, "몇 시간 뒤인지"(horizon)를
평범한 입력 feature로 모델에 직접 알려준다 — 그래서 horizon이 커져도 오차가
누적되지 않는다(옛 버전은 재귀 방식이었으나 폐기됨). 날씨/인구/캘린더는
horizon마다 그 target_ts(anchor_ts+(horizon-1)시간) 기준으로 새로 계산된다 —
target_ts가 미래면(horizon>1) 날씨는 예보를 먼저 시도하고 없으면 관측으로
fallback한다.

## 전체 정류소 배치 (`publication.py`가 내부적으로 호출)

운영에서는 Airflow `realtime_tick*` 계열 DAG(합쳐서 5분마다)가 `publication_cli.py` →
`publication.py`를 호출하고, `publication.py`가 아래 함수를 내부적으로 호출한다 —
CLI로 직접 호출하는 것은 수동 진단·백필용이다.

```python
from inference.predict_single import predict_demand_multi_hour_all_stations

outcome = predict_demand_multi_hour_all_stations(
    date="2025-06-01", hour=8, temp=22.5, precip=0.0,
    n_hours=5,
)
# -> {"results": [...], "failed": [...], "expected_count": 2582, "actual_count": 2581}
```

CLI: `./inference/.venv/bin/python -m inference.predict_single --all-stations --date ... --hour ... --n-hours 5 --out result.parquet`
(`--station-id`와 동시 사용 불가, 인구는 정류소별 격자 평소 인구로 항상 자동
대체). 시간(horizon)마다 전체 정류소를 배치로 묶어서 LightGBM을 한 번만
호출하고, feature 조립·DataFrame 캐스팅도 horizon당 한 번만 한다 — 대여의
"직전 실적" 조회(가장 무거운 부분)도 정류소 축이 아니라 anchor 축으로
벡터화돼 있다. 정류소별 실패는 재시도 없이 건너뛰고 `failed` 목록 +
`sys.stderr` 로그로 남는다.

**부분 실패는 CLI 종료 코드를 막지 않는다** — 하나라도 성공하면(`actual_count > 0`)
성공한 결과는 그대로 S3에 저장되고 exit 0으로 끝난다(실패 목록은 별도 파일로
같이 저장됨). `actual_count == 0`(완전 실패)일 때만 exit 1로 막는다 — 예전엔
실패가 하나라도 있으면 exit 1이라 Airflow가 이미 성공한 나머지 결과의 DB
적재까지 막아버리는 운영 취약점이 있었다.

## 한계

- 학습 기간(feature_engine이 만든 테이블의 커버리지)을 크게 벗어난 시각을
  예측하면 lag/rolling이 전부 fallback(또는 프로필도 없으면 NaN)이 되어
  정확도가 떨어진다.
- 예보(`weather_short_term_forecast`) 자동 수집 스케줄이 아직 없어(수동
  트리거만 가능) horizon>1이어도 실제로는 관측 fallback을 타는 경우가 많다.
- station_id는 실제로 트립 실적이 있어 학습에 포함된 정류소만 유효하다.
  그 외 ID를 넣으면 `ValueError`.

## 검증

```bash
cd ml
./inference/.venv/bin/python -m pytest inference/tests/ -q
```
