# inference — 실행 방법

학습이 끝난 모델(`training/models/*.txt`)로 예측을 실행하는 두 가지 경로를
제공한다. 이 프로젝트엔 실시간 서빙 API가 없다 — 둘 다 배치/함수 호출로
예측을 확인하는 용도다.

| 경로 | 언제 쓰나 | 입력 |
|---|---|---|
| **배치 조회** (`predict_common.py` + `predict_{rental,return}_demand.py`) | 이미 만들어둔 2025년 feature 테이블에서 특정 station/기간을 조회 (백테스트, 결과 확인) | station_id, 날짜/시간 범위 |
| **단일 시점 예측** (`predict_single.py`) | 날짜/시각/날씨/(선택)인구 값을 직접 넣어서 그 시점 하나를 예측 (실서비스 연동 대상) | station_id, date, hour, 날씨 4종, 인구(선택) |

설계 배경은 [DESIGN.md](DESIGN.md) 참고.

## 세팅

```bash
cd ml/inference
uv sync   # pyproject.toml/uv.lock 기준 .venv 생성 — pandas/numpy + ml_core(editable) 포함
```

`training`이 먼저 모델을 학습해둬야 한다 ([training/README.md](../training/README.md)):
`training/models/{rental,return}_{poisson,q10,q50,q90}.txt`,
`{rental,return}_station_categories.json`, `{rental,return}_conformal_correction.json`.

**단일 시점 예측을 쓰려면 추가로 fallback 프로필 2개를 한 번 만들어야 한다**
(`feature_engine`이 만든 `station_hour_merged_2025.parquet`/`population_2025.parquet`이
먼저 있어야 함):

```bash
cd ml
./inference/.venv/bin/python -m inference.build_station_profile      # -> station_hourly_profile.parquet (대여/반납 fallback)
./inference/.venv/bin/python -m inference.build_population_profile   # -> population_hourly_profile.parquet (인구 fallback)
```

## 배치 조회 CLI

```bash
# 기본값: 2025-12(테스트 기간) 전체 정류소
./inference/.venv/bin/python -m inference.predict_rental_demand

# 특정 정류소 + 특정 기간 (2025년 범위 내, YYYY-MM-DD)
./inference/.venv/bin/python -m inference.predict_rental_demand --station-id ST-2000 --start-date 2025-06-01 --end-date 2025-06-07

# 특정 정류소의 특정 시각 하나만
./inference/.venv/bin/python -m inference.predict_rental_demand --station-id ST-2000 --start-date 2025-06-01 --end-date 2025-06-01 --hour 8

# 반납 모델도 옵션은 동일 (exposure 없음)
./inference/.venv/bin/python -m inference.predict_return_demand --station-id ST-2000 --start-date 2025-06-01 --end-date 2025-06-01
```

| 인자 | 기본값 | 설명 |
|---|---|---|
| `--station-id` | 없음(전체) | 정류소 ID. `station_master.parquet`에서 확인 |
| `--start-date`, `--end-date` | 테스트 기간(2025-12) | YYYY-MM-DD |
| `--hour` | 없음(전체 시간) | 0~23 중 하나만 |
| `--out` | `predictions_{rental,return}_test.parquet` | 저장 경로 |

결과가 42행 이하로 좁혀지면 표로 바로 출력하고, 그보다 크면 요약 지표만 찍은 뒤
전체 결과를 parquet로 저장한다. 이 스크립트를 인자 없이 돌리면 `training`이
학습 직후 출력한 지표와 소수점까지 정확히 재현된다(모델 저장/로드 round-trip 검증됨).

## 단일 시점 예측

```python
from inference.predict_single import predict_rental_demand, predict_return_demand

predict_rental_demand(
    station_id="ST-2000", date="2025-06-01", hour=8,
    temp=22.5, precip=0.0, wind=2.1, humidity=55,
    population=3200,               # 없으면 생략 가능 — 격자 평소 인구로 대체됨
)
# -> {'station_id': 'ST-2000', 'date': '2025-06-01', 'hour': 8,
#     'pred_mean': ..., 'pred_p10': ..., 'pred_p50': ..., 'pred_p90': ...,
#     'lag_fallback_used': [], 'lag_data_freshness': 1.0,
#     'population_source': 'provided'}
```

CLI로도 바로 확인 가능 (`--population` 생략 가능):

```bash
./inference/.venv/bin/python -m inference.predict_single \
  --station-id ST-2000 --date 2025-06-01 --hour 8 \
  --temp 22.5 --precip 0.0 --wind 2.1 --humidity 55
```

날짜/시각/날씨/인구만 받는 이유(lag/rolling은 내부에서 자동 조회), 2단계
fallback(실시간 히스토리 없으면 → 정류소/격자 평소 패턴), 실시간 데이터
결측·지연 대응은 [DESIGN.md](DESIGN.md)에 자세히 있다.

## N시간 뒤까지 예측 (재귀, 정확도보다 속도 우선)

```python
from inference.predict_single import predict_demand_multi_hour

predict_demand_multi_hour(
    station_id="ST-2000", date="2025-06-01", hour=8,     # "지금"
    temp=22.5, precip=0.0, wind=2.1, humidity=55,
    population=3200,
    n_hours=12,   # 9시~20시, 1시간 간격 12개
)
# -> [{'station_id': ..., 'date': ..., 'hour': 9,
#      'rental': {'pred_mean': ..., ..., 'lag_fallback_used': [...], 'lag_data_freshness': ...},
#      'return': {'pred_mean': ...}, 'population_source': ...}, ...] (길이 12)
```

CLI는 `--n-hours`만 추가하면 된다: `./inference/.venv/bin/python -m inference.predict_single ... --n-hours 12`.

**h=1(바로 다음 시간)은 위 단일 시점 예측과 완전히 동일한 값**이다. h=2부터는
직전 스텝의 예측값을 그 다음 스텝의 "직전 실적"(lag_1h, roll_mean/std_3h/24h)으로
재귀적으로 사용한다 — 미래라 실측이 없으니 어쩔 수 없는 선택이고, **horizon이
커질수록 오차가 누적되는 걸 알고도 채택한 것**(history.md 20번 항목,
20번이 뒤집은 18번 항목의 기각 사유 참고). 더 정확한 대안(재귀 없이 horizon을
feature로 추가)은 `training/experiments/multi_horizon/`에 이미 구현·검증돼
있으니, 정확도가 문제되면 그쪽으로 교체할 것.

## 전체 정류소 배치 (5분 주기 갱신용)

```python
from inference.predict_single import predict_demand_multi_hour_all_stations

predict_demand_multi_hour_all_stations(
    date="2025-06-01", hour=8, temp=22.5, precip=0.0, wind=2.1, humidity=55,
    n_hours=5,   # 실사용 범위(위 20번 항목 참고)라면 5 정도로 충분
)
# -> predict_demand_multi_hour()과 같은 형태의 dict를 정류소별로 이어붙인 리스트
#    (station_ids 생략 시 모델이 실제로 학습한 정류소 2,582개 전체)
```

CLI: `./inference/.venv/bin/python -m inference.predict_single --all-stations --date ... --hour ... --temp ... --precip ... --wind ... --humidity ... --n-hours 5 --out result.parquet`
(`--station-id`와 동시 사용 불가, 인구는 정류소별 격자 평소 인구로 항상 자동 대체).

시간(h)마다 전체 정류소를 배치로 묶어서 LightGBM을 한 번만 호출하고(정류소별로
따로 부르면 5분 주기에 못 맞을 정도로 느려짐), feature 조립(정류소별 dict
생성)과 DataFrame 캐스팅도 시간(h)당 한 번만 한다(history.md 22/23번 항목).
대여의 "직전 실적" 조회(트립 point-in-time 계산, 가장 무거운 부분)도 정류소
축이 아니라 **anchor 축으로 뒤집어**(정류소가 몇 개든 anchor 개수(최대
25개)만큼만 반복) 벡터화했다(history.md 24번 항목). 실측: 전체 2,582개
x 12시간 **약 1.8분**(캐시 워밍업 포함, 처음 측정한 "약 3시간" 대비 약
100배), 실사용 범위인 5시간이면 약 1.2분. 정류소별 실패는 재시도 없이
건너뛰되 `sys.stderr`에 크게 로그를 남긴다.

## 한계

- 2025년 범위를 크게 벗어난 미래를 예측하면 lag/rolling이 전부 fallback(또는
  프로필도 없으면 NaN)이 되어 정확도가 떨어진다.
- 날씨는 예보가 아닌 관측치 기준으로 학습됨 — 실제 서비스에서 예보 API 값을
  넣으면 학습 때보다 정확도가 떨어질 수 있다(train-serve skew, 알려진 한계).
- station_id는 2025년에 트립이 1건 이상 있었던 정류소만 유효하다. 그 외 ID를
  넣으면 `ValueError`.

## 검증

```bash
cd ml
./inference/.venv/bin/python -m pytest inference/tests/ -q
```
