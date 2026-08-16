# 2025년 전체 피처 생성 + 학습 가이드

이 문서는 로컬 개발 머신(RAM 18GB)에서 검증만 하고, **실제 2025년 전체(1~12월) 피처
생성과 학습은 더 큰 머신에서 진행**할 작업자를 위한 세팅/실행 가이드다. 로컬에서는
디스크/RAM 제약 때문에 기간을 좁혀서(9/27~12/31, 학습은 11월 한 달) 검증만 했고,
여기 적힌 절차는 그 제약이 없는 머신 기준이다.

## 1. 필요한 데이터 (git에는 안 들어있음)

`data/`는 `.gitignore` 대상이라 저장소를 클론해도 아래 파일들은 별도로 전달받아야
한다. **CSV 원본은 필요 없다** — 이 파이프라인은 이미 처리된 parquet만 읽는다(1차
정제/CSV→parquet 변환은 이 저장소 밖에서 이미 끝난 상태로 가정).

```
ml/data/processed_v2/station_master.parquet          (192KB)
ml/data/processed_v2/weather_2025.parquet             (128KB)
ml/data/processed_v2/station_status_2025.parquet      (39MB)
ml/data/processed_v2/targets_2025.parquet             (160MB)
ml/data/processed_v2/return_targets_2025.parquet      (160MB)
ml/data/processed_v2/population_2025.parquet          (240MB)
ml/data/parquet/서울특별시 공공자전거 대여이력 정보_2501.parquet ~ _2512.parquet (12개 파일, 약 530MB)
ml/data/output/analysis_summary.json
```

총 약 1.1GB. 폴더 구조를 정확히 그대로 유지해야 한다(`libs/ml_common/paths.py`가
`ml/` 기준 상대경로로 이 경로들을 계산함 — 모든 명령은 반드시 `ml/` 디렉터리에서
실행할 것).

`analysis_summary.json`은 원본 데이터에서 뽑은 게 아니라 직접 작성한 2025년
공휴일 목록이다. 파일이 없으면 아래 내용 그대로 만들면 된다:

```json
{
  "holidays_2025": [
    "2025-01-01", "2025-01-27", "2025-01-28", "2025-01-29", "2025-01-30",
    "2025-03-01", "2025-03-03", "2025-05-05", "2025-05-06", "2025-06-03",
    "2025-06-06", "2025-08-15", "2025-10-03", "2025-10-05", "2025-10-06",
    "2025-10-07", "2025-10-08", "2025-10-09", "2025-12-25"
  ]
}
```

## 2. 환경 세팅

각 폴더가 독립된 `uv` 프로젝트다(공용 venv 없음). `ml/` 기준으로:

```bash
cd ml
(cd feature_engineering && uv sync)   # PySpark, EMR 대상 — 로컬은 local[*]
(cd training && uv sync)              # LightGBM, pandas
(cd inference && uv sync)             # pandas만
```

`libs/ml_common`은 각 폴더의 `pyproject.toml`이 `path = "../../libs/ml_common"`로
editable 참조하므로 따로 sync할 필요 없음(각 uv sync에 자동 포함).

## 3. 피처 생성 (2차 정제, Spark)

### 3-1. 기본 실행 (옵션 없이 — 전체 12개월, 5분 tick 전체)

```bash
cd ml
./feature_engineering/.venv/bin/python -m feature_engineering.spark.run_pipeline
```

워터마크가 없으면 자동으로 전체 빌드(`_run_full_build`)를 한다. 결과물:
`data/processed_v2/spark/{PARAM_COMBO_ID}/station_hour_merged_2025.parquet`,
`station_hour_features_2025.parquet`(5분 tick, 단일 horizon, 43개 컬럼 — 실측
264,449,840행 규모, 로컬 실험에선 96일치가 1.3GB였으니 전체 연도는 대략 5GB 안팎
예상).

**셔플 파티션 튜닝(권장)**: 로컬 기본값(`spark.sql.shuffle.partitions=8`)은 데이터가
크면 파티션당 행 수가 너무 많아져 셔플 스필이 과도해진다(로컬에서 96일치만
돌렸는데도 30GB 스필을 봤다). 코어/메모리가 넉넉한 머신이면 이 값을 올릴 것:

```bash
SPARK_SHUFFLE_PARTITIONS=64 ./feature_engineering/.venv/bin/python -m feature_engineering.spark.run_pipeline
```

로컬 개발 전용 옵션(`SPARK_MASTER`, `spark.driver.memory` 기본 8g)은
`feature_engineering/spark/spark_session.py` 참고 — EMR 등 클러스터 환경에서는
`SPARK_MASTER`/드라이버 메모리를 안 건드려도 되고, `spark-submit` 자체 옵션으로
제어하면 된다.

### 3-2. Multi-horizon 학습 테이블 생성

```bash
./feature_engineering/.venv/bin/python -m feature_engineering.spark.build_multi_horizon_features
```

`station_hour_features_2025.parquet`(위 3-1 결과)을 horizon=1~12로 self-join해
학습 테이블(`station_hour_features_multihorizon_2025.parquet`)을 만든다. **옵션
없이 실행하면 anchor를 5분 tick 전체·전체 기간으로 유지** — 이게 원래 설계 의도고,
가장 정확하다. 다만 규모가 매우 크므로(아래 4번 "자원 가이드" 참고) 머신이 이
규모를 못 받으면 anchor 밀도를 좁히는 옵션을 쓸 수 있다:

| 환경변수 | 효과 |
|---|---|
| `MULTI_HORIZON_ANCHOR_SINCE="2025-01-01 00:00:00"` | anchor 시작 시각 제한 |
| `MULTI_HORIZON_ANCHOR_UNTIL="2026-01-01 00:00:00"` | anchor 끝 시각 제한(미만) |
| `MULTI_HORIZON_ANCHOR_HOURLY_ONLY=1` | anchor를 매시 정각(60분)만 사용 — 12배 감소 |
| `MULTI_HORIZON_ANCHOR_TICK_MINUTES=20` | anchor를 N분 간격만 사용(60분 전용인 위 옵션의 일반화) — 예: 20이면 3배 감소 |

target_ts(라벨/날씨/캘린더) 쪽은 이 옵션들과 무관하게 항상 원본 5분 tick 전체를
그대로 쓴다 — anchor 쪽만 솎아내는 옵션이라 서빙 정밀도(`inference`가 라이브로
계산하는 5분 tick lag/rolling)에는 전혀 영향 없다.

## 4. 자원 가이드 (실측 기반)

로컬 머신(RAM 18GB, macOS)에서 **2025년 11월 한 달만** 떼어 실측한 값:

| anchor 밀도 | 학습 행 수(1개월) | 학습 시 peak RAM(`/usr/bin/time -l`의 `maximum resident set size`) | 학습 소요시간(대여 모델 1개, poisson+q10+q50+q90) |
|---|---|---|---|
| 정각(60분) | 2,197만 | 약 3GB (참고용 — 정밀 측정 아님) | 수 분 |
| 20분 | 6,592만 | **10.14GB** (정밀 측정) | **약 60분** |
| 5분(전체) | 2.6억+ | 로컬(18GB)에서 OOM으로 실패 | - |

**macOS 주의**: `/usr/bin/time -l`이 내는 `peak memory footprint`(압축메모리 등
포함, 훨씬 크게 보임)는 무시하고 `maximum resident set size`만 볼 것 — 전자는
실제 메모리 압박과 안 맞는 경우가 많다(Linux 서버라면 이런 구분 자체가 없음).

**1년 전체로 단순 외삽하면**(11월 한 달 → 12배, 위 20분 앵커 실측치 기준):

| anchor 밀도 | 1년 전체 예상 행 수 | 1년 전체 예상 peak RAM |
|---|---|---|
| 정각(60분) | 약 2.6억 | 대략 40GB대(추정) |
| 20분 | 약 7.9억 | 대략 130GB대(추정) |
| 5분(전체) | 약 32억 | 단일 머신 pandas/LightGBM으론 사실상 불가 — 분산 필요 |

정확한 실측이 아니라 11월 실측치의 단순 비례 추정이니, 실제 머신에서 돌리기 전에
스펙에 맞는 밀도를 고르고(위 3-2 옵션), 필요하면 `training/config.py`의
`TRAIN_SAMPLE_FRAC` 등으로 한 번 더 표본을 줄일 것.

**5분 tick 전체로 1년치를 하고 싶다면** 단일 머신은 포기하고 `training/config.py`에
이미 있는 LightGBM 자체 분산 학습(`LGB_TREE_LEARNER=data`/`voting`,
`LGB_NUM_MACHINES` 등)을 실제 인프라와 함께 쓰는 걸 권한다 — 단, 지금 구현은 각
머신이 station 몫만 걸러 쓰기 전에 **전체 parquet을 일단 다 읽는 구조**라, 이 부분도
같이 개선하지 않으면 머신 1대당 필요 RAM 자체는 안 줄어든다(개선 전이면 결국 위
20분/정각 앵커 축소 옵션과 병행 필요).

## 5. 학습

`training/config.py`의 학습/검증/평가 기간이 환경변수로 열려있다(기본값은 로컬
검증용 11월 한 달).

**주의 — valid/test 구간은 학습에 안 들어간다.** walk-forward split이라 `TRAIN_*`
구간의 데이터만 실제로 모델이 학습하고, `VALID_*`/`TEST_*` 구간은 조기종료 판단과
평가 지표 계산에만 쓰인다. 즉 **valid/test로 잡은 기간의 패턴은 모델이 전혀 배우지
못한다.** 12월 들어 기온 급락으로 대여 수요가 11월 대비 1/30 수준까지 떨어지는 걸
실측으로 확인했는데(예: 12/13 227건), 만약 11~12월을 통째로 valid/test에만 넣으면
모델이 이 겨울 패턴을 한 번도 못 보고 학습한 채로 배포되는 것과 같다 — 실제로
로컬 검증(11월만 학습)에서 12월 예측이 크게 어긋났던 것과 같은 종류의 문제다.
그래서 **valid/test는 짧게(예: 마지막 1~2주) 잡고, 학습 구간은 최대한 최근까지
포함시킬 것**:

```bash
export TRAIN_START=2025-01-01
export TRAIN_END=2025-12-17
export VALID_START=2025-12-18
export VALID_END=2025-12-24
export TEST_START=2025-12-25
export TEST_END=2025-12-31

./training/.venv/bin/python -m training.train_rental_model
./training/.venv/bin/python -m training.train_return_model
```

이렇게 하면 학습이 12월 중순까지(겨울 저수요 패턴 포함) 다 포함되고, 마지막
2주만 정직한 평가용으로 떼어둔 것이다.

**더 엄격하게 하고 싶다면(참고, 지금 코드엔 없음)**: walk-forward 평가로
"이 정도면 됐다"를 확인한 뒤, valid/test 구간까지 포함한 **전체 기간으로 한 번
더 재학습**해서 그걸 실제 배포용 모델로 쓰는 방법도 있다(조기종료로 정해진
라운드 수는 고정해서 그대로 씀). 이러면 평가에 쓴 기간의 패턴까지 최종 모델이
배우게 된다 — 다만 `train_common.py`에 이 "최종 재학습" 단계가 아직 구현돼
있지 않으니, 필요하면 별도로 추가해야 한다.

메모리가 빠듯하면
`TRAIN_SAMPLE_FRAC`/`VALID_SAMPLE_FRAC`/`TEST_SAMPLE_FRAC`(0~1, 기본 1.0)도
같이 조절할 수 있다 — 단, 현재 구현은 **전체를 다 읽은 뒤에** 표본을 뽑으므로
읽기 시점의 peak 메모리 자체는 안 줄어든다(표본 비율을 낮춰도 최초 로딩은 전체
크기만큼 RAM이 필요) — 진짜 메모리가 부족하면 표본 비율보다 위 3-2의 anchor
밀도 옵션으로 원본 테이블 자체를 줄이는 쪽이 확실하다.

결과물: `training/models/{rental,return}_{poisson,q10,q50,q90}.txt` +
`*_conformal_correction.json` + `*_station_categories.json` + `*_metrics.json`.

## 6. 검증(선택)

학습이 끝나면 `inference` 쪽에서 fallback 프로필부터 만들고:

```bash
./inference/.venv/bin/python -m inference.build_station_profile
./inference/.venv/bin/python -m inference.build_population_profile
```

간단한 스모크 테스트:

```bash
PYTHONPATH="$(pwd)" ./inference/.venv/bin/python -c "
from inference import predict_single as ps
print(ps.predict_rental_demand(station_id='ST-2000', date='2025-12-13', hour=15, minute=45, temp=3.6, precip=0.0, wind=1.0, humidity=89, population=3000.0))
"
```

에러 없이 `pred_mean` 등이 찍히면 정상이다.
