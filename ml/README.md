# 따릉이 수요예측 — LightGBM 대여/반납 파이프라인

collector가 S3(MinIO) Silver 레이어에 쌓은 원본(대여이력, 정류소, 날씨, 250m
생활인구)을 station×5분 tick 단위로 병합하고, lag feature를 붙여 대여/반납을
완전히 분리된 LightGBM 모델(Poisson+exposure, quantile P10/50/90)로
학습·추론하는 파이프라인.

## 폴더 구조

기능별로 서로 다른 인스턴스에서 배포·실행할 수 있도록 5개 폴더로 나뉜다.
**환경은 폴더별로 `uv`가 관리한다**(`pyproject.toml`/`uv.lock` 각자 보유) — 공용
`.venv`/`.venv-spark`는 더 이상 안 쓴다.

| 폴더 | 역할 | 실행 환경 |
|---|---|---|
| **[../libs/ml_core/](../libs/ml_core/README.md)** | 세 인스턴스가 공유하는 파라미터·경로·핵심 알고리즘(censoring 로직, 모델 계약, 채점 함수). `ml/`과 별도로 관리되는 독립 라이브러리(`<repo-root>/libs/ml_core/`) — 아래 세 폴더가 각자 editable 의존성으로 참조 | 어디든(가벼운 순수 로직) |
| **[feature_engine/](feature_engine/README.md)** | station×5분 tick feature 테이블 생성(Spark, EMR/로컬 `local[*]` 단일 노드), Silver를 직접 읽어 대여/반납 multi-horizon 테이블 2개를 만든다 — **본 서비스 코드는 `spark/`뿐**(옛 pandas 1차/2차정제는 전부 삭제됨) | `feature_engine/.venv`(uv, Python 3.11)/EMR |
| **[training/](training/README.md)** | feature 테이블로 LightGBM 대여/반납 모델 학습, 챔피언 승격, 성능 모니터링 — [MLflow](../docs/ml/MLFLOW_SETUP.md)로 실험 추적 | `training/.venv`(uv) |
| **[inference/](inference/README.md)** | 학습된 모델로 배치 조회 + 단일/다중 시점 예측 | `inference/.venv`(uv) |
| **data/** | 로컬 개발용 샘플 원본(`dev/seed_s3_from_local.py`가 이걸 Silver 스키마로 MinIO에 시딩) — 실제 원본은 collector가 S3 Silver에 직접 쌓는다, 로컬 파일시스템 폴백 없음 | — |

각 폴더의 실행 방법은 그 폴더의 `README.md`, 설계 배경은 `DESIGN.md`를 참고.

**실제 배포에서는 세 인스턴스가 각자 자기 단계만 실행한다** — 아래 "빠른 시작"의
개별 명령이 그 방식이다. 로컬 한 대에서 전체 파이프라인을 처음부터 끝까지
빠르게 검증해보고 싶을 때만 [run_full_pipeline.py](run_full_pipeline.py)를 쓴다
(`--only dataset`/`training`/`inference`로 한 단계만 실행 가능) — 개발 편의용이지
운영 배포 방식이 아니다.

## 빠른 시작

```bash
cd ml
brew install libomp   # macOS에서 LightGBM 실행에 필요

# 각 폴더의 venv를 uv로 준비(최초 1회, 폴더별로) — ml_core은 editable 의존성으로 같이 설치됨
(cd feature_engine && uv sync)
(cd training && uv sync)
(cd inference && uv sync)

# 0) 로컬 개발만: MinIO/Postgres/MLflow 기동 + 샘플 원본을 Silver 스키마로 시딩 (저장소 루트에서)
(cd .. && make up)
uv run python ../dev/seed_s3_from_local.py --start-date 2025-01-01 --end-date 2025-12-31

# 1) 데이터셋 생성 (feature_engine/README.md) — Silver를 직접 읽는다, 로컬 legacy 1차정제 없음
./feature_engine/.venv/bin/python -m feature_engine.spark.run_pipeline
./feature_engine/.venv/bin/python -m feature_engine.spark.build_multi_horizon_features   # 대여/반납 최종 테이블 2개

# 2) 학습 (training/README.md) — 결과는 MLflow(../docs/ml/MLFLOW_SETUP.md)에도 기록됨
./training/.venv/bin/python -m training.train_rental_model
./training/.venv/bin/python -m training.train_return_model

# 3) 추론 (inference/README.md)
./inference/.venv/bin/python -m inference.build_station_profile
./inference/.venv/bin/python -m inference.build_population_profile
./inference/.venv/bin/python -m inference.predict_single --station-id ST-2000 --date 2025-06-01 --hour 8
```

## 검증

```bash
cd ml
./training/.venv/bin/python -m pytest ../libs/ml_core/tests training/tests -q
./inference/.venv/bin/python -m pytest inference/tests -q
./feature_engine/.venv/bin/python -m pytest feature_engine/tests/dev_spark_rolling_parity.py feature_engine/tests/dev_spark_build_features.py feature_engine/tests/dev_spark_incremental.py feature_engine/tests/dev_spark_multi_horizon_parity.py -q
```

`feature_engine/legacy/`(옛 pandas 1차/2차정제)는 저장소에서 완전히
삭제됐다 — 옮겨진 게 아니라 없어졌다. Silver를 직접 읽는 Spark 경로(`spark/`)만
남아 있다.

테스트 파일은 `test_*.py`가 아니라 **`dev_*.py`**로 짓는다(`pytest.ini`의
`python_files = dev_*.py`) — ML의 train/valid/**test** split(`TEST_START`,
`multi_horizon_test.parquet` 등)과 "test_"가 겹쳐서 헷갈리는 걸 피하기 위함.
`libs/ml_core/`은 `ml/` 밖이라 이 설정을 상속받지 못해서 자체 `pyproject.toml`에
같은 규칙을 따로 정의해뒀다.

Spark 관련 테스트는 반드시 `feature_engine/.venv`(uv, Python 3.11)로 실행할 것 —
다른 두 폴더의 venv는 pyspark가 지원하지 않는 Python 버전을 쓴다.

## 결과 요약

**(2026-08) 아래 고정 수치는 지웠다** — 시간 단위 그리드 시절(20분 tick 전환,
피처 축소, multi-horizon 분리 전) 마지막 측정값이라 지금 스키마와 안 맞고,
계속 갱신하며 여기 박아두면 금방 또 stale해진다. 지금은 학습마다
[MLflow](../docs/ml/MLFLOW_SETUP.md)(`bike-demand-training` experiment)에
`poisson_deviance_test`/`rmse_test`/`pinball_test_q{10,50,90}`/
`p10_p90_coverage_calibrated_test`가 기록되니 그쪽에서 최신 값을 확인할 것.
월별 실측 드리프트는 `bike-demand-monitoring` experiment 참고.

## 문서 구조

| 문서 | 내용 |
|---|---|
| **README.md** (이 문서) | 폴더 구조, 빠른 시작 |
| **[../libs/ml_core/README.md](../libs/ml_core/README.md)** | 공유 로직, 프로필 시스템, 타임존 규칙 |
| **[feature_engine/README.md](feature_engine/README.md)** / **[DESIGN.md](../docs/ml/feature_engine/DESIGN.md)** | 데이터 파이프라인: 실행 방법 / 설계 배경 |
| **[training/README.md](training/README.md)** / **[DESIGN.md](../docs/ml/training/DESIGN.md)** | 모델 학습: 실행 방법 / 설계 배경 |
| **[inference/README.md](inference/README.md)** / **[DESIGN.md](../docs/ml/inference/DESIGN.md)** | 추론: 실행 방법 / 설계 배경 |
| **[../docs/ml/MLFLOW_SETUP.md](../docs/ml/MLFLOW_SETUP.md)** | MLflow 실험 추적 세팅/사용법 |
| **[../docs/ml/history.md](../docs/ml/history.md)** | 의사결정 히스토리(시간순) — 무엇을 왜 그렇게 결정했는지 |
| **[../docs/adr/](../docs/adr/)** | 개별 아키텍처 결정 기록(ADR) — 굵직한 결정 하나당 파일 하나, `template.md` 형식 |
| **[../docs/ml/DATA_CATALOG.md](../docs/ml/DATA_CATALOG.md)** | 원본·참고 데이터 소스별 상세 |
| **[../docs/ml/REALTIME_FEATURES.md](../docs/ml/REALTIME_FEATURES.md)** | point-in-time 대여 카운트 설계(train-serving skew 대응) |

## 꼭 알아야 할 핵심 제약

- **학습 기간은 feature_engine이 실제로 쌓아둔 기간으로 한정** — 날씨·재고·인구
  등 소스별 커버리지가 제각각일 수 있다(`training/config.py`의 `TRAIN_YEAR`).
- **날씨는 학습=관측치, 추론=관측 또는 예보** — 학습은 항상 target_ts의 실제
  관측 날씨(ground truth)로 배운다. 추론은 target_ts가 미래면(horizon>1)
  예보를 먼저 시도하고 없으면 관측으로 대체한다 — collector의 예보 자동
  수집 스케줄이 아직 없어 실제로는 관측 fallback이 아직 흔하다
  ([inference/DESIGN.md](../docs/ml/inference/DESIGN.md) 참고).
- **생활인구는 250m 격자 기준** — 격자 ID를 좌표로 역산해 정류소와 직접 매칭.
- **단일 시점 예측은 실시간 데이터 결측에 대비한 fallback 내장** — [inference/DESIGN.md](../docs/ml/inference/DESIGN.md) 참고.
- **타임존은 KST(Asia/Seoul)로 통일** — [../libs/ml_core/README.md](../libs/ml_core/README.md) 참고.

각 제약의 배경과 대안 검토 과정은 [../docs/ml/history.md](../docs/ml/history.md)/각 폴더 `DESIGN.md`에 자세히 정리돼 있다.
