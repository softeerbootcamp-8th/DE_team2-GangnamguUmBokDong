# 따릉이 수요예측 — LightGBM 대여/반납 파이프라인

`data/`의 원본(대여이력, 정류소, 날씨, 250m 생활인구)을 station×5분tick 단위로
병합하고, lag/rolling feature를 붙여 대여/반납 수요를 LightGBM(Poisson+exposure,
quantile P10/50/90)으로 학습·추론하는 파이프라인.

## 폴더 구조

기능별로 서로 다른 인스턴스에서 배포·실행할 수 있도록 5개 폴더로 나뉜다.
**환경은 폴더별로 `uv`가 관리한다**(`pyproject.toml`/`uv.lock` 각자 보유) — 공용
`.venv`/`.venv-spark`는 더 이상 안 쓴다.

| 폴더 | 역할 | 실행 환경 |
|---|---|---|
| **[../lib/ml_common/](../lib/ml_common/README.md)** | 세 인스턴스가 공유하는 파라미터·경로·핵심 알고리즘(censoring 로직, 모델 계약, 채점 함수). `ml/`과 별도로 관리되는 독립 라이브러리(`<repo-root>/lib/ml_common/`) — 아래 세 폴더가 각자 editable 의존성으로 참조 | 어디든(가벼운 순수 로직) |
| **[make_dataset/](make_dataset/README.md)** | station×5분tick feature 테이블 생성(Spark, EMR/로컬 `local[*]` 단일 노드) — **본 서비스 코드는 `spark/`뿐**. pandas 1차/2차정제는 전부 `legacy/`(로컬 테스트 입력 준비용, `LEGACY_AUDIT.md` 참고) | `make_dataset/.venv`(uv, Python 3.11)/EMR |
| **[training/](training/README.md)** | feature 테이블로 LightGBM 대여/반납 모델 학습, 성능 모니터링 | `training/.venv`(uv) |
| **[inference/](inference/README.md)** | 학습된 모델로 배치 조회 + 단일 시점 예측 | `inference/.venv`(uv) |
| **data/** | 원본·중간·최종 산출물 (로컬 개발은 파일시스템 그대로, 실제 배포는 S3) | — |

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

# 각 폴더의 venv를 uv로 준비(최초 1회, 폴더별로) — ml_common은 editable 의존성으로 같이 설치됨
(cd make_dataset && uv sync)
(cd training && uv sync)
(cd inference && uv sync)

# 1) 데이터셋 생성 (make_dataset/README.md)
# 1차 정제는 실제 배포에선 이 저장소 밖에서 처리된다 — 로컬에서 처음부터 테스트해보려면
# legacy pandas로 입력을 준비: ./make_dataset/.venv/bin/python -m make_dataset.legacy.scripts.run_build_pipeline
./make_dataset/.venv/bin/python -m make_dataset.spark.run_pipeline   # 본 서비스 경로(2차정제, local[*] 단일 노드)

# 2) 학습 (training/README.md)
./training/.venv/bin/python -m training.train_rental_model
./training/.venv/bin/python -m training.train_return_model

# 3) 추론 (inference/README.md)
./inference/.venv/bin/python -m inference.build_station_profile
./inference/.venv/bin/python -m inference.build_population_profile
./inference/.venv/bin/python -m inference.predict_rental_demand --station-id ST-2000 --start-date 2025-06-01 --end-date 2025-06-07
```

## 검증

```bash
cd ml
./training/.venv/bin/python -m pytest ../lib/ml_common/tests training/tests -q
./inference/.venv/bin/python -m pytest inference/tests -q
./make_dataset/.venv/bin/python -m pytest make_dataset/tests/dev_spark_rolling_parity.py make_dataset/tests/dev_spark_build_features.py make_dataset/tests/dev_spark_incremental.py -q
```

`make_dataset/legacy/`로 이동한 옛 pandas 2차정제 구현의 테스트(`dev_features_rental_censoring.py`/
`dev_completion_curve_integration.py`)는 더 이상 기본 검증에 포함하지 않는다 — 여전히
`./make_dataset/.venv/bin/python -m pytest make_dataset/legacy/tests -q`로 개별 실행
가능(자세한 분류는 [LEGACY_AUDIT.md](LEGACY_AUDIT.md) 참고).

테스트 파일은 `test_*.py`가 아니라 **`dev_*.py`**로 짓는다(`pytest.ini`의
`python_files = dev_*.py`) — ML의 train/valid/**test** split(`TEST_START`,
`multi_horizon_test.parquet` 등)과 "test_"가 겹쳐서 헷갈리는 걸 피하기 위함.
`lib/ml_common/`은 `ml/` 밖이라 이 설정을 상속받지 못해서 자체 `pyproject.toml`에
같은 규칙을 따로 정의해뒀다.

Spark 관련 테스트는 반드시 `make_dataset/.venv`(uv, Python 3.11)로 실행할 것 —
다른 두 폴더의 venv는 pyspark가 지원하지 않는 Python 버전을 쓴다.

## 결과 요약

2025-12 테스트셋 기준 (시간 단위 그리드 시절 마지막 측정값 — 5분 tick 그리드
전환 후 재검증 필요, [history.md](history.md) 참고):

| | Poisson deviance | RMSE | pinball P10/P50/P90 | P10~P90 커버리지(이론 0.80) |
|---|---|---|---|---|
| 대여 | 0.962 | 1.107 | 0.091 / 0.318 / 0.191 | 0.828 |
| 반납 | 0.920 | 1.091 | 0.089 / 0.308 / 0.185 | 0.865 |

## 문서 구조

| 문서 | 내용 |
|---|---|
| **README.md** (이 문서) | 폴더 구조, 빠른 시작 |
| **[../lib/ml_common/README.md](../lib/ml_common/README.md)** | 공유 로직, 프로필 시스템, 타임존 규칙 |
| **[make_dataset/README.md](make_dataset/README.md)** / **[DESIGN.md](make_dataset/DESIGN.md)** | 데이터 파이프라인: 실행 방법 / 설계 배경 |
| **[training/README.md](training/README.md)** / **[DESIGN.md](training/DESIGN.md)** | 모델 학습: 실행 방법 / 설계 배경 |
| **[inference/README.md](inference/README.md)** / **[DESIGN.md](inference/DESIGN.md)** | 추론: 실행 방법 / 설계 배경 |
| **[history.md](history.md)** | 의사결정 히스토리(시간순) — 무엇을 왜 그렇게 결정했는지 |
| **[adr/](adr/)** | 개별 아키텍처 결정 기록(ADR) — 굵직한 결정 하나당 파일 하나, `adr/template.md` 형식 |
| **[DATA_CATALOG.md](DATA_CATALOG.md)** | `data/`의 모든 원본·참고 데이터 소스별 상세 |
| **[REALTIME_FEATURES.md](REALTIME_FEATURES.md)** | point-in-time 대여 카운트 설계(train-serving skew 대응) — 지금도 유효, 경로만 `make_dataset`/`ml_common`/`inference`로 갱신해서 읽을 것 |
| **[LEGACY_AUDIT.md](LEGACY_AUDIT.md)** | 파일별 사용/레거시 분류 기록 — 각 폴더 `legacy/`로 옮긴 파일과 그 이유, Spark 로직 정합성 검증 결과 |

## 꼭 알아야 할 핵심 제약

- **학습 기간은 2025년 한정** — 대여이력(2024-01~2026-06)에 비해 날씨·재고·인구가 2025년만 커버.
- **날씨는 예보가 아닌 관측치** — 실제 예보 API 연동은 보류 상태 (train-serve skew 한계 있음).
- **생활인구는 250m 격자 기준** — 격자 ID를 좌표로 역산해 정류소와 직접 매칭 (`make_dataset/legacy/grid.py`, 1차정제 전용).
- **단일 시점 예측은 실시간 데이터 결측에 대비한 fallback 내장** — [inference/DESIGN.md](inference/DESIGN.md) 참고.
- **타임존은 KST(Asia/Seoul)로 통일** — [../lib/ml_common/README.md](../lib/ml_common/README.md) 참고.

각 제약의 배경과 대안 검토 과정은 [history.md](history.md)/각 폴더 `DESIGN.md`에 자세히 정리돼 있다.
