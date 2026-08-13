# common — 세 인스턴스가 공유하는 로직

`make_dataset`/`training`/`inference`가 서로 다른 인스턴스에서 따로 배포되지만,
아래 계약(파라미터·경로·핵심 알고리즘)만큼은 세 쪽이 정확히 같은 값/로직을
써야 한다. 여기 모아두지 않으면 한쪽만 고치고 잊어버려 조용히 갈라지는
(train-serving skew와 같은 종류의) 사고가 난다.

이 폴더는 순수 pandas 로직(+ 일부 lightgbm)만 담는다 — `make_dataset/spark/`는
pyspark 의존성이 있어 별도로 분리돼 있고, `common/common_config.py`만 예외적으로
아주 가벼운 상수 모듈이라 Spark 쪽도 함께 참조한다.

## 파일별 역할

| 파일 | 담당 | 누가 쓰나 |
|---|---|---|
| `common_config.py` | 하이퍼파라미터/censoring 파라미터 — **프로필 로더** | 전부(`make_dataset`, `make_dataset/spark`, `training`, `inference`) |
| `profiles/*.json` | 프로필 파일(파라미터 조합) | `common_config.py`가 읽음 |
| `paths.py` | `data/processed_v2/*.parquet` 산출물 경로, `MODELS_DIR` | 전부 |
| `rolling_window_features.py` | point-in-time censoring 핵심 로직(차분 배열, as-of 조회) | `make_dataset`(배치), `inference`(서빙 시뮬레이션) |
| `trip_events.py` | 대여이력 원본 로딩 + station_no 정규화 | `make_dataset`(배치), `inference`(실시간 시뮬레이션) |
| `model_contract.py` | `FEATURE_COLUMNS`(모델 입력 스키마), station_id 카테고리 저장/로드 | `training`(학습), `inference`(서빙) |
| `metrics.py` | poisson deviance, pinball loss | `training`, `common/scoring.py` |
| `scoring.py` | 저장된 booster로 채점(`predict()`) | `inference`, `training/monitor_performance.py`, `training/legacy/scripts/compare_baselines.py` |

## 프로필 시스템

`common_config.py`는 하드코딩 대신 `ML_PROFILE` 환경변수(기본값 `"default"`)로
`common/profiles/{ML_PROFILE}.json`을 읽는다. 파라미터 조합(window/embargo/tick,
LightGBM 하이퍼파라미터 등)을 프로필 파일로 미리 만들어두고 환경변수 하나로
통째로 바꿔 낄 수 있다. **본 서비스는 `default.json`(챔피언) 하나만 쓴다** —
`common/profiles/`에는 그것만 있다.

```bash
ML_PROFILE=default ./.venv-spark/bin/python -m make_dataset.spark.run_pipeline
```

개별 환경변수(예: `ROLLING_EMBARGO_MINUTES=45`)는 프로필 값 위에 추가로
덮어쓸 수 있다 — 우선순위는 **개별 환경변수 > 프로필 파일**. 새 프로필을
추가하려면 `profiles/default.json`을 복사해서 원하는 값만 바꾸면 된다.
`common/legacy/profiles/embargo45.json`은 이 메커니즘 자체를 검증할 때 쓴
예시 프로필(챌린저 파라미터 튜닝용)이라 실제로 서비스가 읽지 않는다 — legacy로
분류.

## 타임존

이 프로젝트의 타임존은 **KST(Asia/Seoul)**로 통일한다. Spark 세션을 새로 만드는
코드는 반드시 `TZ=Asia/Seoul` env(SparkSession 생성 **전**)와
`spark.sql.session.timeZone=Asia/Seoul`을 **같이** 설정해야 한다
(`make_dataset/spark/spark_session.py` 참고). 초 단위 정수 ↔ 타임스탬프 왕복이
필요한 Spark 코드는 `F.unix_timestamp()`/`F.timestamp_seconds()`를 직접 쓰지 말고
`make_dataset/spark/rolling_window_features.py`의 `_unix_seconds_ntz()`/
`_seconds_to_ntz()`를 쓸 것 — 세션 타임존과 무관하게 정확한 왕복을 보장한다
(모듈 docstring에 근거 설명 있음).

## 검증

```bash
cd ml
./.venv/bin/python -m pytest common/tests/ -q
```
