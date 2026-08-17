# training — 설계 문서

실행 방법은 [README.md](README.md), 결정의 배경/시행착오는 [history.md](../history.md)를
참고. 이 문서는 "지금 코드가 왜 이렇게 짜여 있는지"에 집중한다.

## 1. 왜 학습은 Spark가 아니라 로컬 LightGBM인가

`feature_engine`은 Spark로 분산 처리하지만(EMR, 데이터 규모가 히스토리 길이에
비례해 계속 커짐), 학습은 항상 **최근 N개월만 잘라서** 단일 머신 LightGBM으로
돌린다 — 학습 데이터량이 히스토리 길이와 무관하게 고정되므로 확장성 문제가
없다. 처음엔 이 이유로 LightGBM 자체 분산 학습(Socket/MPI)이나
SynapseML(LightGBM-on-Spark)도 검토했지만 채택하지 않았었다(history.md 5번
항목) — EMR 클러스터를 쓴다고 학습이 자동으로 분산되는 게 아니라 별도
인프라/구현 부담이 컸기 때문. 이후 여러 해치 데이터로 확장 계획이 서면서
분산 학습 자체는 다시 쓰기로 했다(history.md 17번 항목) — 다만 여전히 "학습은
최근 N개월만 자른다"는 원칙은 유지하고, 그 잘린 데이터를 여러 머신에 나눠
학습 속도/메모리를 더 아끼는 용도로 쓴다.

## 1-1. LightGBM 분산 학습(Socket) — 코드는 준비, 인프라는 아직

`train_common.py`는 `training/config.py`의 환경변수(`LGB_TREE_LEARNER`,
`LGB_NUM_MACHINES`, `LGB_MACHINE_RANK`, `LGB_MACHINES`, `LGB_LOCAL_LISTEN_PORT`,
`LGB_TIME_OUT`)로 LightGBM 자체 분산 학습(`tree_learner="data"`/`"voting"`)을
켤 수 있다. 기본값은 `LGB_TREE_LEARNER="serial"`(단일 머신) — 워커 인프라가
아직 없어서(2026-08 기준) 코드만 먼저 준비해둔 상태다.

- **왜 SynapseML이 아니라 LightGBM 자체 분산인가**: SynapseML은 `VectorAssembler`로
  피처를 재조립해야 하고, 이 프로젝트 핵심인 exposure offset(`init_score`,
  §2 참고) 우회 구현이 필요해 리스크가 컸다. LightGBM 자체 분산은 지금
  `train_common.py`의 `lgb.Dataset`/`lgb.train` 호출에 파라미터만 얹으면 되고
  offset 로직을 그대로 재사용할 수 있다.
- **데이터 분배(data-parallel)**: LightGBM 소켓 분산은 전체 데이터를 자동으로
  나눠주지 않는다 — 각 머신이 미리 자기 몫만 들고 `lgb.train()`을 호출해야
  한다. `train_common._shard_for_this_machine()`이 `station_id`를 `zlib.crc32`
  해시로 머신 수만큼 나눠 배정한다(날짜 범위는 모든 머신에 동일 — station
  집합만 갈림). `hash()` 내장 함수는 안 씀 — `PYTHONHASHSEED`가 프로세스마다
  달라 머신 간 배정이 어긋날 수 있음.
- **모든 머신이 `lgb.train()`을 같은 횟수만큼 호출해야 함**: 소켓 핸드셰이크가
  전 머신 동기 호출을 전제로 하므로, Poisson + quantile 3개 총 4번의
  `lgb.train()` 호출을 어느 머신도 건너뛰면 안 된다(중간에 `return`하면 다른
  머신이 다음 호출에서 무한 대기). 그래서 파일 저장/최종 지표 계산만
  `LGB_MACHINE_RANK == 0`으로 막고, 학습 호출 자체는 항상 전 머신이 진행한다 —
  boosting이 끝나면 모든 머신의 booster가 동일(매 라운드 gradient를 네트워크로
  동기화)하므로 대표 머신 하나만 저장/평가하면 충분하다.
- **알려진 근사(추후 개선 여지)**: split-conformal correction(§3)은 검증셋
  전체가 아니라 대표 머신(rank 0)의 station 샤드만으로 계산된다 — 여러 머신의
  conformity score를 모아 합치려면 LightGBM 소켓 프로토콜 밖의 별도 집계 단계가
  필요해 지금은 범위 밖으로 남겨뒀다.
- **아직 안 한 것**: 실제 워커 인프라(머신 IP/포트, 방화벽/보안그룹, 동시 기동
  스크립트)는 준비되지 않았다 — 지금은 `LGB_NUM_MACHINES=1`(기본값)로 기존과
  동일하게 동작하는 것만 검증됨. 인프라가 서면 `LGB_MACHINES`에 실제
  `host:port` 목록을 넣고 각 머신에서 같은 스크립트를 동시에 띄워 검증해야 한다.

## 2. Poisson + exposure offset

대여 모델은 품절(stockout) 시간대 censoring을 `init_score=log(exposure)`
offset으로 보정한다. 반납은 거치대 상태와 무관하게 항상 성공하므로
`exposure_col=None`(순수 Poisson)으로 학습한다.

**LightGBM은 `init_score`를 모델 파일에 저장하지 않는다.** 학습 시
`eta = init_score + tree(x)`로 적합되지만 `predict()`는 `tree(x)`의 objective
역변환(Poisson이면 `exp(tree(x))`)만 반환한다 — 그래서 실제 예측값은 항상
`exposure * booster.predict(X)`로 직접 복원해야 한다(`ml_core/scoring.py`가
이 규칙을 지킴).

## 3. Quantile(P10/50/90) + split-conformal 보정

`objective='quantile'` 3개(alpha=0.1/0.5/0.9)를 별도로 학습한다(exposure offset은
적용 안 함 — quantile loss와 offset의 결합이 표준적이지 않음). 검증셋에서
conformity score(구간 밖으로 벗어난 정도)의 분위수를 correction으로 구해
`[p10-correction, p90+correction]`을 적용한다(`train_common._conformal_correction()`,
Romano et al. CQR) — 테스트셋 P10~P90 커버리지가 이론값(기본 0.80)에 더 가까워짐.

## 4. station_id 카테고리 — 학습/서빙이 반드시 같은 코드를 써야 함

`train_target()`이 전체 데이터 기준 station_id `CategoricalDtype`을 한 번만
고정하고 `{model_name}_station_categories.json`에 저장한다. split(train/valid/test)마다
따로 `astype("category")`하면 LightGBM 카테고리 코드(정수)가 어긋나 조용히
오염되는 흔한 실수라 명시적으로 피한다. `inference`는 `ml_core/model_contract.py`의
`load_station_dtype()`으로 이 파일을 그대로 읽어 인코딩을 재현한다 — 이 계약이
깨지면(둘이 다른 카테고리 순서를 쓰면) 모델이 station_id를 조용히 잘못
해석한다.

## 5. `ml_core/`으로 뺀 것과 이 폴더에 남은 것

학습(`train_target()`, split, conformal correction)과 서빙(`predict()`)이
정확히 같은 **모델 계약**(feature 목록, station_id 인코딩, 평가 지표 정의)을
써야 한다 — 그래서 `FEATURE_COLUMNS`/`station_categories_path`/`load_station_dtype`은
`ml_core/model_contract.py`로, `poisson_deviance`/`pinball_loss`는 `ml_core/metrics.py`로,
채점 로직(`predict()`)은 `ml_core/scoring.py`로 뺐다. 이 폴더에는 학습에만
필요한 것(`_split`, `_prepare_xy`, `_conformal_correction`, `train_target()`
자체, LightGBM 파라미터 튜닝)만 남는다.

`monitor_performance.py`/`scripts/compare_baselines.py`가 `ml_core/scoring.py`의
`predict()`를 가져다 쓰는 이유도 같다 — "저장된 모델로 채점"하는 로직은
서빙 전용이 아니라 평가/모니터링에서도 똑같이 필요하다.

## 6. 월별 성능 모니터링 — 고정 baseline이 아니라

`monitor_performance.py`는 "절대 수치"가 아니라 baseline(마지막 학습 시점 테스트
성능) 대비 **상대 악화율**로 재학습 여부를 판단한다. 절대 임계값을 안 쓰는 이유:
Poisson deviance가 계절성에 강하게 비례한다는 걸 실측으로 확인했다(1월 0.890 vs
6월 1.259, +42%) — 고정 baseline 대비로 여름철을 평가하면 모델이 멀쩡해도
"재학습 필요"로 오탐이 난다. 임계값(deviance 10%, 커버리지 15%p)의 근거는
실측 노이즈 바닥(재학습 run-to-run 편차 0.3~0.5%, embargo 스윕 편차 0.6%)보다
한참 위로 잡은 것 — `ml_core/common_config.py` 주석 참고.

**아직 미구현**: 고정 baseline 대신 최근 N개월 이동평균(rolling baseline) 대비로
바꾸는 개선(history.md 9번 항목 마지막 문단) — 계절이 서서히 바뀌는 건 흡수하고
급격한 이탈만 잡도록.

## 7. 실험 격리 원칙

`scripts/run_embargo_sweep.py`, `scripts/build_embargo_candidate.py` 같은 스윕
스크립트는 `feature_engine`/`ml_core`/`training`을 전부 가져다 쓰지만, 산출물은
항상 `models/experiments/{run_id}/`, `data/processed_v2/experiments/`처럼 챔피언
경로와 분리된 곳에 쓴다 — 스윕이 실패하거나 중간에 멈춰도 챔피언 아티팩트는
안전하다. `experiment_log.py`가 실행마다 (run_id, git_sha, dirty, params, metrics)를
`manifest.jsonl`에 append해서, 나중에 정식 experiment tracker(MLflow 등)로 옮길 때도
그대로 이관 가능한 최소 스키마를 유지한다.
