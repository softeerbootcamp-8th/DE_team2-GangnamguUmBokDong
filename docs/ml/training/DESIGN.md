# training — 설계 문서

실행 방법은 [README.md](../../../ml/training/README.md), 결정의 배경/시행착오는 [history.md](../history.md)를
참고. 이 문서는 "지금 코드가 왜 이렇게 짜여 있는지"에 집중한다.

현재 시간 해상도 기본 계약은 **g20/r20/a20 학습 + 5분 서빙**이다. g/r은
`{5, 10, 15, 20, 30, 60}`분 중 같은 값을 쓰고, formal
`TRAIN_ANCHOR_TICK_MINUTES`(a)는 생략 시 g와 같으며 명시 시 g 이상인
배수이면서 1시간과 1일을 나눠야 한다. 모델 grid/anchor는 학습 데이터의
해상도이고 `SERVING_TICK_MINUTES=5`는 별도 고정 계약이다.

해상도 비교는 A=g20/r20/a20, B=g5/r5/a20, C=g5/r5/a5 세 arm으로 한다.
A와 B의 공통 20분 anchor는 feature/label parity를 확인하고, 정확도는 세 모델
모두 같은 독립 5분 test mart에서 평가한다. 각 arm의 자체 test split 지표는
행 집합이 다르므로 서로 직접 비교하지 않는다.

## 1. 왜 학습은 Spark가 아니라 로컬 LightGBM인가

`feature_engine`은 Spark로 분산 처리하지만(EMR, 데이터 규모가 히스토리 길이에
비례해 계속 커짐), 운영 재학습은 **최근 N개월만 잘라서** 단일 머신 LightGBM으로
돌린다 — 학습 데이터량이 히스토리 길이와 무관하게 고정되므로 확장성 문제가
없다. 단, 최초 챔피언은 `TRAIN_WINDOW_START=2025-01-01`과
`TRAIN_WINDOW_END=2025-12-31`을 feature_engine/training 양쪽에 함께 주어 2025년
전체를 exact window로 사용한다. 두 변수가 없을 때만 최근 N개월 rolling 규칙을
적용하며, 한쪽만 있거나 오형식·역전이면 fail-closed한다. 처음엔 이 이유로
LightGBM 자체 분산 학습(Socket/MPI)이나
SynapseML(LightGBM-on-Spark)도 검토했지만 채택하지 않았었다(history.md 5번
항목) — EMR 클러스터를 쓴다고 학습이 자동으로 분산되는 게 아니라 별도
인프라/구현 부담이 컸기 때문. 이후 여러 해치 데이터로 확장 계획이 서면서
분산 학습 자체는 다시 쓰기로 했다(history.md 17번 항목) — 다만 여전히 "학습은
최근 N개월만 자른다"는 원칙은 유지하고, 그 잘린 데이터를 여러 머신에 나눠
학습 속도/메모리를 더 아끼는 용도로 쓴다.

## 1-1. LightGBM 분산 학습(Socket) — YARN Distributed Shell로 워커 기동

`train_common.py`는 `training/config.py`의 환경변수(`LGB_TREE_LEARNER`,
`LGB_NUM_MACHINES`, `LGB_MACHINE_RANK`, `LGB_MACHINES`, `LGB_LOCAL_LISTEN_PORT`,
`LGB_TIME_OUT`)로 LightGBM 자체 분산 학습(`tree_learner="data"`/`"voting"`)을
켠다. 기본값(`LGB_TREE_LEARNER="serial"`)은 여전히 기존과 동일한 단일 머신
학습이고, 분산은 명시적으로 켜야 한다.

- **왜 SynapseML이 아니라 LightGBM 자체 분산인가**: SynapseML은 `VectorAssembler`로
  피처를 재조립해야 하고, 이 프로젝트 핵심인 exposure offset(`init_score`,
  §2 참고) 우회 구현이 필요해 리스크가 컸다. LightGBM 자체 분산은 지금
  `train_common.py`의 `lgb.Dataset`/`lgb.train` 호출에 파라미터만 얹으면 되고
  offset 로직을 그대로 재사용할 수 있다.
- **워커를 어떻게 띄우는가(YARN Distributed Shell)**: 학습용 EC2가 없어지고
  EMR도 m4.large만 허용되는 제약에서, SSM은 이 계정 SCP가 전면 차단하고
  SSH도 자동화하려면 SSM만큼의 신규 인프라가 필요해서 둘 다 안 쓴다. 대신
  피처마트 Spark 잡과 똑같은 경로(EMR Step)로 YARN Distributed Shell을
  제출해 `LGB_NUM_MACHINES`개 컨테이너를 core 노드에 뿌린다 — 각 컨테이너의
  진입점은 `training/scripts/yarn_worker_bootstrap.py`로, 자기 host:port를
  S3의 정해진 위치에 등록하고 전체가 다 모일 때까지 기다린(barrier) 뒤 자기
  `LGB_MACHINE_RANK`와 전체 `LGB_MACHINES` 문자열을 계산해 실제 학습 스크립트를
  실행한다(YARN은 컨테이너 배치만 해줄 뿐 서로의 주소를 알려주는 기능은 없어
  이 등록/barrier는 직접 구현했다). 배경과 대안 비교는
  [ADR-0007](../../adr/0007-yarn-distributed-shell-workers.md) 참고.
- **데이터 분배(data-parallel)**: LightGBM 소켓 분산은 전체 데이터를 자동으로
  나눠주지 않는다 — 각 머신이 미리 자기 몫만 들고 `lgb.train()`을 호출해야
  한다. `lazy_train_dataset._shard_for_this_machine()`이 `station_no`를
  `zlib.crc32` 해시로 머신 수만큼 나눠 배정한다(날짜 범위는 모든 머신에 동일 —
  station 집합만 갈림). `hash()` 내장 함수는 안 씀 — `PYTHONHASHSEED`가
  프로세스마다 달라 머신 간 배정이 어긋날 수 있음. **단, station_no
  `CategoricalDtype`을 고정하는 `station_categories_for_dates()` 호출은 이
  샤딩과 무관하게 항상 전체 station 목록을 스캔한다** — 머신마다 다른
  station 부분집합만 보고 카테고리를 고정하면 머신 간 카테고리 코드가
  어긋나서 `inference`가 조용히 잘못된 station을 읽게 된다.
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
- **EMR 노드 수**: 피처마트(Spark)는 core 3대로 충분하고 분산 학습만 8대가
  필요해서, 클러스터를 껐다 켜지 않고 `ModifyInstanceGroups`로 그 자리에서
  늘린다. 줄이는 건 실행 중인 작업을 죽일 위험이 있어 하지 않는다 — 한 사이클
  안에서 한 번 8로 늘리면 그 사이클이 끝날 때까지 유지한다(피처마트 단계에서
  유휴 노드 비용은 감수).

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

## 4. station_no 카테고리 — 학습/서빙이 반드시 같은 코드를 써야 함

모델 feature는 station_id(텍스트, "ST-2565")가 아니라 station_no(정수
일련번호)다 — Parquet dictionary encoding이 `to_pandas()`에서 안 살아남아
매 학습 읽기마다 object dtype 문자열 배열을 통째로 만드는 비용이 있었는데,
station_no는 처음부터 정수라 그 비용 자체가 없다(station_id는 출력/CLI 식별
용도로만 계속 쓰임). `train_target()`이 전체 데이터 기준 station_no
`CategoricalDtype`을 한 번만 고정하고 `{model_name}_station_categories.json`에
저장한다. split(train/valid/test)마다 따로 `astype("category")`하면 LightGBM
카테고리 코드(정수)가 어긋나 조용히 오염되는 흔한 실수라 명시적으로 피한다.
`inference`는 `ml_core/model_contract.py`의 `load_station_dtype()`으로 이
파일을 그대로 읽어 인코딩을 재현한다 — 이 계약이 깨지면(둘이 다른 카테고리
순서를 쓰면) 모델이 station_no를 조용히 잘못 해석한다.

## 5. `ml_core/`으로 뺀 것과 이 폴더에 남은 것

학습(`train_target()`, split, conformal correction)과 서빙(`predict()`)이
정확히 같은 **모델 계약**(feature 목록, station_id 인코딩, 평가 지표 정의)을
써야 한다 — 그래서 `FEATURE_COLUMNS`/`station_categories_path`/`load_station_dtype`은
`ml_core/model_contract.py`로, `poisson_deviance`/`pinball_loss`는 `ml_core/metrics.py`로,
채점 로직(`predict()`)은 `ml_core/scoring.py`로 뺐다. 이 폴더에는 학습에만
필요한 것(`_dates_for_split`, `_conformal_correction`, `train_target()` 자체,
`lazy_train_dataset.py`의 S3 지연 로딩, LightGBM 파라미터 튜닝)만 남는다.

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

## 7. 실험 격리 원칙 — S3 아카이브 + MLflow (2026-08 갱신)

예전엔 `scripts/run_embargo_sweep.py`/`build_embargo_candidate.py` 같은 별도
스윕 스크립트와 `experiment_log.py`가 만드는 `manifest.jsonl`로 실험을
격리·기록했다 — 지금은 둘 다 삭제됐고, 격리/기록 방식이 다음 두 가지로
정리됐다:

- **격리**: 모든 학습은 `train_target()` 호출 한 번마다 S3 아카이브
  (`libs/ml_core/paths.archive_models_prefix(date, profile_name)`)에만 쓴다 —
  챔피언 자리에는 절대 직접 안 쓴다(§8 참고). 실험이 몇 번을 실패하거나
  중간에 멈춰도 챔피언 아티팩트는 그 자체로 안전하다.
- **기록**: (run_id, params, metrics, artifacts)를 이제 정식
  experiment tracker인 [MLflow](../MLFLOW_SETUP.md)가 기록한다 — 예전
  `manifest.jsonl`이 하던 일을 그대로 대체하되, 파일을 직접 열어 파싱하는
  대신 웹 UI에서 여러 시도(예: `TRAIN_DAY_DIVISOR`/`MAX_TRAIN_HORIZON` 조합)를
  나란히 비교할 수 있게 됐다. 자세한 설정/기록 내용은
  [MLFLOW_SETUP.md](../MLFLOW_SETUP.md).

## 8. 챔피언/챌린저 승격 — 파일 복사가 아니라 포인터 전환 (2026-08 신규)

학습(`train_rental_model`/`train_return_model`)은 항상 S3 아카이브에만 쓰고,
"지금 서빙 중인 모델"(챔피언)은 별도 포인터 객체
(`champion/{model_name}.json`, `ml_core.paths.write_champion_pointer()`/
`read_champion_prefix()`)가 어느 archive_prefix를 가리키는지로 정해진다.

최초 챔피언은 각 학습 CLI의 명시적 `--promote-if-no-champion`으로만 만든다.
학습 성공 후에도 `promotion.bootstrap_challenger()`가 일반 승격과 같은 effective
profile 계약 검증을 거치며, 같은 모델 포인터가 이미 있으면 오류로 중단해 기존
챔피언을 덮지 않는다. 대여/반납을 독립적으로 부트스트랩하므로 첫 모델 성공 뒤
둘째 모델만 실패해도 실패한 명령만 안전하게 재실행할 수 있다.

**왜 파일 복사가 아니라 포인터인가**: 예전엔 승격할 때 archive의 파일 8개
(booster 4개 + station_categories/conformal_correction/metrics/profile)를
챔피언 prefix로 하나씩 복사했다 — S3는 여러 키에 걸친 트랜잭션을 지원하지
않으므로, 복사가 절반쯤 끝난 순간 inference가 실행되면 booster는 새 버전인데
station_categories는 옛 버전인 식으로 섞인 모델을 읽을 수 있었다(station_no
카테고리 코드가 학습 시점의 정렬 순서에 의존해서, 섞이면 성능 저하가 아니라
엉뚱한 정류소에 대한 예측이 조용히 나감). archive 자체는 immutable이므로
포인터 하나만 원자적으로 바꾸면 파일을 복사할 필요가 아예 없다.

`training.promotion.should_promote(challenger_metrics, champion_metrics)`이
판정한다 — 챔피언이 없으면 무조건 승격(부트스트랩). 있으면 **둘 다** 만족해야
승격: `poisson_deviance_test`가 챔피언보다 나쁘지 않고(작거나 같고),
`p10_p90_coverage_calibrated_test`가 목표 커버리지(§6와 같은
`common_config.CONFORMAL_TARGET_COVERAGE`) ± 허용 드리프트
(`COVERAGE_DRIFT_THRESHOLD`) 범위 안 — 승격 전용 새 절대 임계값을 따로 만들지
않고 §6이 이미 근거를 댄 상대 임계값을 재사용한다. `promote_challenger()`는
포인터를 쓴 뒤 `read_champion_prefix()`/`load_boosters()`/
`load_conformal_correction()` 세 캐시를 전부 비운다 — 재학습해봤더니 구려서
같은 프로세스 안에서 재학습→재승격을 반복하는 코드가 있다면, 재승격 직후
다음 채점부터 셋이 전부 새 archive로 일관되게 나오게 하기 위함이다(하나만
비우면 셋 중 일부만 새 값을 보는 더 나쁜 불일치가 생긴다 — 실측 확인됨,
`libs/ml_core/paths.py`의 `read_champion_prefix()` docstring 참고).

`scripts/monthly_retrain_check.py`가 `monitor_performance.check_all_models()`로
재학습 필요 여부를 판정한 뒤, 필요하면 후보 프로필들을 순서대로 재학습
(별도 subprocess)해보고 `should_promote()`/`promote_challenger()`로 이어간다 —
어느 프로필도 기준을 못 넘으면 챔피언은 그대로 두고 조용히 종료한다.

## 9. 학습 테이블이 로컬 RAM보다 커질 때 — 날짜 파티션 단위 지연 로딩 (2026-08 전면 개편)

multi-horizon 테이블은 원본 tick 테이블의 최대 `HORIZON_COUNT`배 행 수라
("horizon을 feature로" 설계, [feature_engine/DESIGN.md](../feature_engine/DESIGN.md) §7 참고),
과거 20분 base/anchor·full horizon·2025년 전체 기준으로도 실측 8억 행대까지 커졌다 — 통째로 하나의
pandas DataFrame(float64/int64 컬럼 13개)으로 읽으면 원본만 수십GB라 로컬(RAM 18GB)
에서 반복적으로 OOM이 났다. 기본은 20분 anchor이며 필요하면 formal 설정으로
5분 base/anchor도 선택할 수 있다. 어느 해상도든 날짜·계절·기상 다양성과
`HORIZON_COUNT` 전체를 우선 보존하므로 `TRAIN_DAY_DIVISOR`로 날짜를
솎아내는 방식은 기본값에서 뺐다. 대신 `train_common.py`가 데이터를 한 번에 로드하지 않고,
**`lazy_train_dataset.py`가 날짜 파티션(`date=YYYY-MM-DD/`) 단위로 S3를 지연
조회**한다.

핵심 아이디어는 LightGBM `lgb.Sequence` API가 `Dataset.construct()` 중에 각
Sequence를 필요할 때만(그것도 두 단계 — 표본 추출용 개별 인덱스, 그 다음 실제
적재용 연속 슬라이스 — 로) 접근한다는 점을 이용하는 것이다: 날짜 하나를
`_DatePartitionSequence`로 표현해 `__getitem__`이 호출될 때만 그 날짜의 S3
파일을 읽고, 공유 LRU 캐시(`ChunkCache`, 기본 최대 2개)로 오래된 날짜를
비워서 항상 최대 1~2개 날짜분만 메모리에 남긴다(캐시에서 밀려난 날짜가 나중에
다시 필요해지면 재조회 — 메모리 대신 네트워크 I/O를 쓰는 트레이드오프).

- **train/valid**: `build_lazy_dataset()`이 이 방식으로 Sequence 기반 `lgb.Dataset`을
  만든다. 라벨(+exposure)은 날짜 하나씩 읽어 삭제 예약된 로컬 scratch memmap에
  순서대로 기록한다. `lgb.Dataset(label=...)`은 전체 길이의 1차원 배열을 구성
  시점에 요구하지만, disk-backed ndarray로 같은 인터페이스를 제공해 전체 기간의
  pandas/Arrow 합본과 numpy 재복사를 피한다. 대여 init-score도 별도 memmap에서
  계산한다.
- **test**: 학습에 안 쓰이고 `predict()`/지표 계산에만 쓰이므로 `Dataset`으로
  만들지 않는다 — `predict_over_dates()`가 날짜별로 그 청크만 읽어 즉시
  predict한 뒤, 큰 feature 행렬은 버리고 작은(1D) 예측값/라벨 배열만
  이어붙인다. valid도 학습 후 conformal correction 계산에 같은 함수를 다시
  쓴다(학습용 Sequence 적재와는 별개 시점이라 청크를 한 번 더 읽음).

단일 머신에서 native train/valid Dataset을 동시에 유지하는 것만으로 메모리
보호선을 넘으면 `LGB_DEFER_VALID_DATASET=true`를 쓸 수 있다. 이 모드는 train
전체로 요청한 boosting round를 고정 실행한 뒤 Dataset을 해제하고, valid 전체를
날짜별 streaming predict해서 지표와 conformal correction을 계산한다. 날짜나
horizon을 줄이는 샘플링은 아니지만 학습 중 valid Dataset이 없으므로 early
stopping은 사용하지 않는다. 기본값은 false여서 기존 학습·조기 종료 계약은
그대로 유지된다.

날짜(train/valid/test 소속)는 여전히 `TRAIN_DAY_DIVISOR`/`VALID_DAYS_OF_MONTH`/
`TEST_DAYS_OF_MONTH`로 정해지지만, `_dates_for_split()`가 Spark의 `date=` 파티션
이름 자체(day-of-month를 문자열에서 바로 뽑음)만으로 계산한다 — 데이터를 전혀
읽지 않는 순수 캘린더 연산이라 예전 `_split()`(로드된 df의 `day` 컬럼을
역산)보다 더 이르게, 더 싸게 구간을 확정한다. `TRAIN_DAY_DIVISOR`는 기본값
1(=날짜 다운샘플링 없음, 1년 전체)이고, 로컬 RAM이 급하게 부족한 특수 상황에서만
2, 3, 5로 올리는 비상 dial로 남아있다. `horizon`은 여전히 같은 날짜 파티션 안에
1..`HORIZON_COUNT`가 섞여 있어 `filters=[("horizon", "<=", MAX_TRAIN_HORIZON)]`
(pyarrow row-group 필터)로 따로 거른다.

`TRAIN_SAMPLE_FRAC`/`VALID_SAMPLE_FRAC`/`TEST_SAMPLE_FRAC`는 실제 로더에 연결되지
않은 채 설정만 존재했던 가짜 dial이라 제거했다. 설정 시 명시적으로 실패하며,
OOM 폴백은 실제 I/O를 줄이는 위 두 옵션만 지원한다. 날짜를 줄이면 계절·요일
표본이 감소하고 horizon을 줄이면 먼 구간을 아예 학습하지 않는 품질 tradeoff가 있다.

같은 anchor의 horizon 행이 자정을 넘어 서로 다른 target `date`에 저장될 수 있으므로,
평가일 전후 `SPLIT_EMBARGO_DAYS`(현재 horizon/target 기준 최소 1일)는 train에서
purge한다. valid/test가 서로 이 거리 안에 있으면 두 평가셋도 같은 anchor를 공유할
수 있어 설정 오류로 실패한다. 이 embargo 없이 day-of-month만 인터리브하면 early
stopping과 conformal correction이 같은 anchor 정보를 간접 공유해 낙관적으로 보인다.

로드가 실제로 진행 중인지 확인하려면 `TRAIN_PROGRESS_LOG_PATH`(기본
`training_progress.log`)를 tail — 날짜 청크 하나가 로드될 때마다(및 사전 스캔
파일 완료마다) 그 시점 peak RSS를 남긴다(표준출력과 별개 채널).

### 장시간 boosting checkpoint

`TRAIN_CHECKPOINT_INTERVAL_ROUNDS=N`과 `TRAIN_RESUME_FROM_CHECKPOINT=true`를
사용하면 Poisson/Q10/Q50/Q90 phase마다 N round 간격의 Booster와 작은 state
pointer를 immutable model archive 아래에 남긴다. state는 Booster 업로드가 끝난
뒤에만 갱신되어 중간 업로드를 재개 대상으로 선택하지 않는다. 학습 데이터·split·
effective profile·LightGBM 파라미터·핵심 코드 fingerprint가 기존 state와 모두
같을 때만 재개한다.

재개 시 Dataset은 다시 구성해야 하지만 완료된 boosting round는 반복하지 않는다.
validation을 사용하는 경로는 최고 점수, 최고 iteration, patience 상태도 함께
복원한다. 완전히 끝난 phase는 최종 Booster를 다시 로드해 건너뛰고 평가와 metrics는
현재 프로세스에서 다시 계산한다. 부분 checkpoint는 serving release가 참조하지
않으므로 중단된 실행이 현재 서빙 모델을 바꾸지 않는다.

**분산 학습과의 연동**: `LGB_NUM_MACHINES>1`이면 `_DatePartitionSequence`가 날짜
파티션을 읽은 직후 `_shard_for_this_machine()`으로 이 머신 몫만 남기고 반환한다
(라벨/exposure memmap도 같은 필터를 통과한 행 기준). station_no
`CategoricalDtype`을 고정하는 `station_categories_for_dates()`는 이 필터와
무관하게 항상 전체 station 목록을 스캔한다 — 1-1번 항목 참고.
