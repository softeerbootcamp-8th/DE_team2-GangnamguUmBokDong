# Training 설계

> 현재 상태: **운영 코드와 일치**
>
> 기준 구현: `ml/training/`
>
> 실행 방법: [training README](../../../ml/training/README.md)

Training은 Feature Engine의 대여·반납 multi-horizon mart를 읽어 서로 독립된
LightGBM 모델을 학습한다. 모든 결과는 새 immutable archive에 challenger로 저장하며,
학습 완료만으로 운영 serving release를 변경하지 않는다.

## 1. 학습 단위와 모델

| 모델 | 라벨 | 평균 모델 | 구간 모델 | 추가 처리 |
|---|---|---|---|---|
| 대여 | `rental_count` | Poisson | Quantile P10/P50/P90 | `rental_exposure` offset |
| 반납 | `return_count` | Poisson | Quantile P10/P50/P90 | 없음 |

`horizon`이 feature에 포함되므로 모델별 booster 하나가 horizon 1..12를 함께 학습한다.
대여와 반납은 feature mart, lag, booster, category, metrics, conformal correction을
공유하지 않는다.

대여 Poisson 학습은 `init_score=log(exposure)`를 사용한다. LightGBM 모델 파일에는
이 offset이 저장되지 않으므로 채점 시 `exposure × booster.predict(X)`로 복원해야
한다. 이 규칙은 `libs/ml_core/scoring.py`가 학습 평가와 추론에 공통 적용한다.

Quantile booster에는 exposure offset을 적용하지 않는다. validation의 conformity
score로 split-conformal correction을 계산하고 P10/P90 구간을 보정한다. 목표
coverage 기본값은 `CONFORMAL_TARGET_COVERAGE=0.80`이다.

## 1-1. LightGBM 분산 학습(Socket) — YARN Distributed Shell로 워커 기동

`train_common.py`는 `training/config.py`의 환경변수(`LGB_TREE_LEARNER`,
`LGB_NUM_MACHINES`, `LGB_MACHINE_RANK`, `LGB_MACHINES`, `LGB_LOCAL_LISTEN_PORT`,
`LGB_TIME_OUT`)로 LightGBM 자체 분산 학습(`tree_learner="data"`/`"voting"`)을
켠다. 기본값(`LGB_TREE_LEARNER="serial"`)은 여전히 기존과 동일한 단일 머신
학습이고, 분산은 명시적으로 켜야 한다.

- **왜 SynapseML이 아니라 LightGBM 자체 분산인가**: SynapseML은 `VectorAssembler`로
  피처를 재조립해야 하고, 이 프로젝트 핵심인 exposure offset(`init_score`,
  바로 위 문단 참고) 우회 구현이 필요해 리스크가 컸다. LightGBM 자체 분산은 지금
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
  [ADR-0008](../../adr/0008-yarn-distributed-shell-workers.md) 참고.
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
- **알려진 근사(추후 개선 여지)**: split-conformal correction(바로 위 문단 참고)은 검증셋
  전체가 아니라 대표 머신(rank 0)의 station 샤드만으로 계산된다 — 여러 머신의
  conformity score를 모아 합치려면 LightGBM 소켓 프로토콜 밖의 별도 집계 단계가
  필요해 지금은 범위 밖으로 남겨뒀다.
- **EMR 노드 수**: 피처마트(Spark)는 core 3대로 충분하고 분산 학습만 8대가
  필요해서, 클러스터를 껐다 켜지 않고 `ModifyInstanceGroups`로 그 자리에서
  늘린다. 줄이는 건 실행 중인 작업을 죽일 위험이 있어 하지 않는다 — 한 사이클
  안에서 한 번 8로 늘리면 그 사이클이 끝날 때까지 유지한다(피처마트 단계에서
  유휴 노드 비용은 감수).

## 2. 시간 해상도 계약

내장 프로필은 **g20/r20/a20 학습 + 5분 서빙**이다.

- `GRID_TICK_MINUTES`와 `ROLLING_TICK_MINUTES`는 같아야 한다.
- 지원 grid는 `{5, 10, 15, 20, 30, 60}`분이다.
- `TRAIN_ANCHOR_TICK_MINUTES`는 base grid 이상의 배수이며 한 시간과 하루를 나눈다.
- `SERVING_TICK_MINUTES=5`는 학습 grid와 별도의 고정 운영 계약이다.
- feature mart를 만든 effective profile과 학습 profile이 달라지면 경로 또는 계약
  검증에서 실패해야 한다.

해상도 실험의 자체 test 지표는 anchor 표본이 다르므로 직접 비교하지 않는다.
g20/a20, g5/a20, g5/a5는 동일한 독립 5분 test mart에서 비교해야 한다.

## 3. 학습 기간과 split

`TRAIN_WINDOW_START`와 `TRAIN_WINDOW_END`를 모두 지정하면 inclusive 고정 기간을
사용한다. 둘 다 없으면 `TRAIN_LOOKBACK_MONTHS`와
`TRAINING_SAFETY_MARGIN_DAYS`로 rolling window를 계산한다. 한쪽만 지정하거나 날짜가
잘못되면 실행 전에 실패한다.

train/valid/test는 `date=YYYY-MM-DD` partition 이름으로 결정한다. 같은 anchor의
서로 다른 horizon이 날짜 경계를 넘어갈 수 있으므로 평가일 주변 train 날짜를
`SPLIT_EMBARGO_DAYS`만큼 purge한다. valid와 test가 이 거리 안에 있어 같은 anchor를
공유할 수 있는 설정도 거부한다.

기본값은 모든 안전한 train 날짜와 모든 horizon을 사용한다. 자원 부족 시에만 다음
dial을 사용한다.

| 설정 | 동작 | 품질 비용 |
|---|---|---|
| `LGB_DEFER_VALID_DATASET=true` | valid Dataset을 학습 후 streaming 평가 | early stopping 없음 |
| `TRAIN_DAY_DIVISOR>1` | 결정적으로 일부 train 날짜만 사용 | 계절·요일 표본 감소 |
| `MAX_TRAIN_HORIZON<N` | 먼 horizon을 읽지 않음 | N 이후 품질 미검증 |

과거 `TRAIN_SAMPLE_FRAC`, `VALID_SAMPLE_FRAC`, `TEST_SAMPLE_FRAC`는 실제 I/O에
연결되지 않았으므로 제거됐으며 설정하면 실패한다.

## 4. 메모리 제한 학습

multi-horizon mart 전체를 pandas DataFrame 하나로 합치지 않는다.
`lazy_train_dataset.py`는 날짜 partition을 `lgb.Sequence`로 감싸 LightGBM이 요청할
때만 읽고, 작은 LRU cache에서 오래된 날짜를 비운다.

- feature: 날짜별 Arrow/Pandas chunk를 필요할 때만 로드한다.
- label·exposure·init score: 로컬 scratch memmap에 순차 기록한다.
- test: 날짜별로 예측하고 작은 label/prediction 배열만 합친다.
- valid: conformal 계산 시에도 날짜별 streaming prediction을 사용한다.

따라서 전체 feature 행렬의 RAM 상주는 피하지만 scratch disk와 S3 재조회 비용은
필요하다. 진행 상황과 peak RSS는 `TRAIN_PROGRESS_LOG_PATH`에 기록한다.

LightGBM socket 분산 설정은 코드에 존재하지만 기본값은 `serial`이다. 실제 다중
worker 네트워크·동시 기동·전체 conformity score 집계는 운영 검증이 완료된 경로가
아니므로 현재 표준 운영 방식으로 간주하지 않는다.

## 5. Checkpoint와 재개

`TRAIN_CHECKPOINT_INTERVAL_ROUNDS`가 양수이면 Poisson, Q10, Q50, Q90 phase별
Booster와 state를 archive의 `_checkpoints/` 아래에 저장한다. Booster 업로드 후
state를 갱신하므로 중간 업로드를 정상 checkpoint로 오인하지 않는다.

`TRAIN_RESUME_FROM_CHECKPOINT=true`일 때 다음 fingerprint가 모두 같아야 재개한다.

- 입력 데이터 경로와 split 날짜
- feature 목록과 effective profile
- LightGBM 파라미터
- 관련 핵심 코드 bytes

validation을 사용하는 phase는 best score, best iteration, patience도 복원한다.
완료 phase는 최종 Booster를 다시 읽어 건너뛰지만 최종 평가·conformal·metrics는
현재 실행에서 다시 계산한다. checkpoint는 serving release가 아니며 운영 추론에
노출되지 않는다.

## 6. Feature와 station category 계약

feature 순서와 dtype의 단일 기준은 `libs/ml_core/model_contract.py`다.
`station_no`는 전체 학습 데이터에서 한 번 정렬한 `CategoricalDtype`으로 고정한다.
split마다 category를 다시 만들면 같은 코드가 다른 정류소를 뜻할 수 있으므로
금지한다.

고정된 category 순서는 `{model_name}_station_categories.json`에 저장되고 model
snapshot에 포함된다. Inference는 snapshot의 exact bytes로 dtype을 복원한다.

## 7. Archive와 실험 추적

각 학습 실행은 충돌하지 않는
`{MODELS_ARCHIVE_PREFIX}/dt={MODEL_ARCHIVE_DATE}/{ML_PROFILE}/` prefix에 기록한다.
`MODEL_ARCHIVE_DATE`를 지정하지 않으면 같은 날 재실행도 겹치지 않는 unique ID를
생성한다.

모델별 필수 archive는 다음과 같다.

| artifact | 내용 |
|---|---|
| `*_poisson.txt` | 평균 예측 booster |
| `*_q10.txt`, `*_q50.txt`, `*_q90.txt` | quantile booster |
| `*_station_categories.json` | station category 순서 |
| `*_conformal_correction.json` | 구간 보정값 |
| `*_metrics.json` | 평가·모니터링 baseline |
| `*_profile.json` | 실제 적용된 effective profile |

동일한 params·metrics·artifact 사본을 MLflow에도 기록한다. MLflow는 비교와 관찰을
위한 experiment tracker이며 S3 archive나 serving authority를 대체하지 않는다.

## 8. Challenger 판정과 serving release

일반 학습은 champion 위치에 쓰지 않는다. `should_promote()`는 다음 두 조건을 모두
확인한다.

1. challenger의 `poisson_deviance_test`가 champion보다 나쁘지 않다.
2. calibrated P10–P90 coverage가 목표 coverage ± 허용 drift 범위 안이다.

legacy model별 champion pointer 전환은 같은 serving feature 계약 안에서만 허용된다.
서로 다른 rolling·embargo·grid·horizon 계약으로 한 모델만 바꾸는 것은 거부한다.

현재 운영 inference authority는 개별 champion pointer가 아니라
`models/serving-release/current.json`이 가리키는 rental/return pair다.
`publish_serving_release.py`는 다음을 모두 검증한 뒤 마지막에 단일 pointer CAS를
수행한다.

- rental/return archive의 필수 artifact와 model snapshot
- 두 모델의 effective serving contract 일치
- station profile의 grid와 category coverage
- station master의 `station_id ↔ station_no` 1:1 crosswalk
- 입력 object의 실제 bytes와 checksum

계약이 바뀌는 migration은 수동 `--allow-contract-change`가 필요하며 월별 자동
재학습 경로에는 연결하지 않는다. pointer 게시 전까지 archive와 snapshot은
challenger일 뿐 운영 모델이 아니다.

**분산 학습과의 연동**: `LGB_NUM_MACHINES>1`이면 `_DatePartitionSequence`가 날짜
파티션을 읽은 직후 `_shard_for_this_machine()`으로 이 머신 몫만 남기고 반환한다
(라벨/exposure memmap도 같은 필터를 통과한 행 기준). station_no
`CategoricalDtype`을 고정하는 `station_categories_for_dates()`는 이 필터와
무관하게 항상 전체 station 목록을 스캔한다 — 1-1번 항목 참고.

## 9. 월별 모니터링과 재학습

`monitor_performance.py`는 현재 모델의 저장된 test baseline과 최근 완결 월의 지표를
비교한다. Poisson deviance의 상대 악화와 coverage drift가 임계값을 넘으면 재학습
후보가 된다.

`monthly_retrain_check.py --execute`는 호환되는 후보 profile만 순서대로 시도한다.
각 후보는 별도 subprocess에서 Feature Engine과 학습을 실행하고, 기준을 통과하지
못하면 기존 포인터를 유지한다. feature 의미가 다른 profile은 무거운 Spark 작업을
시작하기 전에 제외한다.

자동 재학습의 model별 legacy pointer 승격과 pair serving release 게시는 같은 의미가
아니다. 운영 pair 교체에는 두 모델과 station dependency를 함께 검수한 release 게시가
필요하다.

## 10. 검증 기준

```bash
cd ml
./training/.venv/bin/python -m pytest training/tests/ ../libs/ml_core/tests -q
```

변경 시 다음 불변조건을 유지한다.

1. split purge 후 train/valid/test가 같은 anchor를 공유하지 않는다.
2. lazy loader 결과가 eager reference와 같다.
3. 대여 exposure offset이 학습·평가·추론에서 같은 의미다.
4. checkpoint는 fingerprint가 다르면 재사용되지 않는다.
5. archive 저장이 기존 champion/release pointer를 변경하지 않는다.
6. 불완전하거나 cross-contract인 model pair는 serving release가 되지 않는다.
7. serving release pointer CAS 실패 시 기존 release가 유지된다.
