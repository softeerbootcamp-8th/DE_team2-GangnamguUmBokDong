# Inference 설계

> 현재 상태: **운영 코드와 일치**
>
> 운영 진입점: `inference.publication_cli`
>
> 실행 방법: [inference README](../../../ml/inference/README.md)

Inference는 pinned serving release의 대여·반납 모델로 전체 서빙 대상 정류소의
12개 horizon을 계산하고, 검증된 결과를 immutable snapshot으로 게시한다. 예측값을
계산하는 것뿐 아니라 입력·모델·결과의 정확한 identity와 완전성을 보장하는 것이
운영 경로의 책임이다.

## 1. 실행 경로

두 예측 경로의 목적은 다르다.

| 경로 | 용도 | Authority 여부 |
|---|---|---|
| `predict_common.py`, `predict_{rental,return}_demand.py` | 기존 feature mart 조회·백테스트 | 아님 |
| `predict_single.py` | 단일 정류소 또는 전체 정류소의 실시간 feature 조립·채점 | 계산 엔진 |
| `publication_cli.py` → `publication.py` | serving plan 기반 운영 추론·검증·게시 | **정식 authority** |

Airflow는 5분마다 도는 단일 `realtime_tick` DAG 하나로 이 체인을 실행한다(과거에는
날씨 필요 시각에 맞춰 4개 cron/DAG로 쪼갰으나, 서로 다른 DAG의 `max_active_runs`가
겹칠 때 생기는 경합을 없애기 위해 다시 하나로 합쳤다 — `airflow/dags/realtime_tick.py`
참고).

```text
collector / normalizer
        │
        ▼
prepare_serving_plan
        │ exact plan URI + SHA-256
        ▼
run_inference
        │ immutable inference manifest
        ▼
finalize_serving → urgency → routes
```

책임 경계는 다음과 같다. Prepare는 Gold projection·station scope·final transaction에
필요한 authority를 고정한다. Inference는 rental history·생활인구·모델용 horizon별
weather를 포함한 실제 ML 입력 선택과 provenance authority를 담당한다. 두 단계가 함께
사용하는 serving release와 `station_master_enriched`만 plan v3 exact identity로
명시적으로 결합한다.

날씨가 필요한 tick에서는 초단기실황·초단기예보·단기예보 collector가 같은 DAG의
선행 task로 실행된다. 수집 실패 시에는 `ALL_DONE` gate를 지나 이전에 게시된 날씨로
계속할 수 있지만, serving plan·정류소·대여이력·normalizer 의존성은 우회하지 않는다.

### Prepare와 inference의 authority 경계

두 task는 같은 입력 준비 작업을 앞뒤로 나눈 것이 아니라 서로 다른 authority를
소유한다.

| Task | 소유하는 authority | 소유하지 않는 것 |
|---|---|---|
| `prepare_serving_plan` | Gold station·stock·weather projection, expected station scope, 기존 Gold/RDS state와 final transaction 전제, 두 task가 공유하는 serving release·enriched master exact identity | rental history, horizon별 모델 날씨·생활인구·stockout source resolution |
| `run_inference` | plan이 pin한 shared identity 소비, actual model-input 선택, 실제 읽은 non-model S3 bytes와 prediction provenance | Gold station·stock·weather projection과 RDS publication transaction |

Prepare가 보존하는 rental·return support ID ref는 expected station scope를 계산하고
inference model support와 대조하기 위한 것이다. Serving plan v3는 그 support ID를
만든 exact serving release와 eligible station 계산에 쓴 enriched master identity도 함께
pin한다. Inference는 pointer/latest를 다시 선택하지 않고 plan의 shared identity를
검증·소비한 뒤, 자신이 선택한 rental history·날씨·생활인구 등 실제 model
input을 immutable inference manifest에 기록한다.

## 2. Pinned serving release

운영 실행은 mutable champion key를 채점 도중 다시 읽지 않는다.

1. serving plan이 지정한 logical time과 expected station set을 읽는다.
2. prepare가 plan에 기록한 exact serving-release manifest URI·SHA를 읽는다.
3. release에 결합된 rental/return model snapshot, station categories, effective
   profile, station fallback profile을 실제 bytes와 checksum으로 검증한다.
4. 두 모델과 fallback profile을 한 실행 동안 고정한다.
5. 현재 `common_config`와 artifact의 serving feature 계약이 다르면 실패한다.

Inference는 mutable current pointer를 다시 해석하지 않는다. 따라서 prepare 뒤
champion pointer가 바뀌어도 해당 tick은 plan이 처음 pin한 release로 끝난다. 이 구조는
대여·반납 모델 또는 category 순서가 서로 다른 버전으로 섞이는 것을 막는다. 모델
feature 순서와 dtype의 단일 기준은
`libs/ml_core/model_contract.py`다.

Serving plan v3는 prepare가 eligible station 계산에 실제 사용한
`station_master_enriched` key·SHA도 기록한다. Inference는 latest를 다시 탐색하지 않고
그 exact Parquet을 feature의 capacity·좌표·grid 기준으로 사용한다. v2 plan은 이미
생성된 inference의 finalize/replay에는 호환되지만, 새 inference 실행은 두 shared
identity가 없으므로 fail-closed하고 같은 tick prepare 재실행을 요구한다.

## 3. 시간과 multi-horizon 계약

운영 logical time은 정확한 분 경계이며 KST 기준 `SERVING_TICK_MINUTES=5`의 배수여야
한다. 모델 학습 grid가 기본 20분이어도 서빙은 매 5분 실행할 수 있다.

각 정류소의 anchor 시각 `T0`에서 horizon `h`의 target 시각은
`T0 + (h-1)시간`이다.

- `rental_lag_1h`와 `return_lag_1h`는 anchor에서 한 번 계산해 모든 horizon에 고정한다.
- 날씨·인구·캘린더·`horizon`은 target 시각 기준으로 만든다.
- 이전 horizon의 예측값을 다음 입력에 넣지 않는다.
- 운영 authority는 모든 expected station에 horizon 1..12가 정확히 있어야 한다.

따라서 재귀 예측의 오차 누적은 없으며, 학습용 multi-horizon mart와 같은 feature
의미를 유지한다.

## 4. 실시간 feature와 fallback

### Lag

- 대여 lag는 트립별 `start_dt`와 `end_dt`를 사용해
  `[T-embargo-window, T-embargo)` 중 `end_dt <= T`인 이벤트만 센다.
- 반납 lag는 반납 이벤트를 시간 단위로 집계한다.
- 정상 관측값 0과 데이터 커버리지 밖 결측을 구분한다.
- 실제 데이터가 없을 때만 `station_hourly_profile.parquet`의
  `station_no × minute × dow × month` 평균으로 대체한다.
- 서빙 시각이 학습 anchor 사이에 있으면 profile 조회에 한해 같은 날의 직전 학습
  anchor로 내린다. 미래 anchor를 사용하지 않는다.

### 날씨

target 시각이 미래면 `weather_short_term_forecast`를 먼저 조회하고, 유효한 예보가
없거나 target이 현재·과거이면 `weather_ultra_short_live` 관측을 사용한다. 날씨
collector는 현재 `realtime_tick` DAG에 통합돼 있으며, 실패 시 이전 snapshot을
사용할 수 있다.

### 생활인구와 재고

- 인구는 `living_population_normalized`의 최근 값을 먼저 사용하고, 없으면
  `population_hourly_profile.parquet`의 `grid_id × hour × dow` 평균을 쓴다.
- 재고가 없으면 `stockout=False`로 대체한다. 이는 `rental_exposure=1.0`이 되어 실제
  품절 수요를 과대평가할 수 있으므로 결과 metadata에서 fallback 여부를 추적한다.

직접 호출 API는 `lag_fallback_used`, `lag_data_freshness`, `population_source`,
`stockout_source`를 반환한다. 다만 이 진단 필드는 정식 Gold inference authority의
13개 컬럼에는 포함하지 않는다.

## 5. 전체 정류소 계산과 완전성

전체 정류소 경로는 active station과 두 모델 support의 교집합을 serving plan에서
expected set으로 고정한다. horizon별로 정류소를 묶어 LightGBM을 배치 호출하고,
대여의 최근 실적 계산도 정류소별 반복 대신 벡터화한다.

`predict_single.py` 자체는 진단을 위해 station별 실패를 `failed` 목록에 격리할 수
있다. 그러나 `publication.py`는 다음 중 하나라도 만족하지 않으면 authority를 쓰지
않는다.

- `failed`가 비어 있지 않음
- `actual_count != expected_count`
- expected station 집합과 결과 station 집합이 다름
- 정류소별 horizon 1..12가 중복·누락됨
- target date/hour/minute가 logical time과 horizon 관계에 맞지 않음
- 예측값이 유한한 non-negative 값이 아님

정식 결과는 다음 exact 13-column schema로 canonicalize한다
(`INFERENCE_OUTPUT_COLUMN_NAMES`, `libs/core/src/core/inference_snapshot.py`).

| 컬럼 | 의미 |
|---|---|
| `station_id` | 서빙 정류소 ID |
| `date`, `hour`, `minute` | KST target 시각 |
| `horizon` | 1..12 |
| `rental_pred_mean`, `rental_pred_p10`, `rental_pred_p50`, `rental_pred_p90` | 대여 평균·분위 예측 |
| `return_pred_mean`, `return_pred_p10`, `return_pred_p50`, `return_pred_p90` | 반납 평균·분위 예측 |

`lag_fallback_used`/`lag_data_freshness`/`population_source`/`stockout_source` 같은
fallback 진단값은 직접 호출 결과에는 존재하지만 Gold 전달 authority에는 포함하지
않는다 — P10/P50/P90은 진단값이 아니라 예측 분위값이라 13개 컬럼에 포함된다.

## 6. Immutable publication

`run_and_publish_inference()`는 결과를 다음 순서로 공개한다.

1. 계산 중 실제로 읽은 non-model S3 bytes를 캡처한다.
2. 입력과 13-column Parquet을 content-addressed object로 고정한다.
3. logical time별 immutable revision catalog를 claim한다.
4. 성공 또는 `EMPTY` manifest를 마지막에 기록한다.
5. 기록한 manifest bytes를 다시 읽어 SHA-256과 구조를 검증한다.

같은 logical time과 같은 bytes의 재실행은 새 revision을 만들지 않는 exact replay다.
Rental history, authority weather, realtime처럼 worker thread에서 읽는 source도 caller의
capture context를 전달해 authority manifest와 연결된 Parquet bytes를 빠짐없이 같은
input 집합에 포함한다.
계산 결과가 달라지면 기존 latest manifest가 완전한지 검증한 뒤 다음 revision을 만든다.
manifest가 공개되기 전의 object는 authority가 아니므로 소비자는 catalog와 manifest만
따라가야 한다.

## 7. 알려진 한계

- 정류소 master는 current dimension이므로 과거 좌표·capacity를 시점별로 복원하지
  못한다. 다만 한 tick의 prepare→inference 사이에는 exact bytes가 고정된다.
- fallback profile은 평소 패턴이라 돌발 수요를 반영하지 못한다.
- 실시간 재고 결측 시 `stockout=False`는 대여 수요를 높게 만들 수 있다.
- 학습 support에 없거나 current active set에 없는 정류소는 serving surface에서
  제외된다. 신규 정류소는 재학습·승격 전까지 예측 대상이 아니다.
- 학습은 target 시각의 실제 관측 날씨를 사용하지만 미래 추론은 예보를 사용하므로
  근본적인 weather train-serving 차이는 남는다.

## 8. 검증 기준

```bash
cd ml
./inference/.venv/bin/python -m pytest inference/tests/ -q
```

변경 시 최소한 다음을 검증한다.

1. feature 순서·dtype·station category가 모델 snapshot과 같다.
2. 대여 censoring 결과가 feature engine의 Spark 결과와 같다.
3. lag fallback은 관측값 0을 결측으로 오인하지 않는다.
4. horizon 변화는 anchor lag를 바꾸지 않는다.
5. 예보·관측·인구·재고 fallback의 source 판정이 노출된다.
6. partial 결과는 immutable authority로 게시되지 않는다.
7. replay·revision·manifest-last 계약이 유지된다.
