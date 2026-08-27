# 실시간 Point-in-Time Feature 계약

> 현재 상태: **Feature Engine과 Inference 구현 기준**
>
> 핵심 목적: 학습 시점에는 알 수 없던 정보를 실시간 입력에 섞지 않는다.

실시간 추론은 anchor 시각의 최근 실적과 target 시각의 외부 정보를 조합한다.
학습과 서빙이 같은 시각 의미를 유지하지 않으면 모델 성능이 좋아 보여도 운영에서는
재현되지 않는다.

## 1. 시간축 구분

| 시간축 | 기본값 | 의미 |
|---|---:|---|
| serving tick | 5분 | 운영 추론 호출 간격 |
| model grid | 20분 | 과거 feature/target 생성 간격 |
| training anchor | 20분 | 학습에 남길 anchor 간격 |
| target window | 60분 | `[T, T+60분)` 라벨 구간 |
| multi-horizon | 1..12 | target은 `T0+(h-1)시간` |

serving tick과 model grid는 별개다. 기본 모델은 20분 grid로 학습하지만 5분마다
추론한다. multi-horizon에서도 lag는 항상 anchor `T0` 기준으로 고정하고 날씨·인구·
캘린더만 target 시각 기준으로 바꾼다.

## 2. 대여 이력의 우측 절단

대여 이력 한 행은 반납이 완료된 뒤에야 관측될 수 있다. anchor 시각 `T` 직전의
대여를 단순 집계하면 아직 반납되지 않은 트립이 빠진다. 반면 과거 학습 데이터에는
나중에 완료된 트립까지 모두 있으므로 train-serving skew가 생긴다.

완료율로 나누어 실제값을 추정하지 않는다. 완료율 변동이 큰 최신 구간에서는 작은
관측 오차가 크게 증폭되기 때문이다. 대신 학습에서도 `T` 당시 볼 수 있었던 트립만
사용한다.

```text
window = [T - embargo - width, T - embargo)

포함 조건
1. window_start <= start_dt < window_end
2. end_dt가 존재함
3. end_dt <= T
```

내장 기본값은 width 60분, embargo 40분이다. 즉 `T-100분`부터 `T-40분` 사이에
시작했고 `T`까지 반납된 트립을 센다. 5분은 window 폭이 아니라 서빙 갱신 간격이다.

## 3. 대여 batch와 serving 구현

두 경로는 알고리즘은 다르지만 위 포함 조건이 같아야 한다.

| 경로 | 구현 | 방식 |
|---|---|---|
| 공용 pandas 기준 | `libs/ml_core/rolling_window_features.py` | 차분 배열과 단일 시각 직접 집계 |
| Spark 학습 | `feature_engine/spark/rolling_window_features.py` | Spark Window 기반 차분 배열 |
| 실시간 추론 | `inference/predict_single.py` | 최근 트립에서 anchor 시각 직접 집계 |

배치의 `censored_rolling_counts()`는 트립마다 유효해지는 첫 tick에 `+1`, 유효
구간 다음 tick에 `-1`을 기록하고 station별 누적합을 계산한다. 결과는 sparse step
function이며 base grid에서 as-of 조회한다. station×tick 전체 dense grid를 만들지
않는다.

실시간의 `count_visible_in_window()`는 소량의 최근 트립에 같은 조건을 직접 적용한다.
Spark와 pandas 결과 및 Spark와 serving 결과는 parity 테스트로 고정한다.

## 4. `rental_lag_1h`와 `return_lag_1h`

현재 모델별 실적 feature는 하나씩이다.

- 대여 모델: `rental_lag_1h` — 위 point-in-time censored count
- 반납 모델: `return_lag_1h` — 정확히 한 시간 전의 반납 실적

반납은 `end_dt`가 곧 이벤트 시각이므로 대여와 같은 완료 지연 censoring이 필요하지
않다. 단, station grid에 정확히 한 시간 전 행이 없으면 다른 과거 행을 대신 쓰지
않고 null로 둔다.

과거의 `lag_24h`, `lag_168h`, 여러 rolling mean/std는 현행 모델 feature가 아니다.
feature 목록의 단일 기준은 `libs/ml_core/common_config.py`와
`libs/ml_core/model_contract.py`다.

## 5. 실시간 source snapshot

Inference는 logical Silver key를 실제 object 위치로 직접 읽지 않는다.
`read_exact_source_snapshot()`이 Collector의 correction manifest를 따라
content-addressed Parquet bytes를 검증해 읽는다.

| feature | source | 조회 기준 |
|---|---|---|
| 대여·반납 lag | `bike_rental_history` | anchor 시각 이전 최근 3시간 |
| 재고·품절 | `bike_station_realtime` | anchor 시각 이전 최대 1시간 |
| 날씨 관측 | `weather_ultra_short_live` | target 시각 이전 최대 3시간 |
| 미래 날씨 | `weather_short_term_forecast` | 최근 발표본에서 target과 가장 가까운 예보 |
| 생활인구 | `living_population_normalized` | target 시각 이전 최대 1시간 |

운영 publication은 계산 중 실제로 읽은 non-model S3 bytes를 캡처해 immutable input
reference로 남긴다. 따라서 결과 manifest에서 사용한 source bytes를 역추적할 수 있다.

## 6. 날씨 계약

학습은 과거 target 시각의 실제 관측 날씨를 사용한다. 실시간에서는 target이 미래일
수 있으므로 처리 순서가 다르다.

1. `target_ts > anchor_ts`이면 단기예보를 먼저 찾는다.
2. 예보 시각이 target에서 35분 이내인 유효 격자 행을 평균한다.
3. 예보가 없거나 유효하지 않으면 최근 관측으로 fallback한다.
4. 관측도 3시간 안에 없으면 실패한다.

`realtime_tick*` DAG는 필요한 시각에 날씨 collector를 같은 DAG의 선행 task로
실행한다. 날씨 task 실패는 `ALL_DONE` gate를 통해 이전 snapshot 사용을 허용하지만,
유효한 fallback조차 없으면 추론이 실패한다.

## 7. 생활인구 계약

학습은 Archive `living_population_grid`를 사용한다. 실시간 추론은 normalizer가
5분마다 보정한 `living_population_normalized`를 먼저 조회한다. 없으면
`population_hourly_profile.parquet`의 `grid_id × hour × dow` 평균으로 대체한다.

profile에 month가 없는 것은 의도된 계약이다. 생활인구의 월별 변화보다 시간대별
변화가 크고, month까지 나누면 fallback 그룹의 표본만 줄어든다.

결과의 `population_source`로 직접값/실시간값 사용과 profile fallback 여부를
구분한다. 정식 13-column Gold inference 결과에는 이 진단 필드가 포함되지 않지만
직접 예측 API와 내부 결과에는 유지된다.

## 8. 재고와 exposure

대여소 재고가 0이면 관측 대여량이 실제 잠재 수요보다 작다. 이는 트립 로그 우측
절단과 별개의 문제다.

- 학습: `rental_exposure`를 사용한 Poisson offset
- 서빙: anchor 시각의 최근 재고에서 `stockout` 계산
- 재고 결측: `stockout=False`, 즉 exposure 1.0으로 fallback
- 반납: 거치대 상태와 무관하게 완료되므로 exposure 없음

재고 fallback은 대여 수요를 과대평가할 수 있다. 직접 예측 결과의
`stockout_source`를 함께 확인해야 한다.

## 9. Lag fallback과 0의 구분

최근 트립 window에 대여가 0건인 것은 정상 관측값 0이다. 이를 데이터 결측으로 보고
평균 profile을 채우면 안 된다.

- 요청 window가 로드된 트립 coverage와 겹치면 0도 실제값으로 사용한다.
- coverage 밖이면 `station_hourly_profile.parquet`로 fallback한다.
- 반납은 정확한 history tick이 없을 때 profile fallback한다.
- profile 조회는 serving 시각을 미래 anchor가 아닌 같은 날 직전 학습 anchor로 내린다.

`lag_fallback_used`는 대체된 lag 이름을, `lag_data_freshness`는 실제 lag 사용 비율을
나타낸다. multi-horizon 전체가 같은 anchor lag와 fallback 판정을 공유한다.

## 10. 변경 시 지켜야 할 불변조건

1. 대여 window 경계는 `[start, end)`다.
2. `end_dt <= anchor_ts`인 완료 트립만 센다.
3. model grid와 serving tick을 혼동하지 않는다.
4. multi-horizon lag는 target이 아니라 anchor 기준이다.
5. 관측값 0을 결측으로 처리하지 않는다.
6. 미래 날씨는 예보를 우선하고 미래 관측을 사용하지 않는다.
7. fallback source는 진단 metadata에서 확인 가능해야 한다.
8. Collector manifest 오류를 오래된 logical key 직접 읽기로 우회하지 않는다.

## 11. 검증

```bash
cd ml

./training/.venv/bin/python -m pytest \
  ../libs/ml_core/tests/dev_rolling_window_features.py -q

./inference/.venv/bin/python -m pytest \
  inference/tests/dev_predict_single_rental_censoring.py \
  inference/tests/dev_predict_single_multi_horizon.py \
  inference/tests/dev_predict_single_weather_forecast.py \
  inference/tests/dev_predict_single_population_normalized.py \
  inference/tests/dev_predict_single_stockout_source.py -q

SPARK_LOCAL_IP=127.0.0.1 ./feature_engine/.venv/bin/python -m pytest \
  inference/tests/dev_rental_censoring_cross_parity.py -q
```

Spark cross-parity는 PySpark가 설치된 Feature Engine 환경에서 실행해야 한다.
Inference 환경으로 실행하면 PySpark가 없어 skip된다. 로컬 driver socket 환경에 따라
`SPARK_LOCAL_IP=127.0.0.1`도 필요하다.
