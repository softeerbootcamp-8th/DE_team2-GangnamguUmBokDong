# inference — 설계 문서

실행 방법은 [README.md](../../../ml/inference/README.md), 결정의 배경은 [history.md](../history.md)/
[REALTIME_FEATURES.md](../REALTIME_FEATURES.md)를 참고. 이 문서는 "지금 코드가
왜 이렇게 짜여 있는지"에 집중한다.

## 1. 왜 두 가지 예측 경로인가

배치 조회(`predict_common.py`)는 이미 계산된 feature 테이블(`feature_engine`의
산출물)에서 골라 조회하는 것이라 2025년 범위 밖은 다룰 수 없다. 실서비스 연동
대상은 **단일 시점 예측**(`predict_single.py`)이다 — 정류소ID + 날짜/시각 + 날씨만
받으면 lag/rolling을 내부에서 자동으로 채워 임의 시점을 예측한다.

## 2. `predict_single.py`가 lag를 자동 계산하는 이유

**(2026-08 갱신) lag/rolling 피처가 대폭 축소됐다** — 지금 모델 feature는
대여/반납 각각 `rental_lag_1h`/`return_lag_1h` 딱 1개씩뿐이다(예전엔
`lag_24h/168h`, `roll_mean/std_3h·24h` 등 14개짜리 스키마였다 — 아래 내용이
그 시절 기준이면 지금은 안 맞음). 그래도 원리는 그대로다: 모델이 가장
중요하게 쓰는 정보는 그 정류소의 직전 실적("이미 일어난 일"이라 사용자가
시나리오처럼 지정할 값이 아니라 조회해야 하는 값)이므로, 모듈 내부에서
히스토리를 보고 자동으로 채운다. 히스토리 소스는 두 개로 나뉜다:

- **`_get_history_by_station()`** — 병합 테이블(시간 단위 집계). 반납은 반납
  이벤트 자체가 로그 시점이라 지연 관측 문제가 없어서 `return_lag_1h`
  전체가 이 경로만 쓴다.
- **`_get_rental_events_by_station()`**(`ml_core.trip_events.load_rental_trip_events()`) —
  트립 단위(start_dt/end_dt) 원본. `rental_lag_1h` 계산에 쓴다 — 대여는
  반납이 완료돼야 로그에 잡히는 지연 관측 문제가 있어서, 시간 단위 집계만
  으로는 그 시점에 실제로 관측 가능했던 값을 재현할 수 없기 때문이다
  (`ml_core.rolling_window_features.count_visible_in_window()`로 계산).
  실제 서비스로 갈 때는 이 두 함수만 각각 실시간 소스(집계 스토어 / 트립
  이벤트 버퍼)로 교체하면 나머지 로직은 그대로 재사용된다.

## 3. 2단계 fallback — 실시간 데이터 결측/지연 대응

요구사항은 "근접 미래를 실시간 실적으로 계속 예측하되, 피드가 끊기거나
지연돼도 어느 정도 정확도를 유지해야 한다"는 것이었다. `_lag_rolling_features()`가
`rental_lag_1h`/`return_lag_1h`를 다음 순서로 채운다:

1. **실시간 히스토리에서 조회** — 있으면 그대로 사용
2. **없으면 `station_hourly_profile.parquet`(`inference.build_station_profile`)로 대체** —
   그 정류소가 이 달·이 요일·이 tick(minute)에 보통 어느 정도였는지(월을
   그룹 키에 반드시 포함 — 계절에 따라 대여량이 최대 2.44배 차이나서, 월
   없이 묶으면 겨울 결측을 여름 수준으로 채우는 오류가 생김). **키가
   `hour`가 아니라 `minute`인 이유(2026-08)**: `rental_count`/`return_count`가
   60분짜리 forward-rolling 합계를 5분마다 다시 계산한 값이라, 같은 시간
   (hour) 안의 인접 tick끼리 창이 최대 11/12 겹친다 — hour로 묶어 표본을 늘려도
   그 "추가" 표본이 사실상 중복이라 minute 단위로 묶는 것과 실질적으로
   차이가 없다(오히려 세분화 손해가 없다는 뜻).

"없으면"의 판정 기준: **`rental_lag_1h`**는 "그 anchor의 윈도우에 트립이
0건"(정상 관측값 0)과 "그 윈도우 자체가 로드된 트립 데이터 커버리지
밖"(진짜 결측)을 구분한다 — 전자는 fallback이 아니다. **`return_lag_1h`**는
해당 시각이 히스토리 그리드에 없으면 결측 → fallback.

두 lag 각각을 독립적으로 대체하는 방식이라(재귀적으로 예측값을 다음 입력에
다시 먹이지 않음), 여러 horizon 앞을 예측해도 오차가 쌓이지 않는다 — §7 참고.
반환값의 `lag_fallback_used`/`lag_data_freshness`로 이번 예측이 실시간
데이터를 얼마나 썼는지 확인할 수 있다.

**날씨는 lag와 다르게 다룬다(2026-08 신규)**: `_resolve_live_weather()`가
target_ts(horizon에 따라 미래일 수 있음)와 anchor_ts(T0, "지금")를 비교해서
미래면 예보(`weather_short_term_forecast`)를 먼저 시도하고, 그렇지 않거나
예보를 못 찾으면 관측(`weather_ultra_short_live`)으로 fallback한다 — 학습은
항상 target_ts의 실제 관측 날씨(ground truth, 이미 지난 과거라 실측이
있음)로 배우지만, 추론은 target_ts가 미래일 수 있어 이 분기가 필요하다.
collector의 예보 자동 수집 스케줄이 아직 없어(수동 트리거만 가능) 실제로는
관측 fallback을 타는 경우가 아직 많다.

인구(`population`)도 같은 원리로 `population_hourly_profile.parquet`
(`inference.build_population_profile`)로 대체되지만, 그룹 키에 **월을 넣지
않는다** — 생활인구는 월별로는 거의 안 변하고(1.05배) 시간대별로만 크게
변해서(1.42배, 출퇴근 패턴) station 프로필과 계절 반응이 다르기 때문.

**날씨로도 조건화하지 않는 이유(검토 후 보류)**: station 프로필은 이미
`station × hour × dow × month`로 쪼개져 있어 그룹당 표본이 4~5개뿐이다.
여기에 `rain_flag`(강수 여부)까지 추가해봤더니 표본 1개 이하인 그룹이
17.6%로 뛰었다(반대로 month를 빼고 rain_flag만 넣으면 표본은 넉넉해지지만
계절성을 다시 잃음) — station 단위로 세분화한 상태에서 축을 하나 더 늘리면
1년치 데이터로는 표본이 순식간에 바닥난다. "station 기준값 × 도시 전체
날씨 보정 배수"처럼 계층적으로 접근하는 대안은 있지만, fallback 하나를 위해
별도 보정 로직을 얹는 복잡도 대비 이득이 낮다고 판단해 지금은 month까지만
유지한다.

## 4. `ml_core/`에서 가져오는 것과 이 폴더에 남은 것

- `ml_core.model_contract.RENTAL_FEATURE_COLUMNS`/`RETURN_FEATURE_COLUMNS`/
  `load_station_dtype()` — training이 저장한 station_no(station_id 아님 —
  모델 feature는 정수 station_no, station_id는 식별용) 카테고리를 그대로
  로드해야 모델이 정류소를 올바르게 해석한다(모델 계약,
  [training/DESIGN.md](../training/DESIGN.md) 4절 참고).
- `ml_core.scoring.predict()` — 저장된 booster 채점 로직(exposure 복원,
  conformal 보정 적용) — `monitor_performance.py`와 동일한 로직을 씀.
- `ml_core.rolling_window_features.count_visible_in_window()`,
  `ml_core.trip_events.load_rental_trip_events()` — point-in-time censoring을
  배치(`feature_engine`)와 서빙(이 폴더)이 같은 규칙으로 계산해야 한다.

이 폴더에 남은 건 "서빙 시나리오 조립"(fallback 판정, 프로필 조회, CLI/함수
인터페이스)뿐이다.

## 5. 검증 — 배치와 서빙의 일치

히스토리에 있는 시점을 넣으면 실제 과거 lag 값을 그대로 써서 예측하고, 배치
CLI의 같은 시점 결과와 소수점까지 정확히 일치한다(`population`을 제공한
경우 기준). `return_lag_1h`는 배치·서빙 둘 다 같은 시간 단위 집계를
조회하므로 일치가 자명하다. `rental_lag_1h`는 배치(`feature_engine/spark/build_features.py`의
`censored_rolling_counts`)와 서빙 시뮬레이션(이 모듈, `count_visible_in_window`
반복 호출)이 서로 다른 코드 경로지만 같은 point-in-time censoring 규칙을
쓰므로 일치해야 한다 — `tests/dev_rental_censoring_cross_parity.py`가 합성
데이터로 이를 확인한다.

## 6. 실시간 트립 카운트 스토어 — Kafka+Spark Streaming은 과설계

§2의 `_get_rental_events_by_station()`을 실제 서비스에서 실시간 소스로
교체할 때를 대비해 검토한 내용. 초안에서는 Kafka + Spark Structured
Streaming을 제안했었지만, 실제 처리량을 계산해보니 명백한 과설계였다 —
2025년 실측 기준 서울 전체(정류소 2,582개 합산) 트립 이벤트는 평균 초당
1.2건, 가장 붐비는 시간대(평일 18시)도 초당 3.3건에 불과하다. Kafka는 초당
수만~수백만 건, 여러 독립 프로듀서/컨슈머 분리나 이벤트 재생(replay)이 하드
요구사항인 상황을 위한 도구라, 초당 한두 건짜리 이벤트에 브로커 클러스터+
상시 구동 Spark 클러스터를 얹으면 운영 부담(장애 지점·모니터링 대상 증가)이
얻는 이득보다 훨씬 크다.

**더 적합한 대안**: 트립 이벤트를 수집하는 쪽이 Redis에 직접 쓰면 끝난다 —
`station_id`별로 `INCR bike:{station_id}:{tick_bucket}` 하나면 tick당
카운트가 되고 `EXPIRE`로 오래된 키를 정리하면 `rental_lag_1h`/`return_lag_1h`
조회용 rolling window가 그대로 구현된다. 폴링 주기가 분 단위(예: 5분마다 API 조회)라면 cron/스케줄러가
주기적으로 Redis나 Postgres 테이블을 갱신하는 배치 스크립트로 충분하다 —
"근접 미래, 지연에도 견고해야 함"이라는 §3의 fallback 설계 전제와도 자연스럽게
맞는다. 이벤트 소스가 여러 개라 디커플링이 실제로 필요해지면 그때 메시지
큐를 고려할 수 있는데, 이 처리량대에서는 Kafka보다 Redis Streams나
RabbitMQ로 충분하다 — Kafka는 초당 수천 건 이상·여러 소비자 그룹·장기
보관/재처리가 필요해지는 다음 단계에서나 정당화된다.

## 7. N시간 뒤까지 예측 — horizon-as-feature (2026-08 갱신, 예전엔 재귀 방식)

**이 절은 원래 "재귀 방식을 알고도 임시로 채택했다"고 적혀 있었다 — 그 임시
방식은 이후 실제로 horizon-as-feature로 교체됐다(history.md 18/20번 항목의
후속).** `predict_demand_multi_hour()`는 이제 직전 스텝의 예측값을 다음 스텝
입력으로 재사용하지 않는다 — lag(`rental_lag_1h`/`return_lag_1h`)는
anchor_ts(T0, "지금") 기준으로 딱 한 번만 계산하고, "몇 시간 뒤인지"(horizon,
1~`HORIZON_COUNT`)를 평범한 입력 feature로 모델에 직접 알려준다. 학습
테이블 자체가 이 구조로 만들어져 있다(`feature_engine/spark/build_multi_horizon_features.py`,
같은 anchor에 horizon만 다른 행을 union) — 그래서 서빙도 재귀 없이 그
학습 구조를 그대로 재현하기만 하면 된다. 모든 horizon(h=1이든 h=12든)이
같은 경로(lag는 T0 고정, 날씨/캘린더/타겟만 target_ts 기준 재계산)를 타므로
정확도가 horizon 전체에 걸쳐 균일하게 보존되고, 오차가 누적될 여지 자체가
없다 — 재귀 로직(`_recursive_lag_rolling_features()` 등)은 삭제됐다.
