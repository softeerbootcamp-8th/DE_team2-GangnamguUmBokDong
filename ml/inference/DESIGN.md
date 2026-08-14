# inference — 설계 문서

실행 방법은 [README.md](README.md), 결정의 배경은 [history.md](../history.md)/
[REALTIME_FEATURES.md](../REALTIME_FEATURES.md)를 참고. 이 문서는 "지금 코드가
왜 이렇게 짜여 있는지"에 집중한다.

## 1. 왜 두 가지 예측 경로인가

배치 조회(`predict_common.py`)는 이미 계산된 feature 테이블(`feature_engineering`의
산출물)에서 골라 조회하는 것이라 2025년 범위 밖은 다룰 수 없다. 실서비스 연동
대상은 **단일 시점 예측**(`predict_single.py`)이다 — 정류소ID + 날짜/시각 + 날씨만
받으면 lag/rolling을 내부에서 자동으로 채워 임의 시점을 예측한다.

## 2. `predict_single.py`가 lag/rolling을 자동 계산하는 이유

모델이 가장 중요하게 쓰는 정보는 그 정류소의 직전 실적(lag/rolling, feature
importance 1위)이다 — "이미 일어난 일"이라 사용자가 시나리오처럼 지정할 값이
아니라 조회해야 하는 값이므로, 모듈 내부에서 히스토리를 보고 자동으로 채운다.
히스토리 소스는 두 개로 나뉜다:

- **`_get_history_by_station()`** — `station_hour_merged_2025.parquet`(시간 단위
  집계). 반납(return) 전체와 대여의 `lag_24h/168h`처럼 지연 관측 문제가 없거나
  예측 시점엔 이미 해소된 피처에 쓴다.
- **`_get_rental_events_by_station()`**(`ml_common.trip_events.load_rental_trip_events()`) —
  트립 단위(start_dt/end_dt) 원본. 대여의 "직전 1시간" 4개(`rental_lag_1h`,
  `roll_mean/std_3h·24h`)에 쓴다 — 대여는 반납이 완료돼야 로그에 잡히는 지연
  관측 문제가 있어서, 시간 단위 집계만으로는 그 시점에 실제로 관측 가능했던
  값을 재현할 수 없기 때문이다(`ml_common.rolling_window_features.count_visible_in_window()`로
  계산). 실제 서비스로 갈 때는 이 두 함수만 각각 실시간 소스(집계 스토어 /
  트립 이벤트 버퍼)로 교체하면 나머지 로직은 그대로 재사용된다.

## 3. 2단계 fallback — 실시간 데이터 결측/지연 대응

요구사항은 "근접 미래를 실시간 실적으로 계속 예측하되, 피드가 끊기거나
지연돼도 어느 정도 정확도를 유지해야 한다"는 것이었다. `_lag_rolling_features()`가
각 lag/rolling 값을 다음 순서로 채운다:

1. **실시간 히스토리에서 조회** — 있으면 그대로 사용
2. **없으면 `station_hourly_profile.parquet`(`inference.build_station_profile`)로 대체** —
   그 정류소가 이 달·이 요일·이 시간에 보통 어느 정도였는지(월을 그룹 키에
   반드시 포함 — 계절에 따라 대여량이 최대 2.44배 차이나서, 월 없이 묶으면
   겨울 결측을 여름 수준으로 채우는 오류가 생김)

"없으면"의 판정 기준이 필드마다 다르다:

- **시간 단위 집계 기반(반납 7개 + 대여 lag_24h/168h)**: 해당 시각이 히스토리
  그리드(2025년 전체)에 없으면 결측 → fallback.
- **트립 단위 기반(대여 나머지 4개)**: "그 anchor의 윈도우에 트립이 0건"(정상
  관측값 0)과 "그 윈도우 자체가 로드된 트립 데이터 커버리지 밖"(진짜 결측)을
  구분한다 — 전자는 fallback이 아니다.

재귀적으로 예측값을 다음 입력에 다시 먹이는 방식(오차 누적)이 아니라, 14개
feature 각각을 독립적으로 대체하는 방식이라 여러 시간 앞을 예측해도 오차가
쌓이지 않는다. 반환값의 `lag_fallback_used`/`lag_data_freshness`로 이번 예측이
실시간 데이터를 얼마나 썼는지 확인할 수 있다.

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

## 4. `ml_common/`에서 가져오는 것과 이 폴더에 남은 것

- `ml_common.model_contract.FEATURE_COLUMNS`/`load_station_dtype()` — training이
  저장한 station_id 카테고리를 그대로 로드해야 모델이 station_id를 올바르게
  해석한다(모델 계약, [training/DESIGN.md](../training/DESIGN.md) 4절 참고).
- `ml_common.scoring.predict()` — 저장된 booster 채점 로직(exposure 복원,
  conformal 보정 적용) — `monitor_performance.py`와 동일한 로직을 씀.
- `ml_common.rolling_window_features.count_visible_in_window()`,
  `ml_common.trip_events.load_rental_trip_events()` — point-in-time censoring을
  배치(`feature_engineering`)와 서빙(이 폴더)이 같은 규칙으로 계산해야 한다.

이 폴더에 남은 건 "서빙 시나리오 조립"(fallback 판정, 프로필 조회, CLI/함수
인터페이스)뿐이다.

## 5. 검증 — 배치와 서빙의 일치

히스토리에 있는(2025년 내) 시점을 넣으면 실제 과거 lag/rolling 값을 그대로 써서
예측하고, 배치 CLI의 같은 시점 결과와 소수점까지 정확히 일치한다(`population`을
제공한 경우 기준). 반납 7개 + 대여 `lag_24h/168h`는 둘 다 같은 시간 단위 집계를
조회하므로 일치가 자명하다. 대여 나머지 4개는 배치(`feature_engineering.legacy.features` —
실제 서비스는 `feature_engineering/spark/build_features.py`를 쓰지만 parity 테스트로
검증된 동일 로직이라 pandas 버전을 비교 기준으로 그대로 씀,
`censored_rolling_counts`의 차분배열)와 서빙 시뮬레이션(이 모듈,
`count_visible_in_window` 반복 호출)이 서로 다른 코드 경로지만 같은
point-in-time censoring 규칙을 쓰므로 일치해야 한다 —
`tests/dev_rental_censoring_cross_parity.py`가 합성 데이터로 이를 확인한다.

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
`station_id`별로 `INCR bike:{station_id}:{hour_bucket}` 하나면 시간당 카운트가
되고 `EXPIRE`로 오래된 키를 정리하면 lag_1h/24h/168h 조회용 rolling window가
그대로 구현된다. 폴링 주기가 분 단위(예: 5분마다 API 조회)라면 cron/스케줄러가
주기적으로 Redis나 Postgres 테이블을 갱신하는 배치 스크립트로 충분하다 —
"근접 미래, 지연에도 견고해야 함"이라는 §3의 fallback 설계 전제와도 자연스럽게
맞는다. 이벤트 소스가 여러 개라 디커플링이 실제로 필요해지면 그때 메시지
큐를 고려할 수 있는데, 이 처리량대에서는 Kafka보다 Redis Streams나
RabbitMQ로 충분하다 — Kafka는 초당 수천 건 이상·여러 소비자 그룹·장기
보관/재처리가 필요해지는 다음 단계에서나 정당화된다.

## 7. N시간 뒤까지 예측 — 재귀 방식을 알고도 채택 (임시)

`predict_demand_multi_hour()`는 N번째 시간대부터 직전 스텝의 예측값을 다음
스텝의 lag/rolling 입력으로 재귀적으로 사용한다. 이 방식은 원래
`training/experiments/multi_horizon/`(history.md 18번 항목)에서 "오차가
누적된다"는 이유로 기각했던 방법과 같다 — 이번에는 그 한계를 알고도 구현
속도를 우선해 다시 채택했다(history.md 20번 항목). h=1(바로 다음 시간)만은
기존 단일 시점 예측과 완전히 같은 경로를 타서 정확도가 그대로 보존되고,
h>=2부터만 `_recursive_lag_rolling_features()`가 근사치를 쓴다. 정확도가
문제되면 이미 검증된 horizon-as-feature 모델(`training/experiments/multi_horizon/`)로
교체하는 게 다음 단계다 — 그땐 재귀 로직 자체가 필요 없어진다.
