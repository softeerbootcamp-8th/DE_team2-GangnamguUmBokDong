# 골드 스키마를 raw SQL init 스크립트로 만든다

## 배경
apps/api가 mock 데이터 대신 읽을 실제 테이블이 필요하다. 스키마는 아직 확정이 아니고 프론트/백엔드 개발을 진행하면서 계속 바뀔 걸로 예상된다. 리포에는 아직 어떤 마이그레이션 도구도 없다.

## 결정
Alembic 같은 마이그레이션 도구 없이, `ops/postgres/init/002_gold_schema.sh`에 `CREATE TABLE IF NOT EXISTS`로 테이블을 만든다. 001과 같은 방식이다.

## 결과
스키마가 바뀌면 로컬 볼륨을 지우고 다시 띄우면 된다(`docker compose down -v`). 실 데이터가 쌓이기 시작해 볼륨을 지울 수 없는 시점이 오면, 그때 지금 스키마를 베이스라인 마이그레이션으로 삼아 Alembic으로 전환한다(`alembic stamp head`로 재작성 없이 전환 가능).

---

# urgency_score·action_type 등 파생값은 테이블로 두지 않는다

## 배경
알림 목록(`/alerts`)의 urgency_score, action_type, 예측 재고(predicted_bikes)는 전부 재고·예측 원본값으로부터 계산되는 값이다. 이걸 테이블로 저장하면 재고나 예측이 갱신될 때마다 같이 갱신해야 해서 정합성이 깨질 여지가 생긴다.

## 결정
`stations`(대여소 마스터), `station_stock`(재고 관측 이력), `forecast_points`(대여·반납 예측 원본치)만 테이블로 두고, urgency_score·action_type·predicted_bikes·shared_rate는 apps/api가 요청 시점에 계산해서 응답에 채운다.

## 결과
테이블 간 동기화 문제가 없어진다. 대신 계산 로직(우선순위 점수 공식 등)은 DB가 아니라 apps/api 코드에 있어야 하고, 트래픽이 커지면 그때 캐싱이나 materialized view를 고려한다.

---

# station_stock은 "현재 한 줄"이 아니라 이력 테이블로 둔다

## 배경
우선순위 점수의 위험 시점 감지는 즉시위험·추세감지·예측감지 세 가지 중 가장 이른 걸 쓴다. 예측감지는 예측 배치가 있는 1시간 뒤부터만 유효해서, 그 전 구간(가장 빠르게 위험해질 수 있는 구간)은 최근 재고 추세로만 잡을 수 있다. 그런데 station_stock을 대여소당 최신 한 줄만 upsert하는 구조로 두면 추세를 계산할 이력 자체가 없다.

## 결정
station_stock을 `(sta_id, observed_at)`을 기본키로 하는 이력 테이블로 만든다. 수집 주기마다 upsert 대신 새 행을 추가(insert)한다.

## 결과
1시간 이내의 급격한 변화도 감지할 수 있다. 대신 테이블이 계속 커지므로, 트래픽이나 저장 용량이 문제가 되면 그때 오래된 행을 정리하는 배치를 추가한다.

---

# weather_forecast·forecast_points·cultural_events는 유예기간을 두고 loader가 직접 정리한다

## 배경
`weather_forecast`(`weather_forecast_ultra` 포함), `forecast_points`, `cultural_events`(`cultural_events_performance` 포함)는 전부 시각이 지나거나 만료된 행이 계속 쌓이기만 하고 지워지지 않는다([#116](https://github.com/softeerbootcamp-8th/DE_team2-GangnamguUmBokDong/issues/116), [#117](https://github.com/softeerbootcamp-8th/DE_team2-GangnamguUmBokDong/issues/117)). 지나간 시각이 되는 즉시 삭제하면 "그때 예보·예측이 실제와 얼마나 맞았는지" 사후 비교·분석할 데이터가 안 남는다.

## 결정
칼같이 즉시 삭제하지 않고 테이블별 유예기간(`loader/retention_config.py`의 `RETENTION_GRACE`: weather_forecast·forecast_points 2시간, cultural_events 3일)이 지난 뒤에만 지운다. 새 Airflow 태스크를 따로 만들지 않고, 5개 테이블 스펙이 전부 통과하는 기존 적재 진입점(`loader/main.py:run()`)이 upsert와 같은 트랜잭션으로 만료 행을 지운다. 정리 대상 테이블은 `tables.yaml`에 `expire_col`을 선언한 테이블로만 한정한다(`stations`처럼 마스터 데이터거나 `station_stock`처럼 최신 관측 이력 자체가 목적인 테이블은 대상이 아니다).

## 결과
`realtime_5min`·`weather_10min`·`weather_3h`·`daily_population_and_events` 4개 DAG 모두 이 진입점을 거치므로 별도 배선 없이 자동으로 커버된다. `cultural_events.end_date`는 DATE 컬럼이라 TIMESTAMPTZ cutoff와 비교하면 캐스팅이 모호해지므로, 이 테이블만 cutoff를 date로 변환해 비교한다(`DATE_TYPED_EXPIRE_TABLES`).
