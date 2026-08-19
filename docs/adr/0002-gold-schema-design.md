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

# urgency_score·action_type은 결국 station_urgency 테이블로 뺀다 (위 결정 일부 번복)

## 배경
"urgency_score·action_type 등 파생값은 테이블로 두지 않는다" 결정 이후, 재배치 라우트 계산(대여소 간 이동 계획을 만드는 배치)이 urgency_score를 입력으로 쓰게 됐다(#109). 매 `/alerts` 요청마다 다시 계산되는 값을 배치 입력으로 쓸 수는 없다 — 배치는 정해진 시점에 한 번 도는데, 계산 결과가 그 시점의 데이터로 고정돼 있어야 한다.

또한 팀 판단 기준(`de-project/docs/rds_vs_s3.md`: 데이터를 누가 어떻게 소비하는지가 저장소 선택 기준)에 따르면, urgency_score 계산은 이제 실시간 API 요청이 아니라 5분 배치가 전체 대여소를 한 번에 훑는 작업이라 애초에 "요청 시점 계산"이라는 원래 결정의 전제 자체가 바뀌었다.

## 결정
`station_urgency`((batch_run_at, sta_id) PK, urgency_score, minutes_until_critical, action_type) 테이블을 추가한다(003_station_urgency.sh). 계산 로직 자체는 `apps/api`에서 `rebalance/`(신규 배치 모듈)로 이식하고, `rebalance`가 S3(재고 이력·예측 결과)만 읽어 계산한 뒤 loader가 batch snapshot 이력으로 upsert한다. `apps/api`의 `/alerts`는 `MAX(batch_run_at)`에 해당하는 단일 snapshot만 SELECT한다.

`predicted_bikes`/`shared_rate`처럼 순수하게 요청 시점에만 의미 있는 파생값(요청마다 다른 조건으로 조회될 수 있음)은 원래 결정 그대로 테이블로 두지 않는다 — 이번 변경은 urgency_score/action_type에 한정된다.

## 결과
`/alerts`가 매 요청마다 전체 대여소를 스캔하며 재계산하던 부하가 없어지고, 배치가 이미 계산해둔 값을 조회만 한다. 한 응답에 서로 다른 batch의 row가 섞이지 않으며 향후 route batch도 같은 `batch_run_at` snapshot을 고정해 사용할 수 있다. `compute_urgency`는 anchor tick과 정확히 일치하는 재고만 현재값으로 인정한다. inference는 학습된 station 집합의 partial 결과를 upstream에서 실패시키며, 정상 산출물에 없는 신설·미지원 station은 urgency 대상에서 제외하고 건수를 로그로 남긴다.

---

# 재배치 라우트(rebalance_routes/rebalance_route_stops) 테이블 추가, station_urgency에 bike_qty 보강

## 배경
station_urgency로 "어디가 급한지"는 알아도 "트럭이 몇 대를 옮길지"는 없었다. urgency_score 계산 내부에는 이미 초과/부족 대수(`_max_overshoot`/`_max_deficit`/`_max_unmet_demand`)가 있었지만 결과에 노출되지 않아, 이 값을 `bike_qty`라는 이름으로 뽑아 `station_urgency`에 컬럼을 추가했다. 이 시점엔 이미 003_station_urgency.sh가 develop에 머지돼 실 데이터가 쌓이는 환경이 있으므로, `CREATE TABLE IF NOT EXISTS` 본문을 직접 고치는 대신 `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`로 기존 볼륨에도 안전하게 컬럼을 추가한다.

라우트 자체는 지금까지의 "파생값은 테이블로 안 둔다" 원칙과 다르다 — urgency_score와 달리 라우트는 계산 결과가 아니라 **운영자가 조작하는 상태를 가진 리소스**(proposed → dispatched → completed)라서, 원본이 되어야 한다.

## 결정
`rebalance_routes`(route_id PK, region, status, proposed_at/dispatched_at/completed_at)와 `rebalance_route_stops`(route_id+visit_order PK, sta_id, action, bike_cnt) 두 테이블을 추가한다(004_rebalance_routes.sh). `rebalance/routes.py`가 매 사이클 `proposed` 라우트를 새로 만들어 loader가 insert하고, 상태 전이(dispatched/completed)는 운영자가 대시보드에서 조작(#110)하므로 두 테이블 다 `update_cols=[]`(DO NOTHING)로 적재한다 — 배치 재실행이 운영자가 바꿔놓은 status를 덮어쓰면 안 되기 때문이다.

## 결과
`rebalance_routes`/`rebalance_route_stops`는 station_stock/forecast_points/station_urgency와 달리 배치가 "덮어쓰는" 게 아니라 "계속 쌓이면서 상태만 바뀌는" 테이블이라, 지금까지의 두 패턴(이력 누적, 최신 1건 upsert)과는 다른 세 번째 패턴이다. 오래된 완료/취소 라우트를 언제까지 남겨둘지(보존 기간)는 아직 정하지 않았다 — 트래픽이 쌓이면 그때 정리 정책을 추가한다.

---

# station_urgency는 이력이 아니라 sta_id당 최신 1건만 upsert한다 (위 결정 일부 번복)

## 배경
위 결정에서 `station_urgency`를 `(batch_run_at, sta_id)` 복합 PK 이력 테이블로 두고 `/alerts`가 `MAX(batch_run_at)` snapshot만 조회하도록 했다(배치 섞임 버그 대응). 그런데 이 테이블에 이력을 남겨야 할 이유가 구조적으로 없다([#124](https://github.com/softeerbootcamp-8th/DE_team2-GangnamguUmBokDong/issues/124)):
- `rebalance/urgency.py:compute_all()`은 매 배치마다 S3(재고 이력·예측)만 다시 읽어 처음부터 계산한다 — RDS `station_urgency`를 계산 입력으로 쓰는 곳이 없다.
- `rebalance/main.py`가 매 배치 결과를 이미 S3(`urgency/dt=.../hh=.../urgency_HHMM.parquet`)에 영구 저장한다 — RDS가 이력을 안 남겨도 전체 이력은 S3에 그대로 남는다.
- `/alerts` 외에 과거 배치의 urgency 값을 조회하는 소비자가 없다.

그 결과 `station_urgency`는 삭제 로직 없이 5분마다 대여소 수만큼 영구히 쌓이기만 하는 테이블이 됐다.

## 결정
`station_urgency`의 PK를 `sta_id` 단일키로 되돌린다(진짜 upsert, `bike_qty` 컬럼은 그대로 유지). 배치에서 빠진 대여소의 이전 값을 별도로 지우지는 않는다 — `sta_id`가 PK면 테이블 크기가 대여소 수만큼 고정되므로 지울 필요가 없고, 그 값을 최신으로 볼지는 읽는 쪽이 판단한다. 대신 `apps/api`의 `/alerts`가 `WHERE batch_run_at >= now() - ALERTS_FRESHNESS_WINDOW_MIN분`으로 명시적 신선도 조건을 걸어, 배치가 멈춘 대여소의 낡은 값이 최신인 것처럼 섞이지 않게 한다(`MAX(batch_run_at)` 서브쿼리는 더 이상 필요 없음). 이미 복합 PK로 이력이 쌓인 볼륨은 대여소별 최신 `batch_run_at` 행만 남기고 단일 PK로 전환하며, `bike_qty` 컬럼이 없는 더 오래된 볼륨도 `ADD COLUMN IF NOT EXISTS`로 함께 보정한다.

## 결과
`station_urgency`의 row 수가 대여소 수만큼 고정되고 무한 증가하지 않는다. `station_urgency`를 읽는 소비자가 늘어나면(예: 재배치 라우트 배치) 그쪽도 동일한 신선도 조건을 직접 챙겨야 한다 — 테이블 자체가 강제해주지는 않는다.

---

# 아무도 안 건드린 proposed 라우트는 다음 배치가 지운다

## 배경
`route_id`를 매 사이클 새 UUID로 발급하는 건(운영자가 바꿔놓은 status를 배치 재실행이 덮어쓰지 않기 위함) 의도한 대로 동작하지만, 부작용이 있다 — `_dispatched_qty()`가 `dispatched` 상태만 순수요 계산에서 빼기 때문에, 아직 아무도 안 건드린 `proposed` 라우트는 다음 사이클에서도 그대로 다시 계산 대상에 들어가 거의 같은 내용의 라우트가 또 생긴다. 한 권역의 픽업 수요만 트럭 용량(20대)을 넘어도 사이클 하나에서 여러 라우트가 나오므로(`routes.py`의 `while` 분할), 바쁜 시간대엔 5분마다 수십 개씩 새로 생기고 예전 것은 그대로 남아 하루 수천 건 단위로 쌓일 수 있다.

5분마다 재계산하는 것 자체는 맞다 — 재고·예측이 계속 바뀌므로 최신 정보로 다시 만든 라우트가 더 정확하다. 문제는 "재계산"이 아니라 "예전 미처리 제안을 안 지우고 쌓아두는 것"이다.

## 결정
`loader`가 `rebalance_routes`를 적재하기 직전에, 아직 `proposed`인(=아무도 안 건드린) 라우트와 그 스톱을 지우고 나서 이번 사이클의 새 라우트를 넣는다(`loader/main.py:_retire_stale_proposed_routes`). `dispatched`/`completed`로 이미 넘어간 라우트는 운영자가 실제로 처리한 기록이므로 건드리지 않는다. `cancelled`로 상태만 바꿔서 남기는 대신 완전히 삭제하는 이유는, 아무도 안 건드린 제안은 "무슨 일이 일어났다"는 기록 자체가 없고(사람이 아무 조작도 안 함), `rebalance/routes_main.py`가 매 사이클 결과를 이미 S3(`routes/`, `route_stops/`)에 영구 저장해두므로 RDS에서 지워도 "그때 뭘 제안했었는지"라는 정보를 잃지 않기 때문이다 — station_urgency(#124)와 같은 논리다.

## 결과
`proposed` 상태 row는 항상 권역 수(11개) 이하로 유지된다. `dispatched`/`completed`는 여전히 계속 쌓이지만, 실제 트럭 운행 건수만큼만 늘어나 사이클마다 늘어나는 것보다 훨씬 느리다 — 오래된 `completed`/`cancelled`(향후 운영자가 명시적으로 취소하는 경우)를 언제까지 남길지는 위에서 이미 미뤄둔 질문 그대로 남아있다.
