# ADR-0006: Gold RDS는 소비자 중심 PostGIS 서빙 모델로 구성한다

- 상태: 채택
- 결정일: 2026-08-19
- 작성자: Data Engineering 2팀
- 대체 대상: ADR-0002
- 대체한 ADR: 없음

## 배경

Gold는 S3 데이터를 그대로 복제하는 저장소가 아니라 API와 재배치 워크플로가 안정적으로 조회·변경할 서빙 계층이어야 한다. 초기 스키마는 개발 과정의 테이블과 증분 init 스크립트가 누적돼 현재 소비 계약, 공간 연산과 publication 단위를 일관되게 보장하지 못했다.

ML 학습·추론과 Normalizer는 S3 Silver·Archive를 읽는다. 따라서 생활인구, 대여이력, 행정 계층과 원천 제품별 날씨 이력은 Gold에 복제하지 않고 실제 서비스 소비자가 요구하는 최신 projection만 관리한다.

## 결정

### 1. Gold의 범위를 10개 public 테이블로 제한한다

- 기준정보: `weather_grid`, `station`, `dispatch_center`
- 최신 서빙 상태: `station_stock`, `station_demand_forecast`, `weather_forecast`, `event`, `station_urgency`
- 운영 aggregate: `rebalance_route`, `rebalance_route_stop`

별도 `gold_meta` schema의 `publication_state`는 publication별 logical time, revision, manifest와 fingerprint를 기록한다. 행정구역, 생활인구, 대여이력과 원천 날씨 이력은 S3에 남긴다.

### 2. 위치는 PostGIS Point를 단일 원본으로 사용한다

`station`, `event`, `dispatch_center`의 위치는 `geometry(Point, 4326)`로 저장하고 유효한 서울 인근 좌표 범위를 제약한다. API의 위도·경도와 거리 값은 `ST_X`, `ST_Y`, `ST_Distance(...::geography)`로 파생하며 세 Point 컬럼에 GiST index를 둔다.

### 3. 최신 projection을 완전한 단위로 교체한다

재고와 긴급도는 대여소당 최신 한 행을 유지하고, 수요·날씨 예측은 현재 서빙 horizon만 유지한다. 날씨 publisher는 기준 시각 다음 13개 정각을 준비해 API가 다음 12시간을 끊김 없이 제공할 수 있게 한다.

각 publisher는 입력 manifest와 row count를 검증한 뒤 대상 행과 `publication_state`를 같은 transaction에서 갱신한다. `claim_publication`과 advisory lock으로 동일하거나 오래된 publication의 덮어쓰기를 막고, 정상적인 빈 결과도 명시적인 revision으로 게시한다.

### 4. 재배치 경로를 상태가 있는 aggregate로 관리한다

`rebalance_route` header와 하나 이상의 `rebalance_route_stop`을 하나의 aggregate로 취급한다. stop은 1부터 연속된 `visit_no`, `pickup` 또는 `dropoff`, 양수 `bike_cnt`를 가져야 하며 DB trigger가 station·배차센터 관계와 변경 가능 상태를 검사한다.

경로는 `proposed → dispatched → completed` 또는 `proposed → dispatched → cancelled`로 전이한다. 완료·취소 경로는 화면에서 숨김 처리할 수 있고, 취소 경로 복원은 원본과의 관계 및 열린 복원 작업의 유일성을 보장한다. API 상태 변경과 publisher는 동일한 topology·route advisory lock 순서를 사용한다.

### 5. 비어 있는 PostgreSQL 16 DB에 단일 baseline을 적용한다

`docs/gold/target-schema.sql`을 Gold DDL의 단일 진실 공급원으로 사용한다. 기존 relation이 있는 DB에는 fail-fast하며 테이블을 자동 삭제하거나 과거 임시 스키마를 변환하지 않는다.

로컬과 운영 기준은 PostgreSQL 16과 PostGIS 3.4다. 로컬 Compose는 새 volume 최초 초기화 때 baseline을 적용하고 read-only schema check가 relation, 함수, trigger, GiST index와 ACL을 검증한다. RDS는 `ops/postgres/bootstrap_rds.sh`로 PostGIS 3.4 가용성을 확인한 뒤 같은 baseline을 적용한다. 기존 운영 DB의 후속 변경은 명시적인 `ops/postgres/migrations/` SQL로 수행한다.

## 근거

- 소비자 중심 경계는 S3와 RDS에 같은 이력을 중복 저장하는 비용과 정합성 책임을 줄인다.
- Point 하나만 저장하면 위도·경도와 공간 연산용 좌표가 서로 달라질 수 없다.
- publication state와 transaction 교체는 서로 다른 배치가 섞인 응답과 stale overwrite를 차단한다.
- route를 DB aggregate로 관리하면 운영자가 변경한 상태를 다음 5분 배치가 덮어쓰지 않는다.
- 단일 clean baseline은 최초 운영 스키마를 재현 가능하게 만들고, 기존 DB 변경은 별도 migration으로 명확히 분리한다.

## 결과

Gold는 API와 재배치 운영에 필요한 최신 상태만 제공하고, 장기 이력과 재처리 책임은 S3에 남는다. 공간 거리 계산, publication 순서, route topology와 상태 전이가 DB 제약 및 transaction으로 보호된다.

대신 publisher는 manifest 검증, dependency 일치, advisory lock 순서와 transaction 경계를 지켜야 한다. Gold에 새 테이블을 추가하려면 실제 소비자, 조회 grain, freshness와 보존 기간을 먼저 정의해야 한다.

이 결정은 [ADR-0002](0002-gold-schema-design.md)의 초기 물리 모델과 증분 init 방식을 대체한다.

## 구현 및 검증 근거

- 스키마 SSOT: `docs/gold/target-schema.sql`
- 스키마 시작 차단 검사: `ops/postgres/check_gold_schema.sql`
- 로컬 초기화: `ops/postgres/init/002_gold_schema.sh`
- RDS 초기화: `ops/postgres/bootstrap_rds.sh`
- publication 구현: `loader/gold/`
- API 공간 조회·route 상태 변경: `apps/api/queries.py`
- Gold publication 통합 테스트: `loader/tests/test_gold_*_integration.py`
- PostGIS API 통합 테스트: `apps/api/tests/test_postgis_integration.py`
- 실행 계약: `docs/gold/publication-contract-v1.md`
- 물리 모델: `docs/gold/target-erd.md`, `docs/gold/data-dictionary.md`
