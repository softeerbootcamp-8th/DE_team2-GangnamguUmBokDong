# ADR-0002: 초기 Gold 스키마를 raw SQL로 관리한다

- 상태: 대체됨
- 결정일: 2026-08-13
- 작성자: Data Engineering 2팀
- 대체 대상: 없음
- 대체한 ADR: ADR-0006

## 배경

초기 대시보드와 API 개발에는 mock 대신 조회할 PostgreSQL 테이블이 필요했지만, 서비스 계약과 스키마가 아직 자주 바뀌고 있었다. 저장소에는 Alembic 같은 migration 체계가 없었고 운영 데이터도 쌓이기 전이었다.

## 결정

초기 Gold 스키마는 Docker 초기화 단계에서 raw SQL로 생성하고, 개발 중 스키마 변경은 새 로컬 volume에 다시 적용한다. 대여소 재고·예측·긴급도·재배치 경로처럼 API가 사용하는 데이터를 PostgreSQL에 두되, 원본 이력은 S3에 보존한다.

당시 구현 과정에서 파생값 계산 위치, 이력 보존 여부와 route 상태 모델을 여러 차례 수정했다. 이 파일의 이전 판본에 기록된 개별 테이블과 retention 결정은 초기 구현의 변화 과정이며 현재 계약으로 사용하지 않는다.

## 근거

운영 데이터가 없는 단계에서는 migration 체계를 먼저 도입하는 것보다 SQL 한 벌을 빠르게 검증하는 편이 단순했다. S3를 이력 저장소로 유지하면 RDS는 API와 운영 워크플로에 필요한 최신 projection에 집중할 수 있었다.

## 결과

초기 API와 재배치 기능을 빠르게 개발할 수 있었지만, 여러 변경이 init 스크립트에 누적되면서 현재 물리 모델과 결정의 경계가 불명확해졌다.

[ADR-0006](0006-gold-postgis-schema.md)이 소비자 중심 Gold 경계, PostGIS 물리 모델, publication 원자성과 clean baseline 적용 방식을 다시 확정해 이 ADR 전체를 대체한다. raw SQL을 사용하는 원칙은 ADR-0006의 `docs/gold/target-schema.sql` 단일 baseline으로 계승됐으며, 과거 증분 init 스크립트와 테이블명은 더 이상 기준이 아니다.

## 관련 자료

- [ADR-0006](0006-gold-postgis-schema.md)
- `docs/gold/target-schema.sql`
- `ops/postgres/init/002_gold_schema.sh`
- `ops/postgres/bootstrap_rds.sh`
