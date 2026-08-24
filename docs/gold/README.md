# Gold/PostGIS 문서 안내

Gold는 대시보드와 재배치 운영이 읽는 최신 serving projection이다. 원천과 학습 이력은
S3 Bronze/Silver가 소유하고, PostgreSQL/PostGIS에는 현재 소비 계약에 필요한 projection만
원자적으로 게시한다.

## 문서 읽는 순서

1. 전체 구조는 [목표 ERD](./target-erd.md)에서 확인한다.
2. 컬럼 의미는 [데이터 사전](./data-dictionary.md), 입력 lineage는
   [원천–목표 매핑](./source-target-mapping.md)에서 확인한다.
3. Publisher를 수정할 때는 [Publication byte contract](./publication-contract-v1.md)를
   먼저 확인한다.
4. 변경 검증은 [통합 검증 가이드](./integration-validation.md)를 따른다.

## 현재 계약 문서

| 문서 | 역할 | 최종 기준 |
| --- | --- | --- |
| [목표 ERD](./target-erd.md) | 테이블 grain, 관계, 키와 생명주기 | `target-schema.sql`과 loader publisher |
| [데이터 사전](./data-dictionary.md) | column 의미, 단위, nullable과 code | `target-schema.sql` |
| [원천–목표 매핑](./source-target-mapping.md) | Collector·ML·seed에서 Gold와 API까지의 lineage | loader와 API 코드 |
| [Publication byte contract](./publication-contract-v1.md) | canonical bytes, fingerprint, manifest와 ID | `libs/core` publication 코드 |
| [통합 검증 가이드](./integration-validation.md) | 계층별 검증 수준, 격리 runner와 안전 경계 | 실제 test와 runner 코드 |

## 실행 가능한 계약

| 파일 | 역할 |
| --- | --- |
| `target-schema.sql` | 빈 PostGIS DB에 적용하는 Gold baseline |
| `target-schema-validation.sql` | DDL 제약·공간·publication 검증 SQL |
| `target-schema-concurrency-validation.sh` | 두 session advisory-lock 경쟁 검증 |
| `dispatch-center-seed.yaml` | 현재 배차센터 seed와 좌표 품질 metadata |

Weather grid seed는 별도 YAML로 유지하지 않는다. `loader/gold/weather_grid.py`가 단기예보와
초단기예보 source YAML의 동일한 34개 격자를 검증해 canonical seed를 생성한다.

`make test-gold-transition-available`은 과거 SSOT commit의 문서 bytes를 고정한 전환
runner다. 현재 문서 변경이 포함된 worktree에서는 precheck 실패가 정상이며, 새 기준
commit으로 `SSOT_COMMIT`을 갱신하기 전까지 현재 PASS 근거로 사용하지 않는다.

## 보관 기록

| 문서 | 성격 |
| --- | --- |
| [스키마 설계 평가](./design-evaluation.md) | #129 설계 반복과 2026-08-20 검증 점수 기록 |

## 현재 물리 범위

`public` schema에는 다음 10개 serving table이 있다.

```text
weather_grid
dispatch_center
station
station_stock
station_demand_forecast
weather_forecast
event
station_urgency
rebalance_route
rebalance_route_stop
```

`gold_meta` schema에는 API가 직접 읽지 않는 `publication_state` 제어 table 하나가 있다.

Route는 `proposed → dispatched → completed` 또는 `dispatched ↔ cancelled` 생명주기를
가진다. 완료·취소 route는 dismiss할 수 있으며, 현재 restore API는 동일 route ID와 stop을
보존한 채 cancelled route를 dispatched로 되돌린다.

## 코드 기준 위치

- Schema bootstrap·check: `ops/postgres/`
- Gold publisher: `loader/gold/`
- Publication CLI: `loader/gold_cli.py`, `loader/serving_cli.py`
- 공통 canonical·manifest·transaction 계약: `libs/core/src/core/gold_publication/`
- Gold 소비 query와 freshness: `apps/api/`
- 계약·PostGIS integration test: `loader/tests/`, `apps/api/tests/`, `ops/gold/tests/`
