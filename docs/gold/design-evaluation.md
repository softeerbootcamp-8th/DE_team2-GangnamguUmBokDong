# Gold 스키마 설계 평가 기록

> **보관 문서:** 2026-08-20, commit `6a5cbb9` 시점의 #129 설계 종료 판단을 기록한다.
> 현재 계약이나 테스트 결과를 주장하는 문서가 아니다.

## 현재 문서를 찾는 경우

| 필요한 정보 | 현재 기준 |
| --- | --- |
| 물리 테이블·제약 | [target-schema.sql](target-schema.sql) |
| 테이블 grain·관계 | [target-erd.md](target-erd.md) |
| 컬럼·단위·상태 코드 | [data-dictionary.md](data-dictionary.md) |
| 원천부터 API까지 lineage | [source-target-mapping.md](source-target-mapping.md) |
| Artifact·fingerprint·manifest | [publication-contract-v1.md](publication-contract-v1.md) |
| 현재 검증 방법과 한계 | [integration-validation.md](integration-validation.md) |
| 설계 결정과 대안 | [ADR-0006](../adr/0006-gold-postgis-schema.md) |

## 평가 목적

당시 Gold ERD가 실제 원천, 생산 코드, API 소비 계약과 실행 가능한 PostGIS DDL을 함께
만족하는지 평가하고 설계 반복의 종료 시점을 결정했다. 문서끼리만 일치하고 실제 데이터
흐름에서 검증되지 않은 항목은 완료로 보지 않았다.

## 평가 기준

| 영역 | 배점 | 핵심 조건 |
| --- | ---: | --- |
| 원천·소비자 추적성 | 20 | 모든 생산자와 소비자를 Gold, Silver 또는 제외 사유에 연결 |
| Grain·key·관계·생명주기 | 25 | 행 의미, PK/UK/FK, 충돌·삭제·보존 정책 확정 |
| 시간·공간 의미 | 15 | UTC/KST, 기준/대상 시각, SRID·축·거리 단위 확정 |
| 실행 DDL·제약 | 15 | 빈 PostGIS 적용과 정상·부정 fixture 검증 |
| 적재·조회·index | 10 | 재실행·stale·원자 교체와 소비 query plan 검증 |
| Master·환경 운영성 | 10 | Seed 출처·품질, local/RDS 전환 경계 명시 |
| 명명·문서 일치 | 5 | ERD·DDL·사전·mapping의 이름과 nullable 일치 |
| **합계** | **100** | |

평가 당시 종료 조건은 총점 95점 이상, 열린 Critical/Major 0건, 전체 lineage 매핑,
빈 PostGIS 검증 통과, 두 번 연속 구조 변경 없는 적대 리뷰였다.

## 반복 결과

| 반복 | 점수 | Critical | Major | 주요 결과 |
| --- | ---: | ---: | ---: | --- |
| 0 | 55 | 4 | 14 | 날씨 충돌, 비원자 route, 과도한 행정구역 모델과 DDL 부재 확인 |
| 1 | 80 | 2 | 9 | Dashboard 기준 10개 serving table 확정 |
| 2 | 95 | 0 | 4 | Publication watermark, 원자 projection, 상태 전이와 lock 확정 |
| 3 | 99 | 0 | 0 | Byte contract, 부정·index·two-session 검증과 문서 parity 완료 |

## 최종 판단

당시 평가는 다음 구조를 확정한 것으로 기록했다.

- `public` schema의 10개 serving table
- `gold_meta.publication_state` 기반 publication watermark
- PostGIS Point 4326과 geography meter 거리
- Source별 event identity와 weather resolver
- Station·stock·forecast·urgency의 완전 projection 게시
- Header·stop aggregate로 관리하는 route와 상태 전이
- S3 Bronze/Silver 이력과 PostgreSQL 최신 serving projection의 책임 분리

최종 점수는 **99/100**이었다. 남은 1점은 구조 결함이 아니라 dispatch center 좌표가
현장 측량값이 아닌 `landmark_approximation` 중심이라는 운영 품질 부채였다.

## 당시 실행 기록

2026-08-20 KST에 운영 DB가 아닌 일회성 local `postgis/postgis:16-3.5` container에서
다음을 실행한 것으로 기록돼 있다.

1. 빈 DB에 `target-schema.sql` 적용
2. `target-schema-validation.sql`의 정상·부정·공간·plan fixture 실행 후 rollback
3. Baseline 재적용이 exit code 3으로 실패하는 fail-fast 확인
4. 별도 DB에서 publication, topology-route, dispatch-stop lock 경쟁 확인
5. 당시 byte contract SHA-256과 route UUIDv5 회귀값 재계산
6. Shell 문법과 `git diff --check` 확인

두 차례 적대 리뷰에서 새 Critical/Major와 테이블·key·관계·컬럼 구조 변경이 나오지 않아
#129 설계 반복을 종료했다.

## 기록 해석 시 주의사항

- `99/100`은 2026-08-20 설계 평가 점수이며 현재 코드 품질 점수가 아니다.
- 당시 container tag는 현재 격리 runner의 `postgis/postgis:16-3.4`와 다르다.
- 이 기록은 운영 RDS migration, 실제 S3 credential, Airflow scheduler, API와 Web을 잇는
  live E2E를 증명하지 않는다.
- 이후 추가된 route dismiss/restore, publisher와 API 동작은 현재 계약 문서와 코드에서
  확인해야 한다.
- 현재 PASS를 주장하려면 [통합 검증 가이드](integration-validation.md)의 명령과 증거
  수준을 사용해 다시 실행해야 한다.
