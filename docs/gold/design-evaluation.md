# Gold 스키마 설계 평가 기준

## 목적

이 문서는 #129의 ERD 개선을 점수화하고 종료 시점을 판정한다. 평가는 실제 원천,
현재 파이프라인, Gold 소비 코드, `de-project` 표준, 실행 가능한 PostGIS DDL을
근거로 한다. 문서만 서로 일치하고 실제 데이터 흐름과 어긋나는 설계는 합격으로
보지 않는다.

## 필수 산출물

- `docs/adr/0006-gold-postgis-schema.md`: 선택과 대안, 운영 전제
- `docs/gold/target-erd.md`: 테이블 grain, 관계, 키, 제약, 수명주기
- `docs/gold/data-dictionary.md`: 모든 테이블·컬럼·코드·단위의 단일 정의
- `docs/gold/source-target-mapping.md`: 모든 생산자와 소비자의 추적표
- `docs/gold/publication-contract-v1.md`: manifest·fingerprint·route ID의 byte SSOT
- `docs/gold/target-schema.sql`: 빈 PostGIS DB에서 실행 가능한 목표 DDL
- `docs/gold/target-schema-validation.sql`: 정상·오류·공간 조회 검증 SQL
- `docs/gold/target-schema-concurrency-validation.sh`: 두 세션 잠금 경쟁 검증
- 이 문서의 반복 기록: 점수, 발견 사항, 변경, 남은 위험

## 점수표

| 영역 | 배점 | 만점 조건 |
| --- | ---: | --- |
| 원천 추적성 | 10 | Collector·Normalizer·ML·재배치 산출물을 빠짐없이 목표 또는 제외 사유에 연결한다. |
| 소비자 추적성 | 10 | API·배치·ML·운영 쿼리의 컬럼, grain, 최신성, 성능 요구를 빠짐없이 연결한다. |
| grain·키·충돌 방지 | 15 | 모든 테이블의 1행 의미와 PK/UK가 명확하고 실제 원천끼리 충돌하지 않는다. |
| 관계·수명주기 | 10 | FK, 삭제/비활성화, seed 순서, upsert, 보존 정책과 소유자가 정해져 있다. |
| 시간 의미 | 5 | 날짜/일시, UTC/KST, 기준/대상/관리 시각이 이름과 타입으로 구분된다. |
| 공간 의미 | 10 | SRID, 좌표 순서, Point/Polygon, 거리 단위, 경계점·미매핑 정책이 정해져 있다. |
| 실행 가능 DDL | 10 | 고정 PostGIS 버전의 빈 DB에 전체 DDL이 오류 없이 적용된다. |
| 제약·부정 검증 | 5 | FK, CHECK, UNIQUE, 상태 전이, 유효하지 않은 공간값이 예상대로 거부된다. |
| 적재·upsert 검증 | 5 | 최신행 guard, 소스 충돌, 재실행, 보존 시나리오가 대표 데이터로 검증된다. |
| 조회·인덱스 검증 | 5 | 현재 소비 쿼리가 표현 가능하고 공간/시간 인덱스 사용 계획을 확인한다. |
| 마스터 운영성 | 5 | 공식성·출처·버전·갱신 주기·품질 등급·미해결 행 처리 방식이 명시된다. |
| 환경·전환 계획 | 5 | 로컬과 RDS의 PostGIS 활성화 차이, 새 baseline, 로컬 reset, 호환 경계가 명시된다. |
| 명명·문서 일치 | 5 | ERD·DDL·사전·매핑 문서가 같은 테이블/컬럼/코드/nullable을 말한다. |

총점은 100점이다. 부분 점수는 해당 영역의 요구 중 충족된 비율로 계산하고, 근거가
없는 주장은 미충족으로 처리한다.

## 심각도

- **Critical**: 데이터 유실·다른 의미의 행 덮어쓰기·DDL 실행 불가·현재 핵심 소비자
  중단처럼 설계 자체를 사용할 수 없게 만드는 문제
- **Major**: 마스터 출처, 식별자 안정성, nullable, 상태 전이, 보존 정책처럼 구현 전
  반드시 결정해야 하는 구조 문제
- **Minor**: 구조를 바꾸지 않고 문구, 예시, 선택적 인덱스로 해결할 수 있는 문제

## 종료 조건

아래 조건을 모두 만족할 때만 설계 루프를 종료한다.

1. 총점 95점 이상이며 자동으로 판정 가능한 검사가 모두 통과한다.
2. 열린 Critical과 Major가 0건이다.
3. 모든 원천과 모든 현재 Gold 소비자가 매핑되어 있다.
4. 목표 DDL을 빈 PostGIS DB에 적용하고 대표 적재·upsert·공간 조회·부정 검증을
   통과한다.
5. 독립적인 적대적 전수 리뷰를 두 번 연속 수행했을 때 테이블, 키, 관계, 컬럼을
   바꿀 구조 문제가 새로 나오지 않는다.
6. 운영 배포나 기존 운영 DB 변경은 수행하지 않는다.

## 반복 기록

아래 C/M 수는 같은 원인의 지적을 하나로 합친 해당 반복 종료 시점의 open category 수다.

| 반복 | 기준 | 점수 | Critical | Major | 구조 변경 | 결과 |
| --- | --- | ---: | ---: | ---: | --- | --- |
| 0 | 사전 감사 `09885fb` | 55 | 4 | 14 | 기준선 | 제품별 날씨 충돌, 비원자 route, 과대 행정구역 모델, 실행 DDL 부재 확인 |
| 1 | serving scope snapshot | 80 | 2 | 9 | 있음 | dashboard 계약으로 public 10-table을 확정하고 dong·gu·실황·event spot 제거 |
| 2 | publication/invariant snapshot | 95 | 0 | 4 | 있음 | `gold_meta` watermark, 원자 projection, 상태 전이와 statement lock 확정 |
| 3 | validated final snapshot | 99 | 0 | 0 | 없음 | byte contract·부정/인덱스/2-session 검증과 문서 parity 완료 |

각 반복에서는 한 묶음의 구조 문제만 수정하고, DDL 검증과 문서 일치 검사를 다시
수행한다. 점수가 내려가거나 새 Critical/Major가 생긴 변경은 원인을 기록하고 다음
반복에서 교정한다.

## 최종 점수 근거

| 영역 | 점수 | 근거 |
| --- | ---: | --- |
| 원천 추적성 | 10/10 | 10개 Collector source와 seed·ML·urgency·route를 Gold/Silver/제외로 전수 분류했다. |
| 소비자 추적성 | 10/10 | API·Web·route 상태 전이의 필드, 오류, 최신성, 정렬을 target query와 연결했다. |
| grain·키·충돌 방지 | 15/15 | 10개 public table과 meta state의 grain·PK/UK, 날씨 resolver와 행사 source key를 고정했다. |
| 관계·수명주기 | 10/10 | station/center/route FK, 활성화, projection 교체, terminal 보존을 정했다. |
| 시간 의미 | 5/5 | `_dt`/`_dttm`, UTC/KST, base/target/logical/lifecycle 및 미래 상한을 분리했다. |
| 공간 의미 | 10/10 | Point 4326, X/Y, geography meter, 안전 box와 좌표 source를 고정하고 불필요한 Polygon을 제외했다. |
| 실행 가능 DDL | 10/10 | `postgis/postgis:16-3.5`의 빈 DB에서 transaction baseline이 통과했다. |
| 제약·부정 검증 | 5/5 | 타입·FK·CHECK·EMPTY·state·route lifecycle·metadata 소유권 부정 fixture가 통과했다. |
| 적재·upsert 검증 | 5/5 | stale/no-op/correction, snapshot 교체, terminal 보존과 rollback을 검증했다. |
| 조회·인덱스 검증 | 5/5 | meter 거리와 시간 조회 결과 및 GiST/B-tree plan 사용을 검증했다. |
| 마스터 운영성 | 4/5 | seed source·commit·hash·accuracy는 고정했지만 현재 센터 Point는 현장 검증 전 근사값이다. |
| 환경·전환 계획 | 5/5 | PostGIS image, first baseline, legacy init/seed 폐기와 local reset 경계를 분리했다. |
| 명명·문서 일치 | 5/5 | ERD·DDL·사전·mapping·byte contract가 같은 10-table/코드/nullability를 사용한다. |
| **합계** | **99/100** | 종료 기준 95점 이상, open Critical/Major 0건 |

남은 1점은 구조 결함이 아니라 `dispatch-center-seed.yaml`의 좌표 품질 등급이
`landmark_approximation`인 운영 품질 부채다. source와 정확도, 미검증 날짜를 숨기지 않고
기록했으며 현장 검증 좌표로 교체해도 테이블·키·관계·컬럼은 바뀌지 않는다.

## 실행 검증 기록

2026-08-20 KST에 운영 DB가 아닌 일회성 local container
`postgis/postgis:16-3.5`에서 다음을 확인했다.

1. 빈 `gold129_contract_0820` DB에 `target-schema.sql` 전체 적용: 성공.
2. 같은 DB에서 `target-schema-validation.sql`: 모든 정상·부정·공간·plan fixture 성공 후
   `ROLLBACK`.
3. 같은 DB에 baseline 재적용: 기존 relation을 발견해 의도대로 exit 3 fail-fast.
4. 새 `gold129_concurrency_final_0820` DB에서
   `target-schema-concurrency-validation.sh`: publication stale writer, topology-vs-route,
   dispatch-vs-stop 세 경쟁과 no-timeout/deadlock까지 5개 PASS.
5. artifact/EMPTY/input/manifest/ID-set/station-window/relocation/route-coverage/cultural
   SHA-256과 route UUIDv5 회귀값을 독립 재계산해 문서 값과 일치.
6. `bash -n docs/gold/target-schema-concurrency-validation.sh`, `git diff --check`: 성공.

검증용 DB 외의 RDS·운영 DB, S3 artifact와 기존 local volume은 변경하지 않았다.

## 독립 적대 리뷰

| 순서 | 범위 | Critical | Major | 구조 변경 필요 | 판정 |
| --- | --- | ---: | ---: | --- | --- |
| 1 | 원천·소비자·DDL·byte contract 전수 재검 | 0 | 0 | 없음 | clean pass |
| 2 | 문서 parity·dependency binding·검증 증거 독립 재검 | 0 | 0 | 없음 | clean pass |

두 번 연속 독립 리뷰에서 테이블·키·관계·컬럼 구조 변경이 없었고 최종 open
Critical/Major도 0건이다. 점수 99점, 실행 검증, 전수 매핑과 비운영 원칙을 포함한 종료
조건 1~6을 모두 만족하므로 #129 설계 반복을 종료한다. `source-target-mapping.md`의 구현
전환 차단 목록은 후속 PR의 선행 조건이며 이 설계의 미결 C/M은 아니다.
