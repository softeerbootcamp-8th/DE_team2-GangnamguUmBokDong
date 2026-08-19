# Gold PostGIS 통합 검증 runbook

## 목적과 현재 판정

이 문서는 [#155](https://github.com/softeerbootcamp-8th/DE_team2-GangnamguUmBokDong/issues/155)의
검증 범위와 한계를 재현 가능한 형태로 고정한다. 구현 기준은
`chore/gold-schema-postgis-redesign`의 최종 commit
`6a5cbb931f58c7a57ff7e3683fb993c57512244e`이다. 물리 DDL과 데이터 계약은
[target-schema.sql](target-schema.sql), [data-dictionary.md](data-dictionary.md),
[source-target-mapping.md](source-target-mapping.md),
[publication-contract-v1.md](publication-contract-v1.md)를 함께 사용한다.

현재 전환 전체의 판정은 **PARTIAL/BLOCKED**다. 아래 전용 명령이 성공하면 현재 구현된
범위의 회귀 증거는 모두 PASS지만, 파생 producer/publisher와 운영 권한 계약이 없으므로
최초 publication 전체 체인이 완성됐다는 뜻은 아니다. 특히 직접 target에 넣은 fixture,
monkeypatch, mock API로 통과한 테스트를 publication E2E 증거로 승격하지 않는다.

관련 작업은 상위 [#149](https://github.com/softeerbootcamp-8th/DE_team2-GangnamguUmBokDong/issues/149)와
stacked PR [#156](https://github.com/softeerbootcamp-8th/DE_team2-GangnamguUmBokDong/pull/156) →
[#157](https://github.com/softeerbootcamp-8th/DE_team2-GangnamguUmBokDong/pull/157) →
[#158](https://github.com/softeerbootcamp-8th/DE_team2-GangnamguUmBokDong/pull/158) →
[#159](https://github.com/softeerbootcamp-8th/DE_team2-GangnamguUmBokDong/pull/159) →
[#160](https://github.com/softeerbootcamp-8th/DE_team2-GangnamguUmBokDong/pull/160)에 있다.
PR #159와 #160은 blocker가 해소될 때까지 Draft이며, 이 runbook도 merge나 배포 가능 판정을
대신하지 않는다.

## 판정 기준

- **PASS**: 현재 repository만으로 실행 가능한 경계를 실제 코드와 격리 DB에서 검증한다.
- **PARTIAL**: 테스트 자체는 PASS할 수 있지만 fixture나 mock 경계 때문에 publication 전체
  흐름의 증거는 아니다.
- **BLOCKED**: #129 SSOT에 필요한 값 또는 byte contract가 없어서 추정 구현하지 않은
  범위다.

| 검증 영역 | 판정 | 현재 증거 | 이 증거로 주장할 수 없는 것 |
| --- | --- | --- | --- |
| clean bootstrap, schema check, baseline 재적용 | PASS | 빈 PostGIS 16-3.5에 최종 DDL 적용, read-only checker, 두 번째 적용 exit 3과 기존 상태 불변 | 기존 DB migration, RDS 적용 |
| DDL 제약, 공간 조회·GiST plan | PASS | transaction fixture의 정상·오류·Point·meter 거리·index plan 및 추가 edge SQL | publisher가 만든 실제 lineage |
| 두 session lock 경쟁 | PASS | publication, topology/route, dispatch/stop advisory-lock 경쟁과 timeout/deadlock 부재 | 미구현 파생 publisher와 API의 장시간 부하 경쟁 |
| publication 공통 기반 | PASS | canonical bytes, immutable object, manifest-last, replay/stale/correction/EMPTY, 미래 시각, 원자 rollback | 실제 S3/RDS credential과 role ACL |
| Collector authority와 source publisher | PASS | immutable source manifest fixture → 공통 publication 경계 → 실제 PostGIS target/state 경로 | 운영 API 응답의 완전성, 승인 전 weather seed, 신규 station 활성화 |
| demand·urgency·route 파생 계산 | PARTIAL | expected set, 12 horizon, 반올림, EMPTY, route coverage/UUID/capacity의 순수 projection과 artifact readback | immutable upstream producer, Gold publication manifest, target/state mutation, CLI/DAG 실행 |
| API | PARTIAL | 실제 PostGIS에서 좌표·거리·freshness·404/409/503·route 전이를 조회 | source/derived publisher가 채운 target을 읽는 publication E2E |
| Web | PARTIAL | mock API에서 stale clear, 404/503, polling race, weather와 event 상태 및 production build | 실제 API/DB와 연결한 browser E2E |
| Airflow | PARTIAL | DAG import, dependency graph, allowlisted publisher CLI command wiring | scheduler가 source부터 target까지 실행한 task E2E |
| publisher/API 최소 권한 | BLOCKED | `PUBLIC` revoke만 존재 | 서비스별 role·credential·GRANT/REVOKE 검증 |
| weather seed와 station activation | BLOCKED | fixture seed와 inactive station 경계만 검증 | 승인 seed를 사용한 최초 게시와 신규·재활성 station의 활성화 |
| full derived publication과 최초 전체 체인 | BLOCKED | 없음 | seed → source → demand → urgency → route → API의 하나의 lineage chain |

## A. 현재 한 명령으로 검증 가능한 범위

repository root에서 다음 명령 하나만 실행한다.

```bash
make test-gold-transition-available
```

Make target은 [run_transition_validation.sh](../../ops/gold/tests/run_transition_validation.sh)를
호출한다.

명령은 장시간 전체 CI 대신 다음 focused evidence를 순서대로 검증한다. 앞 단계가
실패하면 뒤 단계의 성공을 근거로 사용하지 않는다.

1. 필요한 executable과 설치된 project environment를 확인하고, 최종 SSOT 파일이 commit
   `6a5cbb931f58c7a57ff7e3683fb993c57512244e`의 blob과 같은지 검사한다.
2. [bootstrap guard 테스트](../../ops/postgres/tests/test_bootstrap.sh)로 marker 없는 기존
   `PG_VERSION` 경로의 exit 78, wrapper 실패 전파, schema checker fail-closed를 mock
   경계에서 확인한다.
3. 고유 이름의 `postgis/postgis:16-3.5` container를 `--rm`과 PostgreSQL data directory
   tmpfs로 시작한다. mount가 비어 있고 exact tmpfs만 있는지 `docker inspect`로 확인한 뒤
   검증용 baseline DB, schema checker용 Airflow DB와 별도 package integration DB를 만든다.
4. 실제 [target-schema.sql](target-schema.sql)을 빈 DB에 적용하고
   [read-only schema checker](../../ops/postgres/check_gold_schema.sh)와
   [target-schema-validation.sql](target-schema-validation.sql)을 실행한다.
5. 같은 baseline을 다시 적용했을 때 정확히 exit 3으로 실패하고 read-only schema checker가
   다시 통과하는지 확인한다.
6. [target_edge_validation.sql](../../ops/gold/tests/target_edge_validation.sql)로
   NaN/infinity, 5분 초과 미래 시각, 잘못된 Point와 범위 밖 좌표, route aggregate·상태
   전이, PostGIS meter 거리와 GiST plan을 검사한다. stale/correction/EMPTY는 SSOT validation과
   후속 package 테스트에서 검사한다.
7. 같은 disposable container에서
   [target-schema-concurrency-validation.sh](target-schema-concurrency-validation.sh)를 실행해
   실제 두 session의 lock 직렬화와 no-timeout/no-deadlock을 확인한다.
8. Core의 canonical byte·artifact·manifest·transaction 테스트, Collector의 immutable
   source authority 테스트, Loader의 source publisher PostGIS 통합과 demand/urgency/route
   projection 테스트를 실행한다.
9. API 단위 테스트와 같은 disposable DB의 PostGIS 소비 통합 테스트를 실행한다.
10. Airflow DAG import·publisher wiring focused 테스트를 실행한다.
11. Web Vitest와 TypeScript/Vite production build를 실행한다.
12. 시작 시 기록한 tracked status와 종료 status가 같은지 비교하고 `git diff --check`를
    다시 실행한다.

명령의 exit code가 0이고 모든 phase가 PASS일 때만 위 표의 PASS/PARTIAL 증거를 유효하게
본다. DB URL이 없어서 평소 skip되는 Core/Loader/API PostGIS 테스트는 이 명령이 직접
disposable URL을 주므로 skip되어서는 안 된다. 예상하지 않은 skip, xfail 또는 phase 누락은
전체 PASS가 아니다.

### 2026-08-20 KST 실행 기록

최종 runner를 warm local environment에서 실행한 결과는 다음과 같다.

- SSOT blob, shell syntax, bootstrap safety, clean DDL, read-only checker, SSOT validation,
  edge SQL, baseline 재적용 exit 3, schema 재검사와 3개 two-session concurrency contract:
  모두 PASS
- Core 178, Collector 271, Loader 243, API 56, Airflow 58: Python 합계 806 passed,
  skip/xfail/xpass 0
- Web: 24 tests passed, TypeScript/Vite production build PASS
- 최종 tracked status와 `git diff --check`: PASS
- runner 소유 container 잔존 수: 0; named volume 생성·삭제: 0
- warm cache 실제 wall time: 약 61초

이 기록도 위 matrix의 PASS/PARTIAL 경계 안에서만 해석한다. 특히 Python 806 passed를 full
publication E2E 806건으로 표현하지 않는다.

## 실행 안전 경계

### 실행 전

- 전용 worktree의 `test/gold-publication-integration` branch에서 실행한다. 원 repository의
  현재 작업 directory에서 실행하지 않는다.
- `.env`를 만들거나 복사하지 않는다. runner는 `.env.example`이나 shell의 운영 credential을
  DB 연결 입력으로 사용하지 않으며, 검증용 DSN만 각 subprocess에 전달해야 한다.
- `DATABASE_URL`, `GOLD_PUBLICATION_TEST_DATABASE_URL`, `GOLD_API_TEST_DATABASE_URL`,
  `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`가 운영/공유 DB를 가리키는 shell에서
  실행하지 않는다. runner가 별도로 지정한 disposable 값만 허용한다.
- Docker daemon이 local에서 동작하고 `postgis/postgis:16-3.5` image를 실행할 수 있어야
  한다. 검증 실패 시 기존 Compose PostgreSQL이나 RDS로 대신 연결하지 않는다.
- 시작 전 `git status --short`를 기록한다. tracked file 변경이 있으면 검증 결과와 섞이지
  않게 먼저 분리한다.
- Make target 실행에 GNU Make가 필요하다. runner는 Bash, Docker CLI/daemon, `env`, Git,
  `grep`, `mktemp`, npm, `sed`, `tee`, `uv`를 요구한다. Web의 `vitest`, `tsc`, `vite`가
  `apps/web/node_modules/.bin`에 설치돼 있어야 한다. Python dependency는 각 lock을
  `uv run --frozen`으로 사용하고 PostgreSQL client는 disposable container 안의 도구를
  사용한다.

2026-08-20 KST warm cache 실측은 약 61초였다. 일반 warm 환경에서는 1~3분을 예상한다.
image 또는 dependency가 cold인 환경은 network와 disk 속도에 따라 달라지므로 15~30분을
확보한다. image pull과 dependency 준비 시간은 테스트 실행 시간과 분리해서 기록한다.

### 실행 중

- runner가 만든 고유 container 하나와 그 tmpfs DB만 검증 대상이다. Docker Compose,
  named volume, bind-mounted PostgreSQL data directory를 사용하지 않는다.
- tmpfs는 container 종료와 함께 사라진다. 기존 local volume을 inspect·mount·삭제하지
  않으며 `docker compose down -v`, `docker volume rm`, `docker volume prune`를 실행하지 않는다.
- fixture object store는 test double/moto만 사용한다. 실제 S3 bucket, 운영 RDS, 공유 개발
  DB에는 연결하지 않는다.
- runner가 실패하더라도 테스트를 skip/xfail로 바꾸거나 기대 exit code를 완화하지 않는다.
  `target-schema.sql`을 기존 DB에 적용해 원인을 재현하지 않는다.

### 실행 후

- 성공·실패와 관계없이 cleanup trap이 검증용 container를 중지하고 `--rm` 제거했는지
  확인한다. named volume은 애초에 생성하지 않는다.
- `git status --short`를 실행 전 결과와 비교한다. test cache와 build output 외 tracked
  변경이 생겼다면 결과를 유효한 PASS로 기록하지 않는다.
- stdout/stderr의 phase 이름, 총 소요 시간, 최종 exit code를 PR 검증 기록에 남긴다.
  secret, DSN password, 환경 파일 내용은 기록하지 않는다.
- PASS 뒤에도 운영 RDS 적용, 기존 local volume 삭제, PR merge를 수행하지 않는다.

## B. publication E2E로 인정하지 않는 증거

다음 테스트는 해당 계층의 계약을 검증하는 유효한 focused evidence지만, fixture 경계가
publication 시작점이나 실제 producer를 우회한다.

1. [target-schema-validation.sql](target-schema-validation.sql)과 edge SQL은 target과
   `gold_meta`에 SQL fixture를 직접 쓴다. DDL·trigger·index·lock 검증에는 유효하지만
   immutable artifact → fingerprint → publication manifest → target 흐름의 증거는 아니다.
2. [API PostGIS 통합 테스트](../../apps/api/tests/test_postgis_integration.py)는 10개 target에
   fixture를 직접 INSERT한다. Point 축, 거리, freshness, 정확한 12행, route lifecycle과 HTTP
   오류 매핑은 검증하지만 publisher output, `publication_state`, manifest lineage는 검증하지
   않는다.
3. API query/endpoint 단위 테스트는 DB helper를 monkeypatch한다. SQL shape와 404/409/422/503
   매핑의 증거일 뿐 실제 transaction snapshot의 publication E2E 증거가 아니다.
4. Web 테스트는 API 함수를 mock하고 component state를 검사한다. polling 실패와 늦은 응답
   race 방지는 검증하지만 browser → API → PostGIS 체인은 실행하지 않는다.
5. Airflow focused 테스트는 DAG import, task dependency와 CLI 문자열을 검사한다. scheduler가
   Collector, producer, publisher를 실제로 실행했다는 증거가 아니다.
6. source publisher PostGIS 통합 테스트의 정상 경로는 moto immutable object와 공통
   publication 경계를 거쳐 실제 target/state를 바꾸므로 test-level E2E 증거다. 다만
   monkeypatch로 target mutation 중간 예외를 주입한 rollback case는 원자성의 fault-injection
   증거이며 실제 외부 장애의 E2E 재현은 아니다. station activation을 monkeypatch로 연
   topology/route case와 dependency용 target을 직접 INSERT한 case도 activation/full-chain
   증거로 계산하지 않는다.
7. demand·urgency·route 테스트는 canonical Parquet과 locked expected projection을 검증하지만
   production producer, immutable success manifest, 공통 executor를 통한 target/state mutation이
   없다. 따라서 결과가 모두 PASS해도 derived publication E2E는 PARTIAL이다.

## C. BLOCKED 범위와 SSOT 결정 항목

### 서비스 role과 GRANT matrix

[#151 blocker](https://github.com/softeerbootcamp-8th/DE_team2-GangnamguUmBokDong/issues/151#issuecomment-5346082689)에는
`PUBLIC` revoke 이후 서비스별 권한 계약이 없다. 다음을 #129/PR #145의 SSOT에 먼저 확정해야
한다.

1. publisher, API read, route operator에 사용할 login/group role 이름
2. 각 role의 credential 환경변수와 서비스별 DSN 분리 방식
3. publication key·target별 최소 SELECT/INSERT/UPDATE/DELETE와
   `gold_meta.claim_publication()` EXECUTE GRANT matrix
4. API read 권한과 `proposed → dispatched → completed` 상태 전이 권한의 분리 방식

결정 뒤 security bootstrap과 서비스별 연결 설정을 추가하고, publisher role의 허용 DML,
API의 `gold_meta` 접근·일반 target write 거부, route operator의 제한된 상태 전이를 실제
두 role로 검증한다. 이 전에는 role ACL 항목은 BLOCKED다.

### weather grid seed와 station activation

[#152](https://github.com/softeerbootcamp-8th/DE_team2-GangnamguUmBokDong/issues/152)에
초기 34-grid의 승인된 다음 값이 없다.

1. `grid_seed_version`의 exact 문자열
2. seed가 효력을 갖는 `effective_dttm`의 exact UTC 시각

fixture의 `weather-grid-v1` 같은 값은 승인값이 아니므로 운영 manifest에 재사용하지 않는다.
두 값을 SSOT에 확정한 뒤 immutable seed bytes를 만들고 `seed:weather_grid` publisher로 최초
게시한다. 이후 동일 topology lock 안에서 weather 13시간과 demand model support가 준비된
station만 활성화한다. 현재 신규·재활성 station은 의도적으로 inactive이며, activation과
그 뒤 전체 source publication chain은 BLOCKED다.

### inference와 model immutable byte contract

[#153 blocker](https://github.com/softeerbootcamp-8th/DE_team2-GangnamguUmBokDong/issues/153#issuecomment-5348088645)에
다음 actual-byte 계약이 없다.

1. `inference_output`이 direct canonical Parquet인지 success manifest인지
2. inference success manifest의 `schema_version`, exact key/order, status, logical time과
   revision, output URI/SHA-256, expected/actual/failed count, model binding
3. rental/return model manifest의 exact schema와 immutable champion bundle URI/SHA-256
4. 각 모델 support ID artifact 또는 digest의 형식과 manifest 결합 방식
5. champion discovery/current pointer, manifest-last, same-logical correction 규칙

확정 뒤 rental/return model producer → immutable bundle/support → inference output
manifest-last → demand publisher → PostGIS replay/stale/correction/EMPTY/rollback → CLI/Airflow
순으로 구현하고 검증한다. 이 전에는 demand production publisher와 이를 전제로 한 station
activation을 열지 않는다.

### urgency history와 scoring config

같은 [#153 blocker](https://github.com/softeerbootcamp-8th/DE_team2-GangnamguUmBokDong/issues/153#issuecomment-5348088645)에
fingerprint bytes를 결정하는 다음 값이 없다.

1. `stock_history_manifest_01` … `05`가 oldest → newest인지 newest → oldest인지
2. `stock_window_count`가 과거 5개만 뜻하는지 현재 `stock_publication_manifest`를 포함한
   6개인지, 그리고 parameter에 기록할 exact 값
3. `scoring_config_version`의 exact 문자열과 그 버전이 가리키는 immutable config bytes

확정 뒤 urgency producer/output manifest와 publisher를 구현하고, same-anchor dependency,
correction invalidation, EMPTY, rollback을 실제 DB에서 검증한다. 그 다음에만 route
producer/publisher, locked route coverage, header/stop 원자 게시와 derived DAG를 연결한다.

### blocker의 결과

위 결정 전에는 다음 항목을 완료로 표시할 수 없다.

- 승인 seed와 weather/demand coverage를 사용한 신규·재활성 station activation
- demand, urgency, route의 production producer/publisher·CLI·Airflow 실행
- publisher/API/route-operator role ACL 검증
- grid/center → station/stock → demand/weather/event → urgency → route → API의 full
  first-publication chain

## 실패 해석과 재개 절차

| 실패 위치 | 해석 | 안전한 재개 방법 |
| --- | --- | --- |
| prerequisite 또는 SSOT blob check | 잘못된 stack tip, dependency 누락, SSOT drift | final commit과 현재 branch를 확인하고 dependency를 전용 worktree에 준비한다. SSOT 차이는 기대값을 낮추지 말고 변경 근거를 먼저 검토한다. |
| container 시작 또는 readiness | Docker daemon, image pull, local resource 문제 | runner가 exact container를 정리했는지 확인한 뒤 같은 명령을 재실행한다. 기존 Compose DB나 RDS로 대체하지 않는다. |
| bootstrap/schema/reapply | 최종 DDL, wrapper 또는 fail-fast 회귀 | 해당 phase 로그와 exit code를 보존하고 빈 disposable DB에서만 재현한다. 재적용의 기대 exit 3을 성공으로 바꾸지 않는다. |
| edge SQL 또는 concurrency | 제약·공간·lock 순서 회귀 | 실패 assertion/session을 확인하고 SSOT와 구현을 수정한 뒤 전체 전용 명령을 처음부터 다시 실행한다. timeout을 무조건 늘려 숨기지 않는다. |
| Core/Collector/Loader | byte contract, source authority, mutation 원자성 또는 projection 회귀 | 출력된 focused pytest node를 먼저 재현하고 수정한 뒤 전체 명령을 재실행한다. 기존 test 삭제·skip/xfail 추가는 금지한다. |
| API | 소비 SQL/freshness/error mapping 회귀 또는 직접 fixture 불일치 | API 단위를 수정하고 같은 final DDL의 새 disposable DB로 재실행한다. 직접 fixture PASS를 publisher E2E로 기록하지 않는다. |
| Airflow | DAG import/dependency/CLI wiring 회귀 | task graph와 allowlist를 수정하되 blocked derived CLI를 가짜로 연결하지 않는다. |
| Web | DTO/state/race/type/build 회귀 | focused Vitest와 build를 수정한 뒤 전체 명령을 재실행한다. mock PASS를 live E2E로 기록하지 않는다. |
| 예상하지 않은 skip/xfail | disposable DB 주입 또는 test discovery가 깨짐 | PASS로 기록하지 말고 runner 환경·node selection을 고친 뒤 재실행한다. |

SSOT 결정이 내려오면 해당 이슈에 exact 값과 문서 commit을 연결한다. 위 순서대로 구현과
격리 통합 테스트를 추가한 후 이 matrix에서 해당 행만 BLOCKED → PARTIAL → PASS로 이동한다.
구조·key·상태·nullable 또는 byte contract를 추정해 blocker를 우회하지 않는다.
