# Gold 통합 검증 가이드

> **현재 상태:** 실제 검증 코드와 실행 경계를 설명한다. 과거 실행 건수는 현재 PASS를
> 의미하지 않는다. 코드 확인일: 2026-08-24.

## 검증 목표

**Gold 검증은 “테이블에 행이 들어갔다”가 아니라 입력 근거부터 publication state까지
같이 전진했음을 증명해야 한다.**

검증 범위는 다음 네 단계로 나뉜다.

1. PostGIS DDL과 제약 조건
2. Canonical artifact·fingerprint·manifest 계약
3. Publisher의 target/state 원자성
4. API·Airflow·Web 소비 경계

각 단계의 테스트가 통과해도 실제 AWS credential, Airflow scheduler와 browser를 모두 연결한
운영 E2E가 자동으로 증명되는 것은 아니다.

## 현재 검증 수준

| 영역 | 수준 | 실제 검증 내용 | 포함하지 않는 범위 |
| --- | --- | --- | --- |
| DDL | 통합 | 빈 PostGIS 적용, schema checker, 재적용 실패, 제약·공간·index | 기존 운영 DB migration |
| 동시성 | 통합 | publication, topology, route advisory lock의 두 session 경쟁 | 장시간 운영 부하 |
| Publication core | 단위·통합 | canonical bytes, immutable object, replay, stale, correction, EMPTY, rollback | 실제 S3·RDS 권한 |
| Source/derived publisher | 통합 | artifact에서 실제 PostGIS target와 `publication_state` 게시 | 운영 API 응답과 scheduler 전체 실행 |
| API | 단위·통합 | Gold query, freshness, 좌표·거리, HTTP 오류와 route 상태 | Publisher가 채운 DB를 읽는 live chain |
| Airflow | 구조 | DAG import, task dependency, publisher CLI wiring | Scheduler task 실행 |
| Web | 단위·build | API 상태 처리, polling race, production build | 실제 API를 연결한 browser E2E |
| 서비스 권한 | 미완료 | `PUBLIC` 권한 회수 | Publisher/API/operator별 role과 GRANT matrix |

## 검증 진입점

### 반복 개발 검증

수정한 계층의 project environment에서 focused test를 먼저 실행한다.

```bash
# Canonical publication 계약
UV_CACHE_DIR=/private/tmp/codex-uv-cache \
uv run --project libs/core --frozen pytest \
  libs/core/tests/test_gold_publication_*.py -q

# Gold publisher 로직
UV_CACHE_DIR=/private/tmp/codex-uv-cache \
uv run --project loader --frozen pytest \
  loader/tests/gold loader/tests/test_gold_*.py -q

# API 소비 계약
UV_CACHE_DIR=/private/tmp/codex-uv-cache \
uv run --project apps/api --frozen pytest \
  apps/api/tests/test_main.py apps/api/tests/test_queries.py -q

# Airflow 연결 계약
UV_CACHE_DIR=/private/tmp/codex-uv-cache \
uv run --project airflow --frozen pytest \
  airflow/tests/test_dag_imports.py airflow/tests/test_task_builders.py -q
```

DB URL이 필요한 테스트의 skip은 “통과”가 아니다. Publisher/PostGIS 원자성을 주장하려면
아래 격리 통합 runner 또는 동등한 disposable DB 환경에서 skip 없이 실행해야 한다.

### 격리 전환 검증 runner

```bash
make test-gold-transition-available
```

이 target은 `ops/gold/tests/run_transition_validation.sh`를 호출한다. Runner는 다음 안전장치를
구현한다.

- `postgis/postgis:16-3.4` 일회성 container 사용
- host port 동적 할당과 PGDATA tmpfs 사용
- bind mount와 named volume 금지
- 호출자 DB 환경변수 제거
- schema, edge SQL, concurrency, package test를 순서대로 실행
- pytest skip/xfail/xpass를 실패 처리
- 종료 시 자신이 만든 container만 제거
- 실행 전후 tracked worktree 상태와 `git diff --check` 비교

#### 현재 runner 제한

Runner는 다음 네 SSOT 파일이 commit
`eadf79f925eb64386d009af71fe36854d9e56dc5`와 byte-for-byte 같아야 실행된다.

- `target-schema.sql`
- `data-dictionary.md`
- `source-target-mapping.md`
- `publication-contract-v1.md`

현재 문서 개선 작업은 이 고정 commit 이후의 변경이므로, 현 worktree에서 위 명령은
`verify_ssot` 단계에서 실패하는 것이 정상이다. 새 기준 commit을 확정하고 runner의
`SSOT_COMMIT`을 갱신하기 전까지 이 명령을 현재 통합 PASS 근거로 기록하지 않는다.

## 격리 runner가 확인하는 순서

1. 필수 명령, Web dependency, Docker daemon과 SSOT bytes를 확인한다.
2. PostgreSQL bootstrap fail-closed shell test를 실행한다.
3. 빈 tmpfs의 PostGIS 16-3.4 container를 시작한다.
4. `target-schema.sql` 적용 후 read-only schema checker를 실행한다.
5. `target-schema-validation.sql`과 edge SQL을 실행한다.
6. Schema 재적용이 정확히 exit code 3으로 실패하는지 확인한다.
7. 실제 두 DB session으로 advisory-lock 직렬화를 검증한다.
8. Core, Collector, ML, Inference, Training 테스트를 실행한다.
9. Loader DB를 재생성하며 source·derived publisher 테스트를 실행한다.
10. API PostGIS, Airflow, Web test와 Web production build를 실행한다.
11. Worktree가 실행 전 상태와 같은지 확인하고 container를 제거한다.

중간 단계가 실패하면 뒤 단계만 따로 성공해도 전체 PASS가 아니다.

## DDL과 DB 검증 파일

| 파일 | 책임 |
| --- | --- |
| `target-schema.sql` | Gold/PostGIS baseline DDL |
| `target-schema-validation.sql` | 테이블·제약·함수·기본 동작 검증 |
| `target-schema-concurrency-validation.sh` | 두 session lock 경쟁 검증 |
| `ops/gold/tests/target_edge_validation.sql` | nonfinite, 미래 시각, Point, route 전이, GiST edge case |
| `ops/postgres/check_gold_schema.sh` | 서비스 시작 전 read-only schema 검사 |

Baseline SQL은 기존 DB를 migration하는 파일이 아니다. 이미 적용된 DB에 다시 실행하면
exit code 3으로 실패하도록 설계돼 있다.

## 실행 안전 수칙

### 실행 전

- 운영·공유 DB를 가리키는 `DATABASE_URL`, `PG*` 환경에서 실행하지 않는다.
- Docker daemon과 `postgis/postgis:16-3.4` image를 사용할 수 있어야 한다.
- `apps/web/node_modules/.bin`에 `vitest`, `tsc`, `vite`가 설치돼 있어야 한다.
- 현재 tracked 변경을 `git status --short`로 기록한다.
- SSOT 고정값을 갱신할 때는 검토 완료된 새 commit SHA만 사용한다.

### 실행 중

- Runner가 만든 container와 tmpfs DB만 사용한다.
- 기존 Compose PostgreSQL이나 RDS로 실패를 우회하지 않는다.
- `docker compose down -v`, volume prune 같은 파괴적 명령을 실행하지 않는다.
- 실패 테스트를 skip/xfail로 바꾸거나 기대 exit code를 완화하지 않는다.

### 실행 후

- Runner 소유 container가 제거됐는지 확인한다.
- 시작 전후 tracked status가 같은지 비교한다.
- 로그에는 phase, 종료 코드와 소요 시간만 남기고 credential과 DSN은 남기지 않는다.

## E2E 증거로 인정하지 않는 경우

다음 결과는 각 계층의 유효한 테스트지만 전체 publication E2E는 아니다.

- SQL fixture를 target와 `gold_meta`에 직접 INSERT한 DDL 테스트
- Target fixture를 직접 넣고 읽는 API PostGIS 테스트
- DB helper를 monkeypatch한 API 단위 테스트
- API를 mock한 Web 테스트
- DAG import와 Bash command 문자열만 확인한 Airflow 테스트
- 장애를 monkeypatch로 주입한 publisher rollback 테스트

반대로 immutable input artifact와 manifest를 읽어 실제 disposable PostGIS의 target와
`publication_state`를 함께 바꾸는 Loader 테스트는 해당 publication 경계의 test-level
E2E다. 다만 실제 Collector, S3 credential, scheduler, API까지 한 번에 연결하지는 않는다.

## 아직 완료되지 않은 운영 검증

### 서비스별 최소 권한

현재 DDL은 `gold_meta` schema, `publication_state`, claim/lock function의 `PUBLIC` 권한을
회수한다. 그러나 다음 계약은 구현돼 있지 않다.

- Publisher, API reader, route operator role 이름과 credential 분리
- Publication key·target별 최소 DML 권한
- `gold_meta.claim_publication()`과 lock function의 EXECUTE 권한
- API read와 route lifecycle update 권한 분리

따라서 서비스별 ACL 검증은 아직 완료로 표시할 수 없다.

### 운영 seed와 live chain

Repository에는 dispatch center seed와 34개 weather grid를 만드는 검증 로직이 있고,
`make bootstrap-gold-seeds`가 publisher CLI를 호출한다. 다만 운영에서는 승인된
`GOLD_WEATHER_GRID_SEED_VERSION`과 `GOLD_WEATHER_GRID_EFFECTIVE_DTTM`을 명시해야 한다.
Makefile의 local 기본값은 운영 승인값이 아니다.

다음 전체 chain은 실제 운영 환경에서 별도로 검증해야 한다.

```text
seed → source snapshot → station/stock → demand/weather/event
     → urgency → route → API → Web
```

## PASS 기록 기준

검증 결과에는 다음을 함께 남긴다.

- 검증한 commit SHA와 dirty 여부
- 실행한 명령과 각 phase 종료 코드
- PostGIS image tag
- pytest의 passed/skipped/xfail 수
- Web test와 build 결과
- container cleanup 및 tracked worktree 비교 결과
- fixture 기반, test-level E2E, live E2E 중 어느 수준인지

과거 실행 건수나 PR 상태를 현재 PASS 근거로 재사용하지 않는다.
