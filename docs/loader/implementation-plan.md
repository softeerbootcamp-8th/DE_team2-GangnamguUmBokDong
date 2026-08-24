# Loader와 Gold publication 구조

> **현재 구현:** `loader/gold/`, `gold_cli.py`, `serving_cli.py`와 Airflow
> `realtime_tick`이 사용하는 적재 경로를 설명한다. 코드 확인일: 2026-08-24.

## 역할

**Loader는 행별 upsert 도구가 아니라, 검증된 immutable 입력을 Gold serving projection으로
원자 게시하는 publisher다.**

원천 snapshot, serving plan, inference 결과와 선행 publication manifest를 URI·SHA-256으로
고정하고 다음을 한 transaction에서 처리한다.

1. 입력 artifact와 dependency 재검증
2. Publication key별 advisory lock 획득
3. Staging 결과 검증
4. Gold target projection 변경
5. `gold_meta.publication_state` 전진
6. Commit 뒤 immutable publication manifest 공개

Canonical byte와 manifest 규칙은
[Gold publication contract](../gold/publication-contract-v1.md), 물리 테이블은
[target-schema.sql](../gold/target-schema.sql)을 기준으로 한다.

## 현재 진입점

| 진입점 | 용도 | 현재 상태 |
| --- | --- | --- |
| `gold_cli.py` | Seed와 독립 event source publication | 운영 경로 |
| `serving_cli.py` | Realtime serving chain의 prepare/finalize/urgency/route | 운영 경로 |
| `local_e2e.py` | Local fixture 기반 smoke·검증 | 개발 전용 |
| `main.py`, `tables.yaml`, `transform.py` | 과거 table별 Silver→DB upsert | Legacy 호환 경로, 현재 Airflow Gold publication에서 미사용 |

과거 문서의 `stations`, `weather_current`, `cultural_events`, `forecast_points` DDL은 현행
Gold schema가 아니다. 현재 target은 단수형 `station`, 통합 `event`, resolver 결과인
`weather_forecast`, `station_demand_forecast`를 사용하며 `weather_current`는 Gold에 두지
않는다.

## Publication 흐름

### Seed와 event

`gold_cli.py`가 지원하는 publication은 다음 네 개다.

| CLI 값 | publication key | 입력 |
| --- | --- | --- |
| `seed:dispatch_center` | `dispatch_center` | `docs/gold/dispatch-center-seed.yaml` |
| `seed:weather_grid` | `weather_grid` | 단기·초단기 forecast source YAML의 동일한 34개 grid |
| `event:cultural_event` | `event:cultural_event` | Exact cultural source snapshot |
| `event:performance_event` | `event:performance_event` | Exact performance snapshot와 stadium coordinate asset |

`station-master-correction`, `station-release`, `weather-forecast` 값은 parser 호환을 위해
남아 있지만 실행 시 실패한다. 이 standalone authority는 retired됐으며 coordinated serving
chain을 사용해야 한다.

### Realtime serving chain

```text
bike/weather source publication
        │
        ▼
serving_cli.py prepare ──→ immutable serving plan
        │
        ▼
ML inference ────────────→ immutable inference manifest
        │
        ▼
serving_cli.py finalize
        ├── station
        ├── station_stock
        ├── station_demand_forecast
        └── weather_forecast
                │
                ▼
serving_cli.py urgency ──→ station_urgency
                │
                ▼
serving_cli.py route ────→ rebalance_route + stop
```

Airflow `realtime_tick`은 이 순서를 task dependency로 강제한다. 각 task는 이전 task의
XCom에서 전체 payload가 아닌 manifest `uri`와 `byte_sha256`만 전달받는다.

## 단계별 책임

### 1. Prepare

`serving_cli.py prepare --logical-dttm ...`는 다음 입력을 고정한 immutable serving plan을
만든다.

- Exact realtime station snapshot
- Lookback 안의 최신 station master
- 최신 단기·초단기 forecast snapshot
- 현재 rental/return model pair와 support ID set
- 최신 enriched station master에서 계산한 inference 가능 station ID
- 선택적 relocation approval URI·SHA 쌍
- 기존 Gold station state와 realtime window set

Master/realtime lookback은 각각 `GOLD_STATION_MASTER_LOOKBACK_HOURS`,
`GOLD_STATION_REALTIME_LOOKBACK_HOURS`의 양의 정수 시간으로 받는다.

### 2. Finalize

`serving_cli.py finalize`는 plan과 같은 logical time의 inference manifest를 exact-read하고
다음 네 publication을 coordinated transaction으로 게시한다.

- `station`
- `station_stock`
- `station_demand_forecast`
- `weather_forecast`

결과 evidence key가 이 네 개와 정확히 같지 않으면 실패한다. STALE 결과도 성공으로
취급하지 않고 후속 urgency chain을 중단한다.

### 3. Urgency

`serving_cli.py urgency`는 finalize가 반환한 station·demand·stock manifest의 logical time이
모두 같은지 검증한다. 현재 stock과 `t-25`, `t-20`, `t-15`, `t-10`, `t-5분`의 사용 가능한
realtime source window로 긴급도를 게시한다.

지나간 window가 실제로 없으면 CLI가 누락 offset을 기록할 수 있지만, Publisher가 catalog를
다시 확인하고 최소 window 계약을 검증한다. 누락을 임의로 숨겨서는 통과하지 않는다.

### 4. Route

`serving_cli.py route`는 exact urgency manifest를 입력으로 proposed route header와 stop을
한 transaction에서 교체한다. Route publisher는 urgency 내부 station·demand·stock
dependency가 현재 route 입력과 같은지 다시 확인하며, STALE이면 실패한다.

## 주요 구현 모듈

| 모듈 | 책임 |
| --- | --- |
| `gold/source_catalog.py` | S3 source manifest 탐색과 exact authority 선택 |
| `gold/source_policy.py` | Snapshot completeness와 source 정책 |
| `gold/serving_plan.py` | Prepare artifact와 coordinated 4-key finalize |
| `gold/station_release.py` | Station·stock lifecycle와 원자 게시 |
| `gold/demand.py` | Inference 결과를 demand projection으로 게시 |
| `gold/weather_forecast.py` | 단기·초단기 resolver와 13시간 buffer 게시 |
| `gold/event.py` | Source별 event identity·reconcile |
| `gold/urgency.py` | Stock history와 demand 기반 urgency 게시 |
| `gold/rebalance_route.py` | Route/stop 산출과 coverage 검증 |
| `gold/dispatch_center.py`, `gold/weather_grid.py` | Versioned seed publication |
| `gold/state.py`, `gold/versioning.py` | Dependency state와 publication version 처리 |

공통 canonicalization, immutable storage, evidence와 DB transaction은 Loader 내부에서
재구현하지 않고 `libs/core/src/core/gold_publication/`을 사용한다.

## 실행 계약

### Source publication 예시

```bash
cd loader
uv run --frozen python gold_cli.py \
  --publication event:cultural_event \
  --window-start 2026-08-24T09:00:00+09:00
```

### Serving prepare 예시

```bash
cd loader
uv run --frozen python serving_cli.py prepare \
  --logical-dttm 2026-08-24T09:00:00+09:00
```

두 CLI 모두 timezone offset이 있는 ISO 8601 시각을 요구한다. `S3_BUCKET`은 필수이며
공백·slash가 없는 bucket name이어야 한다. 선택적 `S3_ENDPOINT_URL`은 local object store에
사용한다. DB 연결은 `core.db.get_connection()`의 환경 계약을 따른다.

`serving_cli.py`의 stdout 마지막 한 줄은 Airflow XCom이 읽는 compact JSON ref다. 일반 로그와
결측 경고는 stderr로 보내므로 stdout 형식을 바꾸면 orchestration 계약도 함께 깨진다.

## 실패와 재실행

- Exact same version·fingerprint는 no-op이다.
- 더 오래된 logical time 또는 revision은 STALE이다.
- 같은 version인데 fingerprint가 다르면 계약 위반이다.
- 같은 logical time의 correction은 더 큰 `revision_no`가 필요하다.
- Input URI와 SHA가 다르거나 다른 bucket을 가리키면 fail-closed한다.
- Finalize와 urgency가 STALE이면 후속 task를 실행하지 않는다.
- Target mutation 중 실패하면 `publication_state`도 함께 rollback돼야 한다.

따라서 실패 후 target에 직접 upsert하거나 state를 수동 전진시키지 않는다. 입력 authority와
version을 바로잡은 뒤 동일 CLI 경계에서 재실행한다.

## 검증

```bash
# Loader 전체
UV_CACHE_DIR=/private/tmp/codex-uv-cache \
uv run --project loader --frozen pytest loader/tests -q

# CLI와 순수 Gold 로직
UV_CACHE_DIR=/private/tmp/codex-uv-cache \
uv run --project loader --frozen pytest \
  loader/tests/test_gold_cli.py \
  loader/tests/test_serving_cli.py \
  loader/tests/gold -q
```

`GOLD_PUBLICATION_TEST_DATABASE_URL`이 없어서 PostGIS integration test가 skip되면 DB
publication PASS로 기록하지 않는다. 전체 격리 검증 방법은
[Gold 통합 검증 가이드](../gold/integration-validation.md)를 따른다.
