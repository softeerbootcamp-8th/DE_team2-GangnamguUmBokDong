# Gold publication byte contract v1

> **현재 계약:** `libs/core/src/core/gold_publication/`과 Gold publisher가 사용하는 canonical byte·manifest 계약이다. 코드 확인일: 2026-08-24.

## 왜 필요한가

**같은 입력은 언제 실행해도 같은 fingerprint와 manifest를 만들어야 한다.**

Gold publication은 원천 artifact, 선행 publication, 계산 파라미터와 출력 artifact를
immutable URI와 SHA-256으로 고정한다. Publisher는 DB lock 안에서 이 근거를 다시 검증한
뒤 target table과 `publication_state`를 한 transaction으로 전진시킨다.

이 문서의 필드, 정렬 또는 의미를 바꾸려면 schema version과 publisher version을 함께
올려야 한다.

## 공통 byte 규칙

- JSON은 UTF-8 RFC 8785/JCS canonical bytes로 직렬화한다.
- 문자열은 Unicode NFC, 시각은 UTC `YYYY-MM-DDTHH:MM:SS.ffffffZ` 형식이다.
- SHA-256과 EWKB는 lowercase hex다.
- JSON 값은 object, array, string, safe integer, boolean, `null`만 허용한다. float는
  허용하지 않는다.
- object key는 JCS 순서로 정렬한다. 계약이 지정한 배열 순서와 exact key 집합도
  검증하며, 중복 key와 중복 정렬 tuple은 거부한다.
- Point는 SRID 4326, 2D, big-endian(XDR) EWKB로 표현한다.

구현: `canonical.py`

## 출력 artifact set

`gold-artifact-set-v1`의 root key는 정확히 다음 두 개다.

| key | 의미 |
| --- | --- |
| `schema_version` | `gold-artifact-set-v1` |
| `artifacts` | 출력 artifact 배열 |

각 artifact는 `byte_sha256`, `role`, `row_count`, `uri`만 가지며 `(role, uri)`의
UTF-8 byte 오름차순이다. `artifact_set_sha256`은 이 문서의 canonical bytes를 해시한
값이다. 정상 EMPTY의 고정 hash는 다음과 같다.

```text
98f11969010a550c3b20fd37879e45ec1682b3b05d4c7a25e590a7f0874a4cdb
```

## 입력 fingerprint

`gold-input-fingerprint-v1`은 계산 결과를 결정하는 모든 입력을 고정한다.

| 배열 | 원소의 exact key | 정렬 기준 |
| --- | --- | --- |
| `dependencies` | `artifact_set_sha256`, `input_fingerprint_sha256`, `logical_dttm`, `manifest_uri`, `publication_key`, `revision_no` | `publication_key` |
| `input_artifacts` | `byte_sha256`, `role`, `uri` | `(role, uri)` |
| `parameters` | `name`, `value` | `name` |

Root key는 `schema_version`, `dependencies`, `input_artifacts`, `parameters`만
허용한다. Dependency는 target lock 안에서 현재 `publication_state`와 다시 비교하며,
manifest input role은 실제 bytes의 hash와 연결된 dependency 6-tuple까지 검증한다.

Station ID 집합은 임의 문자열을 해시하지 않는다. `gold-id-set-v1`의 `ids`를 NFC·
nonblank·UTF-8 byte 오름차순으로 정렬하고 중복을 제거한 canonical 문서의 SHA-256을
`expected_sta_id_sha256`으로 사용한다.

## Publication registry

아래 집합은 `contract.py`의 `PUBLICATION_REGISTRY`와 정확히 같아야 한다. 표에 없는
dependency, role, parameter는 허용하지 않으며, 별도 표시가 없는 role은 정확히 1개다.

| publication key | dependencies | input artifact role | parameter |
| --- | --- | --- | --- |
| `weather_grid` | 없음 | `weather_grid_seed` | `expected_grid_count`, `grid_seed_version` |
| `dispatch_center` | 없음 | `dispatch_center_seed` | `center_seed_version`, `expected_center_count` |
| `station` | `dispatch_center`, `weather_grid` | `bike_station_master_manifest`, `station_realtime_window_set`, 조건부 `station_previous_projection`, 조건부 `station_relocation_approval` | `center_assignment_version`, `grid_conversion_version`, `station_policy_version` |
| `station_stock` | 없음 | `bike_station_realtime_manifest` | `station_stock_policy_version` |
| `station_demand_forecast` | `station` | `inference_output`, `rental_model_manifest`, `return_model_manifest` | `expected_sta_id_sha256`, `horizon_count`, `rounding_mode` |
| `weather_forecast` | `station`, `weather_grid` | `short_term_manifest`, `ultra_short_manifest` | `forecast_hour_count`, `resolver_version` |
| `event:cultural_event` | 없음 | `cultural_event_manifest` | `event_identity_version`, `event_policy_version` |
| `event:performance_event` | 없음 | `performance_event_manifest`, `stadium_coordinate_seed` | `event_policy_version`, `stadium_coordinate_version` |
| `station_urgency` | `station`, `station_demand_forecast`, `station_stock` | `demand_publication_manifest`, `stock_publication_manifest`, 선택적 `stock_history_manifest_m05`~`m25`, `urgency_output` | `expected_sta_id_sha256`, `rebalance_policy_config`, `scoring_config_version`, `stock_history_offsets`, `stock_window_count` |
| `rebalance_route` | `dispatch_center`, `station`, `station_demand_forecast`, `station_stock`, `station_urgency` | `pickup_cooldown_station_ids`, `route_coverage`, `urgency_publication_manifest` | `max_routes_per_center`, `max_stops_per_route`, `rebalance_policy_config`, `route_algorithm_version`, `route_coverage_sha256`, `route_work_unit_config_version`, `truck_capacity`, `truck_capacity_config_version` |

### 조건부 입력

- `station_previous_projection`: 기존 station state가 있으면 1개, 없으면 0개다.
- `station_relocation_approval`: 100m 초과 Point 변경을 실제 반영할 때만 1개다.
- `stock_history_manifest_m05`~`m25`: 각 offset별 0개 또는 1개다. 현재 stock을 포함해
  urgency 계산에 필요한 최소 window 수는 publisher가 검증한다.
- `stock_history_offsets`는 사용한 과거 offset을 oldest-first로 기록한다.
- `pickup_cooldown_station_ids`는 비어 있어도 생략하지 않는 canonical
  `gold-id-set-v1` artifact다. 최근 pickup 때문에 이번 route 후보에서 제외한 station ID를
  기록한다.
- `station_stock`은 dependency 없이 `station`과 같은 release의 realtime manifest를
  사용한다. 해당 manifest는 station window set의 첫 candidate와 같아야 한다.

### Manifest와 dependency 결합

- `demand_publication_manifest` → `station_demand_forecast`
- `stock_publication_manifest` → `station_stock`
- `urgency_publication_manifest` → `station_urgency`

각 input manifest의 URI, 실제 byte hash, publication key, logical time, revision,
artifact-set hash와 input-fingerprint hash가 dependency와 모두 같아야 한다. Route는 urgency
fingerprint 내부의 `station`, demand, stock dependency도 자신의 dependency와 다시
비교한다. 또한 두 fingerprint의 `rebalance_policy_config`가 byte-for-byte 같아야 한다.
`route-v3-supply-led`는 현행 `scoring_config_version`과 기본 재배치 정책의 canonical
config만 소비하며, 구버전 점수 또는 다른 정책 fingerprint는 fail-closed로 거부한다.
가장 긴급한 supply가 경로 ordinal과 첫 dropoff를 소유하고, pickup 후보는
`center→pickup→supply` 총거리 순으로 고른다.

## Station 보조 문서

### Realtime window set

`gold-station-realtime-window-set-v1`은 `schema_version`, `windows`만 가진다. Window는
`byte_sha256`, `logical_dttm`, `revision_no`, `uri`로 구성되며 다음 규칙을 따른다.

- candidate를 포함해 1~3개다.
- 서로 다른 logical window를 `logical_dttm` 내림차순으로 정렬한다.
- publisher가 선택한 candidate가 첫 원소여야 한다.
- 같은 logical window에는 authoritative correction revision 하나만 사용한다.

### Relocation approval

`gold-station-relocation-approval-v1`은 `schema_version`, `approvals`만 가진다. Approval의
exact key는 `approval_id`, `approved_by`, `approved_dttm`, `candidate_point_ewkb`,
`comparison_cd`, `reference_point_ewkb`, `sta_id`다.

`comparison_cd`는 `gold_vs_master` 또는 `master_vs_realtime`이며 approvals는
`(sta_id, comparison_cd, approval_id)` 순이다. 같은 station·comparison 후보에는 승인이
정확히 하나만 존재한다.

## Publication manifest

`gold-publication-manifest-v1`은 정확히 다음 13개 key를 가진다.

```text
artifact_set_sha256, artifacts, input_fingerprint_schema,
input_fingerprint_sha256, input_fingerprint_uri, logical_dttm,
publication_key, published_row_cnt, publisher_version, revision_no,
schema_version, target_row_counts, target_schema_version
```

- `input_fingerprint_schema`: `gold-input-fingerprint-v1`
- `schema_version`: `gold-publication-manifest-v1`
- `target_schema_version`: `gold-postgis-v1`
- `artifacts`: artifact-set과 동일한 배열
- `target_row_counts`: 이번 publication이 소유해 게시한 projection의 테이블별 행 수
- `published_row_cnt`: registry의 대표 target 행 수. Route는 header 수

정상 EMPTY는 `artifacts=[]`, 고정 EMPTY artifact-set hash, 모든 target count와
`published_row_cnt=0`을 사용한다. EMPTY 허용 정책도 registry에서 검증한다.

| publication key | output role → target | EMPTY 정책 |
| --- | --- | --- |
| `weather_grid` | `weather_grid` → `weather_grid` | 금지 |
| `dispatch_center` | `dispatch_center` → `dispatch_center` | 금지 |
| `station` | `station` → `station` | 금지 |
| `station_stock` | `station_stock` → `station_stock` | 금지 |
| `station_demand_forecast` | `station_demand_forecast` → `station_demand_forecast` | 조건부 |
| `weather_forecast` | `weather_forecast` → `weather_forecast` | 조건부 |
| `event:cultural_event` | `event_cultural_event` → `event` | 허용 |
| `event:performance_event` | `event_performance_event` → `event` | 허용 |
| `station_urgency` | `station_urgency` → `station_urgency` | 조건부 |
| `rebalance_route` | `routes` → `rebalance_route`, `route_stops` → `rebalance_route_stop` | 허용 |

## Route 재현 계약

### Coverage

`gold-route-coverage-v1`의 root key는 `schema_version`, `stock_anchor_dttm`, `routes`다.
Stock anchor에 아직 반영되지 않은 `dispatched` route와 anchor 뒤 완료된 `completed`
route만 포함한다.

- Route exact key: `completed_dttm`, `dispatched_dttm`, `route_id`, `status`, `stops`
- Stop exact key: `action`, `bike_cnt`, `sta_id`, `visit_no`
- Route는 `route_id` 순, stop은 중복 없이 연속된 `visit_no=1..N` 순이다.
- Action은 `pickup` 또는 `dropoff`이며 `bike_cnt`는 양수다.
- `route_coverage_sha256`은 `route_coverage` artifact의 실제 canonical byte hash와 같다.

### UUIDv5

Route ID는 고정 namespace `d0d59897-9e72-541f-bb05-bd3d113c2639`와 다음 canonical
JSON name으로 생성한다.

```json
{"dispatch_center_id":"center_a","logical_dttm":"2026-08-19T16:00:00.000000Z","publication_key":"rebalance_route","revision_no":0,"route_ordinal":1}
```

`route_ordinal`은 center 안에서 1부터 시작한다. 위 회귀값은
`7dd58c8d-7dc7-5279-8845-7673c9c87be2`다.

## 구현과 검증 위치

| 책임 | 코드 |
| --- | --- |
| canonical JSON·SHA·UTC·EWKB | `canonical.py` |
| registry·artifact·fingerprint·manifest | `contract.py` |
| station/route 보조 문서·UUIDv5 | `documents.py` |
| immutable object 검증 | `evidence.py`, `storage.py` |
| DB publication transaction | `transaction.py` |

회귀 테스트는 `libs/core/tests/test_gold_publication_*.py`에 있다.
