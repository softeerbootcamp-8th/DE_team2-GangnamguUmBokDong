# Gold publication byte contract v1

## 목적

이 문서는 producer와 publisher가 같은 publication manifest, artifact 집합,
입력 fingerprint, route coverage와 UUIDv5를 만드는 바이트 단위 계약이다. 아래 이름과
문서 모양은 `v1`에서 고정이다. 필드를 추가·삭제·개명하거나 정렬 규칙을 바꾸면 새 schema
version과 publisher version을 함께 올린다.

## 공통 직렬화

- UTF-8과 RFC 8785 JSON Canonicalization Scheme을 사용한다.
- 문자열은 Unicode NFC다. 시각은 UTC `YYYY-MM-DDTHH:MM:SS.ffffffZ`다.
- SHA-256과 EWKB는 lowercase hex다.
- canonical 문서에는 float를 넣지 않는다. 수량·revision·row count는 JSON integer,
  설정값은 명시된 string, 상태값은 string, 부재는 `null`이다.
- object key는 RFC 8785 순서, 아래 배열은 지정한 tuple 순으로 정렬한다. 중복 key와
  중복 정렬 tuple은 거부한다.
- Point가 필요한 문서는
  `lower(encode(ST_AsEWKB(ST_Force2D(point), 'XDR'), 'hex'))`를 쓴다. 회귀 벡터는
  `POINT(127.0 37.5), SRID 4326 →
  0020000001000010e6405fc000000000004042c00000000000`이다.

## artifact set

`gold-artifact-set-v1` 문서는 정확히 `schema_version`, `artifacts` 두 key다. 각 artifact는
정확히 `byte_sha256`, `role`, `row_count`, `uri`를 가지며 `(role, uri)` 오름차순이다.
URI는 immutable object를 가리키고 publisher가 읽은 실제 bytes의 SHA-256을 사용한다.
`artifact_set_sha256`은 이 전체 canonical JSON bytes의 SHA-256이다.

```json
{"artifacts":[{"byte_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","role":"route_stops","row_count":1,"uri":"s3://fixture/route-stops.parquet"},{"byte_sha256":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","role":"routes","row_count":1,"uri":"s3://fixture/routes.parquet"}],"schema_version":"gold-artifact-set-v1"}
```

위 bytes의 SHA-256은
`576eec2c53f1be8985ce531f512f4f4014fe05879d1f53714128dd774d8abf87`이다. 정상 EMPTY는
`{"artifacts":[],"schema_version":"gold-artifact-set-v1"}`이고 SHA-256은
`98f11969010a550c3b20fd37879e45ec1682b3b05d4c7a25e590a7f0874a4cdb`다.

## input fingerprint

`gold-input-fingerprint-v1` 문서는 정확히 `schema_version`, `dependencies`,
`input_artifacts`, `parameters` 네 key다.

- `dependencies`: Gold 선행 publication state. 각 원소는 정확히
  `artifact_set_sha256`, `input_fingerprint_sha256`, `logical_dttm`, `manifest_uri`,
  `publication_key`, `revision_no`를 가지며 `publication_key` 오름차순이다. publisher는
  target lock 안에서 현재 state tuple과 다시 비교한다. immutable `manifest_uri`까지
  남기므로 dependency가 최신 state로 전진한 뒤에도 당시 input projection을 복원한다.
- `input_artifacts`: upstream manifest·seed·원천 객체. 각 원소는 정확히
  `byte_sha256`, `role`, `uri`를 가지며 `(role, uri)` 오름차순이다.
- `parameters`: 계산 결과를 바꾸는 model/config/policy 값. 각 원소는 정확히 `name`,
  `value` string을 가지며 `name` 오름차순이다. 집합은 정렬한 ID의 별도 artifact SHA나
  lowercase SHA-256 string으로 표현하고 JSON array를 value string에 숨기지 않는다.

```json
{"dependencies":[{"artifact_set_sha256":"1111111111111111111111111111111111111111111111111111111111111111","input_fingerprint_sha256":"2222222222222222222222222222222222222222222222222222222222222222","logical_dttm":"2026-08-19T15:55:00.000000Z","manifest_uri":"s3://fixture/dispatch-center-publication.json","publication_key":"dispatch_center","revision_no":0},{"artifact_set_sha256":"3333333333333333333333333333333333333333333333333333333333333333","input_fingerprint_sha256":"4444444444444444444444444444444444444444444444444444444444444444","logical_dttm":"2026-08-19T15:55:00.000000Z","manifest_uri":"s3://fixture/station-publication.json","publication_key":"station","revision_no":0},{"artifact_set_sha256":"7777777777777777777777777777777777777777777777777777777777777777","input_fingerprint_sha256":"8888888888888888888888888888888888888888888888888888888888888888","logical_dttm":"2026-08-19T16:00:00.000000Z","manifest_uri":"s3://fixture/demand-publication.json","publication_key":"station_demand_forecast","revision_no":0},{"artifact_set_sha256":"9999999999999999999999999999999999999999999999999999999999999999","input_fingerprint_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","logical_dttm":"2026-08-19T16:00:00.000000Z","manifest_uri":"s3://fixture/stock-publication.json","publication_key":"station_stock","revision_no":0},{"artifact_set_sha256":"5555555555555555555555555555555555555555555555555555555555555555","input_fingerprint_sha256":"6666666666666666666666666666666666666666666666666666666666666666","logical_dttm":"2026-08-19T16:00:00.000000Z","manifest_uri":"s3://fixture/urgency-publication.json","publication_key":"station_urgency","revision_no":0}],"input_artifacts":[{"byte_sha256":"13cd1f4fe82d4b09370fd4141d1ee1a727f25c5b109de11f06bb904f9c001e8b","role":"route_coverage","uri":"s3://fixture/route-coverage.json"},{"byte_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","role":"urgency_publication_manifest","uri":"s3://fixture/urgency-publication.json"}],"parameters":[{"name":"max_routes_per_center","value":"3"},{"name":"max_stops_per_route","value":"8"},{"name":"route_algorithm_version","value":"route-v2"},{"name":"route_coverage_sha256","value":"13cd1f4fe82d4b09370fd4141d1ee1a727f25c5b109de11f06bb904f9c001e8b"},{"name":"route_work_unit_config_version","value":"route-work-unit-v1"},{"name":"truck_capacity","value":"20"},{"name":"truck_capacity_config_version","value":"truck-capacity-v1"}],"schema_version":"gold-input-fingerprint-v1"}
```

위 bytes의 `input_fingerprint_sha256`은
`0de0cd4437f089bec16b778cf927c1fef732cd349dca7533a465100b94c5454e`다. 이 예시는
`rebalance_route`의 exact dependency·role·parameter 집합을 사용한다.

`expected_sta_id_sha256`은 임의 join/newline 문자열의 hash가 아니다.
`gold-id-set-v1` 문서의 root는 정확히 `schema_version`, `ids` 두 key이며, `ids`는 NFC·
nonblank ID를 UTF-8 byte 오름차순으로 정렬한 중복 없는 배열이다.

```json
{"ids":["ST-1","ST-2"],"schema_version":"gold-id-set-v1"}
```

위 bytes의 SHA-256은
`a080d2f47ea7c4d0f5d27704264ed23d5a93ec525dd12544812f81b3519fa52f`다. demand와 urgency
publisher는 lock 안의 기대 집합으로 이 문서를 각각 다시 만들어 parameter와 비교한다.

### publication key별 필수 입력

표에 없는 의존·role·parameter를 몰래 추가하지 않는다. 구현에서 입력이 늘면 contract
version을 올린다. `manifest` role은 해당 upstream manifest의 실제 bytes다.

| publication key | dependencies | input artifact role | parameter name |
| --- | --- | --- | --- |
| `weather_grid` | 없음 | `weather_grid_seed` | `expected_grid_count`, `grid_seed_version` |
| `dispatch_center` | 없음 | `dispatch_center_seed` | `center_seed_version`, `expected_center_count` |
| `station` | `dispatch_center`, `weather_grid` | `bike_station_master_manifest`, `station_realtime_window_set`, 조건부 `station_previous_projection`, 조건부 `station_relocation_approval` | `center_assignment_version`, `grid_conversion_version`, `station_policy_version` |
| `station_stock` | 없음; `station`과 같은 release | `bike_station_realtime_manifest` | `station_stock_policy_version` |
| `station_demand_forecast` | `station` | `inference_output`, `rental_model_manifest`, `return_model_manifest` | `expected_sta_id_sha256`, `horizon_count`, `quantile_policy_decision`, `rounding_mode` |
| `weather_forecast` | `station`, `weather_grid` | `short_term_manifest`, `ultra_short_manifest` | `forecast_hour_count`, `resolver_version` |
| `event:cultural_event` | 없음 | `cultural_event_manifest` | `event_identity_version`, `event_policy_version` |
| `event:performance_event` | 없음 | `performance_event_manifest`, `stadium_coordinate_seed` | `event_policy_version`, `stadium_coordinate_version` |
| `station_urgency` | `station`, `station_demand_forecast`, `station_stock` | `demand_publication_manifest`, `stock_publication_manifest`, `stock_history_manifest_01` … `stock_history_manifest_05`, `urgency_output` | `expected_sta_id_sha256`, `quantile_policy_decision`, `rebalance_policy_config`, `scoring_config_version`, `stock_window_count` |
| `rebalance_route` | `dispatch_center`, `station`, `station_demand_forecast`, `station_stock`, `station_urgency` | `route_coverage`, `urgency_publication_manifest` | `max_routes_per_center`, `max_stops_per_route`, `rebalance_policy_config`, `route_algorithm_version`, `route_coverage_sha256`, `route_work_unit_config_version`, `truck_capacity`, `truck_capacity_config_version` |

표의 dependency 집합은 정확히 그 key들이며 각 role과 parameter는 정확히 한 번 나온다.
`stock_history_manifest_01 … stock_history_manifest_05`는 suffix `01`, `02`, `03`, `04`,
`05` 다섯 role을 뜻하며 현재 window는 `stock_publication_manifest`가 소유한다.
시간 방향은 기존 25분 lookback reader와 동일한 오래된 순서다. urgency logical time을
`t`라고 할 때 `01=t-25분`, `02=t-20분`, `03=t-15분`, `04=t-10분`,
`05=t-5분`이고 `stock_publication_manifest=t`다. 따라서
`stock_window_count`의 exact 값은 과거 다섯 window와 현재 하나를 합친 문자열 `"6"`이다.
다섯 과거 source manifest는 모두 authoritative complete snapshot이어야 한다. 다만 각
snapshot 자체가 완전하다면 신규 station이 과거 일부 snapshot에 존재하지 않는 것은
허용하며, station별 추세는 존재하는 과거 point와 current point로 계산한다.

`scoring_config_version`의 최초 exact 값은 `urgency-scoring-v1`이다. 이 version은
`RESPONSE_LAG_MIN=30`, `HALF_LIFE_MIN=60`, `FIRST_FORECAST_MIN=60`,
`SUPPLY_LOW_STOCK_RATIO=0.20`, `SEVERITY_SCALE=1.5`와 현재 trend·severity·rounding 의미를
함께 가리킨다. 이 값이나 알고리즘 의미가 바뀌면 config version과 urgency publisher
version을 함께 올린다. 과거 source snapshot 또는 current stock의 같은 logical time
higher correction을 재계산·재게시할 때는 동일 urgency anchor의 명시적으로 더 큰
revision을 사용해야 하며 exact same version·fingerprint replay만 no-op이다.

`station_previous_projection`만 최초 station state가 없으면 0개,
있으면 정확히 1개다. `station_relocation_approval`은 100m 초과 Point 후보를 이번
publication에서 실제 승인 반영할 때만 정확히 1개이고 그 외에는 0개다. 그 밖의
extra·누락·중복 원소는 거부한다.

### dependency와 input manifest 결합

`demand_publication_manifest`, `stock_publication_manifest`,
`urgency_publication_manifest`의 URI는 각각 동명 dependency
`station_demand_forecast`, `station_stock`, `station_urgency`의 `manifest_uri`와 정확히
같아야 한다. publisher는 실제 manifest bytes의 SHA를 input artifact에 기록하고, manifest
안 publication key·logical time·revision·artifact/input hash가 dependency tuple과 모두
같은지 lock 안에서 검증한다. 이름만 최신 dependency를 넣고 실제로는 과거 artifact를 읽는
조합은 거부한다.

route publisher는 `urgency_publication_manifest`의 `input_fingerprint_uri`를 열어 그 안
`station`, `station_demand_forecast`, `station_stock` dependency tuple이 route fingerprint의
동명 현재 tuple과 byte-for-byte 같은지도 확인한다. 다르면 stock/demand/station correction
뒤 urgency가 아직 재게시되지 않은 것이므로 proposed route를 만들지 않는다.

`station`과 `station_stock`의 같은 realtime release처럼 dependency가 아직 같은 transaction에서
새로 만들어지는 경우 서로의 미커밋 state를 참조하지 않는다. 둘은 같은 realtime manifest
identity를 입력으로 갖고 두 key의 target/state를 한 transaction에서 전진시킨다.

### station lifecycle 입력

`station_realtime_window_set` artifact는 RFC 8785로 직렬화한
`gold-station-realtime-window-set-v1` 문서다. root는 정확히 `schema_version`, `windows` 두
key다. `windows`는 candidate를 포함해 최신 authoritative realtime의 서로 다른 logical
window 최대 3개를 `logical_dttm DESC`로 정렬한다. 같은 logical window는 가장 큰
correction revision 하나만 둔다. 각 원소는 정확히 `byte_sha256`, `logical_dttm`,
`revision_no`, `uri`를 가지며 candidate manifest가 첫 원소여야 한다. 최초라 window가
3개보다 적으면 존재하는 1~2개만 넣고, 0개인 station publication은 거부한다.

```json
{"schema_version":"gold-station-realtime-window-set-v1","windows":[{"byte_sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","logical_dttm":"2026-08-19T16:00:00.000000Z","revision_no":0,"uri":"s3://fixture/bike-station-realtime-20260819T160000Z.json"}]}
```

위 bytes의 SHA-256은
`ad7674bc8e3b0ddc6ac06a0939b9a23519d34132efe28936c9c28fc764740132`다.

기존 station state가 있으면 `station_previous_projection`은 그 state의 `manifest_uri`가
가리키는 manifest 안 `station` output artifact와 URI·byte SHA가 정확히 같아야 한다.
publisher는 station publication lock 안에서 이를 재확인하고 그 prior projection을 LKG와
기존 Point 대비 거리 판단에 쓴다. state가 없으면 이 role을 넣지 않고 LKG·invalid streak가
없는 최초 게시 규칙을 적용한다. window set의 최대 3개 manifest가 연속 invalid 판정을
재현하므로 별도 mutable counter는 두지 않는다. 같은 realtime release의 `station_stock`
입력 manifest는 window set 첫 원소와 같아야 한다.

### station relocation 승인

`station_relocation_approval`은 RFC 8785의
`gold-station-relocation-approval-v1` 문서다. root는 정확히 `schema_version`, `approvals`
두 key다. 각 approval은 정확히 `approval_id`, `approved_by`, `approved_dttm`,
`candidate_point_ewkb`, `comparison_cd`, `reference_point_ewkb`, `sta_id`를 가진다.
`comparison_cd`는 `gold_vs_master` 또는 `master_vs_realtime`이며 배열은
`(sta_id, comparison_cd, approval_id)`의 UTF-8 byte 오름차순이다.

```json
{"approvals":[{"approval_id":"REL-20260820-001","approved_by":"data-owner","approved_dttm":"2026-08-20T00:00:00.000000Z","candidate_point_ewkb":"0020000001000010e6405fc020c49ba5e34042c04189374bc7","comparison_cd":"gold_vs_master","reference_point_ewkb":"0020000001000010e6405fc000000000004042c00000000000","sta_id":"ST-1"}],"schema_version":"gold-station-relocation-approval-v1"}
```

위 bytes의 SHA-256은
`210d13ebc01aae9ae6941eb6b159c98d477f80ae06f4b3ece8616d09367eeed1`다. publisher는 station
lock 안에서 candidate/reference가 현재 source/prior projection Point와 byte-for-byte
같고 geography 거리가 100m를 초과하는지 확인한다. 실제 반영하는 모든 100m 초과 후보에
승인이 정확히 하나 있어야 하며 반영하지 않는 후보의 여분 승인은 거부한다. 따라서 같은
master/realtime 입력에서 LKG를 유지했는지 승인 Point로 바꿨는지가 fingerprint로 재현된다.

## publication manifest

`gold-publication-manifest-v1`은 아래 13개 key가 모두 있어야 한다.

1. `artifact_set_sha256`
2. `artifacts`
3. `input_fingerprint_schema` (`gold-input-fingerprint-v1`)
4. `input_fingerprint_sha256`
5. `input_fingerprint_uri`
6. `logical_dttm`
7. `publication_key`
8. `published_row_cnt`
9. `publisher_version`
10. `revision_no`
11. `schema_version` (`gold-publication-manifest-v1`)
12. `target_row_counts`
13. `target_schema_version` (`gold-postgis-v1`)

`artifacts`는 artifact-set 문서와 byte-for-byte 같은 배열이다. `target_row_counts`는 대상
테이블명을 key, 행 수를 integer로 가진 object다. `published_row_cnt`는 registry가 정의한
대표 target 수이며 route는 header 수다. manifest 자체는 마지막에 새 immutable URI로 쓰고
그 URI를 `publication_state.manifest_uri`에 기록한다.
`input_fingerprint_uri`도 immutable `gold-input-fingerprint-v1` JSON을 가리키며 실제 bytes의
SHA-256이 `input_fingerprint_sha256`과 같아야 한다. 따라서 state의 manifest URI 하나에서
과거 dependency·input artifact·parameter를 복원해 감사할 수 있다.

행 수는 transaction 뒤 물리 테이블 전체가 아니라 해당 publication key가 소유해 이번에
게시한 projection의 수다. 따라서 event key의 `event`는 해당 source 행만, route의 두 count는
새 proposed header/stop만 센다. 보존된 다른 source 행사와 terminal route 이력은 포함하지
않는다.

nonempty publication의 output artifact role과 `target_row_counts` key는 아래 표가 정확한
목록이며 각 role은 한 번만 나온다. 단일-table publication은 해당 target count가 곧
`published_row_cnt`다. 정상 EMPTY는 `artifacts=[]`, EMPTY artifact-set hash를 쓰고 모든
target count와 `published_row_cnt`를 0으로 둔다.

| publication key | output artifact role | `target_row_counts` key |
| --- | --- | --- |
| `weather_grid` | `weather_grid` | `weather_grid` |
| `dispatch_center` | `dispatch_center` | `dispatch_center` |
| `station` | `station` | `station` |
| `station_stock` | `station_stock` | `station_stock` |
| `station_demand_forecast` | `station_demand_forecast` | `station_demand_forecast` |
| `weather_forecast` | `weather_forecast` | `weather_forecast` |
| `event:cultural_event` | `event_cultural_event` | `event` |
| `event:performance_event` | `event_performance_event` | `event` |
| `station_urgency` | `station_urgency` | `station_urgency` |
| `rebalance_route` | `route_stops`, `routes` | `rebalance_route`, `rebalance_route_stop` |

```json
{"artifact_set_sha256":"576eec2c53f1be8985ce531f512f4f4014fe05879d1f53714128dd774d8abf87","artifacts":[{"byte_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","role":"route_stops","row_count":1,"uri":"s3://fixture/route-stops.parquet"},{"byte_sha256":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","role":"routes","row_count":1,"uri":"s3://fixture/routes.parquet"}],"input_fingerprint_schema":"gold-input-fingerprint-v1","input_fingerprint_sha256":"0de0cd4437f089bec16b778cf927c1fef732cd349dca7533a465100b94c5454e","input_fingerprint_uri":"s3://fixture/route-input-fingerprint.json","logical_dttm":"2026-08-19T16:00:00.000000Z","publication_key":"rebalance_route","published_row_cnt":1,"publisher_version":"gold-publisher-v1","revision_no":0,"schema_version":"gold-publication-manifest-v1","target_row_counts":{"rebalance_route":1,"rebalance_route_stop":1},"target_schema_version":"gold-postgis-v1"}
```

위 manifest bytes의 SHA-256은
`01b04e4af53f338184842157ac269915b1be70e45073d1f12b35184e598a49cf`다.

## route coverage

`gold-route-coverage-v1`은 정확히 `schema_version`, `stock_anchor_dttm`, `routes` 세 key다.
route는 `route_id` 오름차순이고 정확히 `completed_dttm`, `dispatched_dttm`, `route_id`,
`status`, `stops`를 가진다. stop은 `visit_no` 오름차순이고 정확히 `action`, `bike_cnt`,
`sta_id`, `visit_no`를 가진다. 대상은 모든 dispatched와 stock anchor 뒤 완료되어 후속
stock에 아직 반영되지 않은 completed route다.

```json
{"routes":[{"completed_dttm":null,"dispatched_dttm":"2026-08-19T16:02:00.000000Z","route_id":"00000000-0000-0000-0000-000000000001","status":"dispatched","stops":[{"action":"pickup","bike_cnt":3,"sta_id":"ST-9001","visit_no":1}]}],"schema_version":"gold-route-coverage-v1","stock_anchor_dttm":"2026-08-19T16:00:00.000000Z"}
```

위 bytes의 SHA-256은
`13cd1f4fe82d4b09370fd4141d1ee1a727f25c5b109de11f06bb904f9c001e8b`다. producer가 이 bytes를
`route_coverage` artifact로 쓰고 manifest parameter `route_coverage_sha256`에 같은 값을
넣는다. publisher는 topology shared→route-operation lock 안에서 DB 현재값으로 같은 문서를
다시 만들어 불일치하면 게시하지 않는다.

## route UUIDv5

namespace는 `d0d59897-9e72-541f-bb05-bd3d113c2639`다. name은 정확히
`dispatch_center_id`, `logical_dttm`, `publication_key`, `revision_no`, `route_ordinal` 다섯
key의 RFC 8785 object다. ordinal은 center 안에서 1부터 시작한다.

```json
{"dispatch_center_id":"center_a","logical_dttm":"2026-08-19T16:00:00.000000Z","publication_key":"rebalance_route","revision_no":0,"route_ordinal":1}
```

위 UTF-8 bytes의 UUIDv5 회귀값은 `7dd58c8d-7dc7-5279-8845-7673c9c87be2`다.
