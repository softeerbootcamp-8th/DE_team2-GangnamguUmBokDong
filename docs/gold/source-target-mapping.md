# Gold 원천–목표 매핑

> 상태: 현재 계약<br>
> 코드 확인일: 2026-08-25

이 문서는 Collector source, 기준정보 seed, 모델 추론과 재배치 산출물이 어떤 Gold publication과 API로 이어지는지 추적한다. 물리 column은 `data-dictionary.md`, table 관계는 `target-erd.md`, exact manifest 계약은 `publication-contract-v1.md`를 따른다.

## 전체 흐름

```text
Collector source authority ─┐
기준정보 seed ──────────────┼→ immutable input fingerprint
Model serving release ──────┤             ↓
Inference output ───────────┘       Gold publisher
                                        ↓
                              publication manifest + PostGIS
                                        ↓
                                  FastAPI → Web
```

Gold publisher는 mutable Silver prefix를 직접 스캔하지 않는다. Collector의 authoritative source manifest가 가리키는 exact URI와 SHA를 읽는다.

## Collector source 10종

| Source | 직접 Gold target | 간접 사용 | Gold에 보관하지 않는 것 |
| --- | --- | --- | --- |
| `bike_station_master` | `station` | 이름·주소·좌표의 우선 source | master snapshot 이력 |
| `bike_station_realtime` | `station`, `station_stock` | urgency의 현재·과거 재고 | 5분 재고 이력 |
| `bike_rental_history` | 없음 | rental·return lag와 모델 feature | 개별 대여·반납 이력 |
| `population_realtime` | 없음 | 격자 인구 normalizer와 inference | POI별 현재·예측 인구 |
| `living_population_grid` | 없음 | nowcast baseline과 inference | 격자·연령·성별 인구 이력 |
| `weather_ultra_short_live` | 없음 | 현재 inference 날씨와 Archive | 실황 이력 |
| `weather_ultra_short_forecast` | `weather_forecast` | 미래 inference 날씨 | 제품별 원본 예보 revision |
| `weather_short_term_forecast` | `weather_forecast` | 초단기 범위 밖 미래 날씨 | 제품별 원본 예보 revision |
| `cultural_event` | `event` | 주변 행사 API | 원본 행사 응답 |
| `performance_event` | `event` | 주변 행사 API | 원본 행사 응답 |

“직접 Gold target 없음”은 버려지는 source라는 뜻이 아니다. 해당 데이터는 Silver·Archive에 보존되고 normalizer, nowcaster, feature engineering 또는 inference가 소비한다.

## 기준정보 Seed

| 입력 | Publication | Target | 의존 consumer |
| --- | --- | --- | --- |
| 세 weather source YAML의 동일 격자 집합 | `weather_grid` | `weather_grid` | station, weather forecast |
| `docs/gold/dispatch-center-seed.yaml` | `dispatch_center` | `dispatch_center` | station, route |

Weather grid seed는 사람이 별도 파일로 중복 관리하지 않는다. Loader가 세 YAML에서 34개 격자를 읽고 집합·ID·canonical bytes를 검증한다.

Dispatch center seed에는 좌표의 source와 정확도 등급이 포함된다. Station은 가장 가까운 활성 센터를 선택해 `dispatch_center_id`를 물리화한다.

## Realtime serving publication

### 준비 단계

`prepare_serving_plan`은 다음 입력을 pin한다.

- 최신 허용 범위의 `bike_station_master`
- exact 현재 `bike_station_realtime`
- `weather_ultra_short_forecast`와 `weather_short_term_forecast`
- 현재 `weather_grid`, `dispatch_center` publication dependency
- rental·return model serving release와 지원 station ID set
- 기존 station·stock·weather state

계획 단계는 아직 Gold를 바꾸지 않는다. 다음 prepared publication을 S3에 만든다.

```text
station
station_stock
weather_forecast
```

### 추론과 최종 게시

Inference는 plan과 동일한 model·source input을 사용해 station별 미래 1~12시간 대여·반납량을 만든다. Finalize는 inference manifest를 검증한 뒤 다음 네 key를 한 DB transaction으로 게시한다.

| Publication key | Target | 핵심 입력 |
| --- | --- | --- |
| `station` | `station` | master + 최근 realtime windows + 두 seed dependency |
| `station_stock` | `station_stock` | exact 현재 realtime snapshot |
| `station_demand_forecast` | `station_demand_forecast` | plan-bound inference output + station dependency |
| `weather_forecast` | `weather_forecast` | 두 예보 source + active grid set |

네 publication은 동일 plan과 anchor를 공유한다. Plan 준비 후 source authority나 Gold dependency가 바뀌면 finalize가 stale input을 게시하지 않고 실패한다.

## Station mapping

| Target column | Source·변환 |
| --- | --- |
| `sta_id` | realtime `stationId`; `ST-<number>` 검증 |
| `sta_nm` | master/realtime 이름 정책 |
| `sta_addr` | master 주소 |
| `hold_cnt` | 유효한 거치대 수 |
| `sta_point` | master 좌표 우선, realtime fallback |
| `sta_point_source_cd` | 실제 선택 좌표의 source code |
| `weather_grid_id` | station Point에서 가장 가까운 승인 격자 |
| `dispatch_center_id` | station Point에서 가장 가까운 활성 센터 |
| `master_base_dttm` | 사용한 master authority 시각 |
| `last_seen_dttm` | station을 확인한 realtime authority 시각 |
| `is_active` | serving activation·lifecycle 정책 결과 |

Station master 한 번만으로 신규 station을 바로 활성화하지 않는다. 현재 재고, 양쪽 모델 지원과 필요한 serving projection이 함께 준비되어야 한다.

## Weather resolver

Gold에는 제품별 table을 만들지 않고 `(weather_grid_id, forecast_dttm)`마다 한 행을 선택한다.

- 가까운 horizon은 유효한 초단기예보를 우선한다.
- 초단기 범위 밖은 단기예보를 사용한다.
- 각 행에 선택된 `source_product_cd`와 원 발표 `base_dttm`을 남긴다.
- Active station이 사용하는 grid와 화면에 필요한 시간 범위가 완전해야 한다.

초단기실황은 모델 feature에 사용하지만 미래 대시보드 projection인 `weather_forecast`에는 직접 게시하지 않는다.

## Event publication

두 source는 서로 다른 publication key로 독립 게시한다.

| Source | Publication key | ID·Point |
| --- | --- | --- |
| 문화행사 | `event:cultural_event` | canonical source field hash + source 좌표 |
| 체육시설 행사 | `event:performance_event` | source event ID + curated stadium 좌표 |

Publisher는 자기 source row만 reconcile한다. 문화행사 publication이 공연행사 row를 삭제하거나 반대 source의 watermark를 갱신하지 않는다. 정상 `EMPTY`도 해당 source row를 비우고 publication state를 전진시킨다.

## Urgency와 Route

```text
station + station_stock + station_demand_forecast
                    ↓
             station_urgency
                    ↓
dispatch_center + station topology + 진행 중 route
                    ↓
       rebalance_route + rebalance_route_stop
```

### `station_urgency`

입력은 동일 release의 station, stock, demand와 최근 authoritative realtime windows다. 예측 재고와 임계 도달 시간을 계산해 score, 필요 유형과 이동량 artifact를 만든다. Gold target에는 API가 사용하는 score·시간·유형을 게시한다.

### `rebalance_route`

`route-v4-supply-led-pickup-sla` publisher는 `station_urgency` publication manifest와 현재
Gold topology를 입력으로 사용한다. 센터별 최고 supply urgency가 경로 순서를 소유하고
`center→pickup→supply` 총거리로 안전한 pickup을 고른다. 실제 pickup 방문은 센터부터
최근접 순서이며 이동속도 20km/h와 stop당 3분을 적용한 마지막 pickup 실행시각이 dispatch
뒤 30분 이하여야 한다. 큰 split이 이 SLA를 넘으면 더 작은 pickup·dropoff 완결 route로
분리하고, 단일 pickup도 30분 밖인 donor는 제외한다. 모든 pickup 뒤 최고 supply를 첫
dropoff로 두어 결정적 UUID와 stop 순서를 만들고 proposed route와 stop을 원자 게시한다.

정책의 exclusive 설정은 같은 pickup station을 한 plan의 여러 route로 나누지 않는다.
진행 중 route가 예약한 pickup과 완료 뒤 cooldown 안의 pickup은
`pickup_cooldown_station_ids` artifact로 고정해 새 후보에서 제외하며, 해당 정책 config와
SLA 속도·작업시간·상한·버전은 route input fingerprint에 남긴다.

이미 dispatched·completed·cancelled인 route와 해당 stop은 새 제안 publication이 삭제하지 않는다.

## Gold target과 API

| Gold target | 주요 API |
| --- | --- |
| `station`, `station_stock`, `dispatch_center` | `GET /stations`, `GET /stations/{sta_id}`, `GET /regions` |
| `station_demand_forecast`, `station_stock` | `GET /status`, `GET /stations/{sta_id}/forecast` |
| `weather_forecast`, `weather_grid`, `station` | `GET /stations/{sta_id}/weather` |
| `event`, `station` | `GET /stations/{sta_id}/events` |
| `station_urgency`, station·stock·center | `GET /alerts` |
| `rebalance_route`, `rebalance_route_stop` | `GET /routes`, `GET /routes/{route_id}`와 상태 변경 POST |

API는 S3를 직접 읽지 않는다. Gold의 공통 anchor, completeness와 freshness가 맞지 않으면 서로 다른 publication을 섞지 않고 503·409·빈 결과 등의 endpoint 계약으로 실패를 드러낸다.

## Publication 전 공통 Gate

Source 기반 publisher는 다음을 검증한다.

- source ID와 배포된 YAML의 config SHA 일치
- 허용된 `SUCCEEDED` 또는 source가 명시적으로 허용한 `EMPTY`
- planned part와 completed part 완전성
- exact Silver URI·SHA와 실제 bytes 일치
- schema, 자연키, ID set, 공간·시간 불변식
- dependency publication state가 준비 이후 바뀌지 않음

`PARTIAL`, `FAILED`, 임의의 legacy Silver key 또는 수정된 manifest bytes는 Gold publication 근거가 될 수 없다.
문화·공연행사 CLI는 completed PARTIAL diagnostic을 새 Gold 입력으로 쓰지 않는다. 기존
`publication_state`와 그 content-addressed publication manifest가 일치하는지 확인해
Gold 행과 state를 변경하지 않을지를 결정하는 근거로만 사용한다. 이 경로에서 event
output artifact 전체나 현재 DB 행을 다시 물질화해 검증하지는 않는다.

## Gold에 의도적으로 두지 않는 데이터

- 대여·반납 원본 이력
- 재고 snapshot 이력
- 생활인구 격자와 실시간 POI 인구
- 제품별 날씨 발표 이력과 실황
- feature mart와 model training data
- inference 입력 원본과 이전 prediction history
- 자치구·행정동 master

이 데이터는 감사·재학습·재현 목적의 S3 계층이 소유한다. Gold table을 추가할지는 실제 API 소비 계약이 생겼을 때 결정한다.

## 코드 기준 위치

- Source authority 탐색: `loader/gold/source_catalog.py`
- Source policy 검증: `loader/gold/source_policy.py`
- Serving plan·4-key finalize: `loader/gold/serving_plan.py`
- Station·stock: `loader/gold/station_release.py`
- Demand·weather: `loader/gold/demand.py`, `loader/gold/weather_forecast.py`
- Event: `loader/gold/event.py`
- Urgency·route: `loader/gold/urgency.py`, `loader/gold/rebalance_route.py`
- API 소비: `apps/api/main.py`, `apps/api/queries.py`
