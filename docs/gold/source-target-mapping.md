# Gold 원천-목표 매핑

## 문서 목적

이 문서는 #129 Gold RDS에 들어오는 데이터와 나가는 서빙 계약을 한곳에서 추적한다.
Gold는 원천 보관소가 아니라 대시보드와 재배치 운영을 위한 최신 서빙 projection이다.
원천·발표·학습 이력은 S3 Bronze/Silver가 소유하며, Gold에는 소비자가 읽는 최소
형태만 둔다.

최종 `public` 서빙 범위는 다음 10개 테이블이다.

1. `weather_grid`
2. `dispatch_center`
3. `station`
4. `station_stock`
5. `station_demand_forecast`
6. `weather_forecast`
7. `event`
8. `station_urgency`
9. `rebalance_route`
10. `rebalance_route_stop`

별도 `gold_meta`에는 API가 읽지 않는 `publication_state` 제어 테이블 하나만 둔다. 이는
원천을 더 보관하는 테이블이 아니라 EMPTY·stale·correction을 target transaction과 함께
기억하는 watermark다.

물리 타입과 제약은 [target-schema.sql](target-schema.sql), 컬럼 정의는
[data-dictionary.md](data-dictionary.md), 관계와 한 행의 의미는
[target-erd.md](target-erd.md)를 기준으로 한다. 이 문서는 그 앞뒤의 데이터 흐름과
게시 계약을 정의한다.

## 범위 경계

다음 항목은 의도적으로 Gold에 만들지 않는다.

- `gu_master`, `dong_master`: 현재 화면은 행정구·행정동으로 표시하거나 필터링하지
  않는다. 주소 문자열은 `station.sta_addr`로 충분하다.
- `station.gu_id`, `station.gu_nm`: 기존 API와 Web 타입의 `gu`도 #129 전환에서
  제거한다.
- `weather_current`: 대시보드 요구사항은 1시간 간격 예보이며 실황 이력 소비자가 없다.
- 제품별 날씨 테이블: 단기·초단기는 Silver에서 분리 보존하고 Gold에는 격자·정시당
  선택된 한 행만 둔다.
- `event_spot`: 장소를 독립 조회·관리하는 소비자가 없으므로 행사명·장소명·Point를
  `event` 한 행에 평탄화한다.
- 대여 이력, 재고 이력, 인구 격자·행정동·POI, 모델 피처, 원천 발표 이력: 학습과
  재현을 위한 Silver 소유 데이터다.

현행 `gu`는 [API 스키마](../../apps/api/schemas.py#L9-L24)와
[Web DTO](../../apps/web/src/api.ts#L5-L20)에 선언돼 있지만, 화면의 실제 지역 필터는
`region`만 사용한다([App.tsx](../../apps/web/src/App.tsx#L102-L108)). 따라서 목표 API의
`StationSummary`/`StationDetail`에서도 `gu`를 제거하며 호환용 빈 값을 남기지 않는다.

## Gold 게시 전 공통 게이트

Collector가 Silver 파일을 썼다는 사실만으로 Gold 게시 자격이 생기지 않는다. 현행
Collector는 게이트 이내의 누락·폐기가 있으면 `PARTIAL` Silver도 쓴다
([pipeline.py](../../collector/pipeline.py#L461-L511)). 삭제·비활성화나 완전 snapshot
교체의 근거로 쓰려면 성공과 EMPTY를 구분해 검증한다.

일반 `SUCCEEDED` source는 다음을 모두 만족해야 한다.

1. collector manifest가 `stage=completed`, `failure_reason=null`, `status=SUCCEEDED`,
   `artifacts.silver!=null`이다.
2. `missing.parts=()`, `missing.rows`가 null/0, `completeness=1.0`, `dropped=0`이고 source별
   pagination 종료·expected/actual 자연키 집합이 맞는다.
3. publisher가 Silver 실제 bytes checksum, schema/config version, 키 중복·필수값·공간·시간
   불변식을 다시 검증한다.

행사 source의 `EMPTY`는 현 collector 구현상 Silver URI와 completeness가 모두 null이다
([pipeline.py](../../collector/pipeline.py#L424-L432)). 따라서 일반 조건을 억지로 적용하지
않고 다음을 모두 요구한다: source config가 `allow_empty=true`, `stage=completed`,
`status=EMPTY`, `failure_reason=null`, `counts.expected=0`, `counts.fetched=0`, missing part/row가
없고 API pagination이 total=0으로 정상 종료됐다는 증거가 있다. unknown total, fetch 오류,
PARTIAL 또는 validation 뒤 전 행 drop은 EMPTY가 아니다.

모든 seed·collector·ML·urgency·route producer는 출력이 완성된 뒤 immutable upstream
success manifest를 마지막에 기록한다. Gold publisher는 이를 staging에서 검증한 뒤 고정
publication key, logical time, correction revision, schema/publisher version, upstream
version/config를 고정한 input fingerprint와 정렬된 output URI·byte SHA-256·행 수를 가진
Gold publication manifest를 만든다. 정확한 13개 field와 immutable input document URI는
[publication-contract-v1.md](publication-contract-v1.md)를 따른다. 동일 S3 key를 덮어쓰거나
일부 artifact를 먼저 canonical key에 쓰는 결과는 게시하지 않는다.

publisher는 필요한 topology/route lock을 정해진 순서로 잡은 뒤
`gold_meta.claim_publication()`을 호출한다. 과거 version은 stale no-op, 같은 version·같은
fingerprint는 exact no-op, 같은 version·다른 fingerprint는 hard fail, 같은 logical time의
더 큰 revision만 correction이다. staging 검증, 기존 projection 교체, 삽입·만료 정리와
watermark 전진을 한 DB transaction으로 수행한다. 검증 실패 시 기존 Gold와 state는 모두
그대로다. 정상 EMPTY는 target을 0건으로 reconcile하고 row count 0인 state를 남긴다.

모든 업무 시각은 유한해야 하고, publication logical time과 관측·계산·발표·제안·last-seen
시각은 DB `clock_timestamp()`보다 5분을 넘게 미래일 수 없다. seed effective time도 실제
발효 전에는 claim하지 않는다. 이는 `infinity`뿐 아니라 2099년 같은 유한한 오염값이
watermark와 최신행을 장기간 독점하는 것을 막는 공통 게이트다. 소비 API의 freshness도
`now-허용기간 <= base_dttm <= now+5분`처럼 양방향으로 검사한다.

`PARTIAL`과 품질 실패 행은 Silver/quarantine에서 조사할 수 있지만 authoritative
snapshot으로 취급하지 않는다. Collector가 폐기 행을 quarantine에 쓰고, 게이트 실패
시 Silver를 쓰지 않는 현재 경계는 [pipeline.py](../../collector/pipeline.py#L475-L511)에
구현돼 있다.

### publication registry

| `publication_key` | `logical_dttm` | `published_row_cnt` | EMPTY |
| --- | --- | ---: | --- |
| `weather_grid` | seed manifest effective time | grid 행 수(정상 34) | 금지 |
| `dispatch_center` | seed manifest effective time | center 행 수(정상 11) | 금지 |
| `station` | station projection effective time(보통 선택 realtime window) | station target 전체 행 수 | 금지 |
| `station_stock` | realtime window | 해당 window에서 반영한 stock 행 수 | 금지 |
| `station_demand_forecast` | inference anchor | projection 전체 행 수 | active∩두 model support가 0임을 topology/model manifest로 입증한 때만 허용 |
| `weather_forecast` | resolver scheduled anchor | 13시간 buffer 전체 행 수 | active grid가 0임을 topology lock에서 입증한 때만 허용 |
| `event:cultural_event` | source collection window | 해당 source의 Gold 행사 행 수 | 허용 |
| `event:performance_event` | source collection window | 해당 source의 Gold 행사 행 수 | 허용 |
| `station_urgency` | compute anchor | projection 전체 행 수 | 기대 집합이 0임을 입력 manifest가 입증한 때만 허용 |
| `rebalance_route` | compute anchor | proposed header 수 | 허용; stop 수는 manifest 별도 필드 |

revision은 각 `(publication_key, logical_dttm)`에서 publisher가 0부터 관리하는 명시적
correction ordinal이다. 복수 upstream revision의 max를 대신 쓰지 않는다. 여러 key를
한 transaction에서 claim하면 위 key 문자열 오름차순으로 잠근다. 교차 dataset lock은
topology `(129,1)` 뒤 route-operation `(129,2)`, 그 뒤 publication key 순서다.

한 transaction이 여러 target을 바꾸면 관련 state도 전부 claim한다. 센터 seed/Point/active
변경과 station 재배정은 `dispatch_center`와 `station`, grid seed와 station grid 재배정은
`station`과 `weather_grid`를 함께 전진시킨다. 새 active grid의 13시간 weather coverage가
현재 projection에 없으면 같은 transaction에서 `weather_forecast`까지 재게시하거나 해당
station 활성화를 미룬다. 실제로 바꾸지 않은 dependent target의 state를 거짓으로
전진시키지 않는다. station manifest의 input fingerprint에는 선택한 master/realtime,
grid와 center seed version이 모두 들어간다.

artifact 집합, input fingerprint, publication manifest, Point EWKB, route coverage와
UUIDv5의 **정확한 JSON key·배열 정렬·바이트·SHA 회귀값은
[publication-contract-v1.md](publication-contract-v1.md)가 유일한 SSOT**다. 임의의 topology
JSON을 따로 만들지 않는다. downstream은 잠근 `publication_state`의 station·grid·center·
stock·demand·urgency state tuple을 input dependency로 포함해 그 publication이 소유한
topology/support를 식별한다. route의 실행 상태만 별도의 `gold-route-coverage-v1` artifact로
고정해 lock 안에서 DB와 다시 대조한다.

producer와 publisher가 같은 contract version과 회귀 벡터를 만들지 못하거나 immutable
URI의 실제 bytes가 manifest SHA와 다르면 claim 전에 실패한다. 입력·parameter·artifact
role을 추가하거나 JSON 모양을 바꾸는 변경은 contract schema와 publisher version을 함께
올린다.

## 원천 10종 전수 분류

### 요약

| Collector source | 주기 | Gold 진입 | 목표 테이블 | 결론 |
| --- | --- | --- | --- | --- |
| `bike_station_master` | 1일 | 직접+결합 | `station` | ID·주소·기본 Point 소유 |
| `bike_station_realtime` | 5분 | 직접+결합 | `station`, `station_stock` | 명칭·정원·현재 존재·재고 소유 |
| `bike_rental_history` | 5분 | 없음 | 없음 | ML 학습·추론용 Silver 이력 |
| `weather_short_term_forecast` | 3시간 | resolver 경유 | `weather_forecast` | 정시별 fallback 후보 |
| `weather_ultra_short_forecast` | 30분 | resolver 경유 | `weather_forecast` | 정시별 우선 후보 |
| `weather_ultra_short_live` | 10분 | 없음 | 없음 | 실황은 Silver 전용 |
| `cultural_event` | 1일 | 직접 변환 | `event` | 유효 Point가 있는 현재·예정 행사만 게시 |
| `performance_event` | 1일 | 정적 좌표 결합 | `event` | `SCH_SEQ` ID, 시설 Point 보강 |
| `living_population_grid` | 1일 | 없음 | 없음 | ML/Normalizer용 Silver, 행정동도 Gold 제외 |
| `population_realtime` | 5분 | 없음 | 없음 | ML/Normalizer용 Silver POI 관측 |

### 따릉이 대여소 마스터

원천 선언은 [bike_station_master.yaml](../../collector/sources/bike_station_master.yaml#L1-L40)이다.

| 원천 | 목표 | 변환·소유 규칙 |
| --- | --- | --- |
| `RNTLS_ID` | `station.sta_id` | 문자열 그대로. realtime `stationId`와 조인하는 기준 키 |
| `ADDR1` | `station.sta_addr` | 비어 있지 않은 실제 도로명주소만 게시 |
| `LAT`, `LOT` | `station.sta_point` | 유효 WGS84이면 기본 좌표. `ST_SetSRID(ST_MakePoint(LOT,LAT),4326)` |
| 수집 window | `station.master_base_dttm` | 사용한 완전 master snapshot의 기준 일시 |
| `ADDR2` | 없음 | 상세주소·거치대 번호 의미가 섞여 있어 Silver에만 보존 |

Master 좌표가 무효일 때만 동일 `sta_id`의 완전 realtime snapshot 좌표를 fallback으로
쓴다. 현재 Normalizer도 master 좌표 유효성 검사 후 realtime으로 폴백한다
([station_master.py](../../normalizer/station_master.py#L94-L135)). 다만 목표 Gold는
생활인구 `CELL_ID`를 함께 싣지 않고 Point에서 `weather_grid_id`와
`dispatch_center_id`만 계산한다.

기존 station의 생존 여부는 실제 운영 존재를 더 자주 보여주는 realtime이 소유한다.
완전 master에서 기존 ID가 빠지거나 ADDR1/Point가 일시적으로 invalid여도 realtime이
serving-valid이면 마지막으로 검증된 주소·master Point와 그때의 `master_base_dttm`을
유지하고 master-missing/invalid 지표를 남긴다. 유효한 master 변경만 topology candidate로
받는다. 같은 ID의 master/realtime Point 또는 기존 Gold/new master Point의 geography
`ST_Distance`가 100m를 초과하면 자동 평균·덮어쓰기하지 않고 이전값을 유지한 채 review
queue로 보낸다. 정확히 100m는 자동 허용 범위다. 승인된
relocation만 명시적 station correction revision과
[publication-contract-v1.md](publication-contract-v1.md)의 immutable approval artifact로
반영한다.

### 따릉이 실시간 대여정보

원천 선언은 [bike_station_realtime.yaml](../../collector/sources/bike_station_realtime.yaml#L1-L51)이다.

| 원천 | 목표 | 변환·소유 규칙 |
| --- | --- | --- |
| `stationId` | `station.sta_id`, `station_stock.sta_id` | master와 동일 키여야 함 |
| `stationName` | `station.sta_nm` | 비어 있지 않은 표시명 |
| `rackTotCnt` | `station.hold_cnt` | `> 0`인 값만 활성 station 게시 |
| `parkingBikeTotCnt` | `station_stock.parking_bike_tot_cnt` | `>= 0`; 정원 초과는 허용 |
| `stationLatitude`, `stationLongitude` | `station.sta_point` | master Point가 무효일 때만 fallback |
| 수집 window | `station.last_seen_dttm`, `station_stock.base_dttm` | 완전 snapshot의 관측 기준 일시 |
| `shared` | 없음 | `stock / hold_cnt`로 계산 가능하므로 Silver에만 보존 |

Gold 게시의 선행 차단 조건이 하나 있다. `bikeList`는 `list_total_count`에 전체 건수가
아니라 현재 페이지 건수를 반환해 현행 adapter가 첫 1,000행에서 멈추는데도 완전으로
판정한다. 실측 원인과 영향은
[source-config-audit.md](../collector/source-config-audit.md#5-18-bike_station_realtime--대여소의-634를-매-tick-놓친다--2차-재점검-신규)에
기록돼 있다. 빈 페이지/`INFO-200`까지 탐색하는 전용 pagination을 적용하고 실제 받은
전체 키 수를 검증하기 전에는 이 source로 `station`, `station_stock`을 게시하지 않는다.

이미 게시된 station의 상태 판정은 서로 다른 authoritative realtime `window_start`만 센다.
미관측뿐 아니라 행은 있지만 표시명이 비거나 `rackTotCnt`가 null/0 이하인 경우도
serving-invalid다. 최신 세 window가 연속 invalid면 비활성화하고, valid 재등장은 즉시
활성화 후보가 된다. 같은 window의 높은 correction revision은 그 window 판정을 교체할 뿐 세 번으로
중복 계산하지 않는다. `PARTIAL`·실패·unknown EMPTY는 상태 근거가 아니다.
[publication-contract-v1.md](publication-contract-v1.md)의 immutable realtime window-set과
prior station projection을 입력으로 잠가 이 streak와 주소·Point LKG를 재현한다.

현 collector의 `stationName required:true`와 `required_missing: drop_row`는 이름 결손 행을
버려 위 ID별 invalid 판정을 불가능하게 한다. 전환 시 station ID는 보존하고 이름을 nullable
Silver 값으로 넘겨 publisher가 serving-invalid로 판정하도록 source 계약을 바꾼다. 행을
식별할 수 없는 폐기나 설명되지 않은 `dropped>0` window는 여전히 authoritative하지 않다.

현재 collector 설정의 `rackTotCnt`, `parkingBikeTotCnt` 200 상한은 미래의 실제 큰 정원·
재고를 null/200으로 변조하므로 운영 전 둘 다 제거한다. 음수는 거부하고 비정상적으로 큰
값은 원문을 보존한 채 metric/review 대상으로 표시하되 Gold 값을 임의 상한으로 자르거나
그 이유만으로 station을 비활성화하지 않는다. realtime과 cultural 좌표의 현재
`lat 37.4..37.7`, `lon 126.7..127.2` 사전 outlier 범위도 DDL 안전 box와 같게 완화해 원문을
보존하고 publisher가 SRID·non-empty·안전 box를 최종 판정한다.

inactive→active 전환은 소비 projection과도 맞춘다. 새 station이 참조하는 grid의 13시간
weather coverage가 먼저 있어야 하고, 두 model support에 포함된 station은 같은 anchor의
완전 demand projection에 포함해 함께 활성화하거나 다음 demand publication까지 활성화를
미룬다. model 미지원 station은 활성화할 수 있으며 demand row가 없는 것이
`404 forecast_not_available`의 명시적 근거가 된다. demand·weather·urgency publisher는
topology shared lock을 사용해 이 판정과 교차하지 않는다.

### 따릉이 대여 이력

원천 선언은 [bike_rental_history.yaml](../../collector/sources/bike_rental_history.yaml#L1-L107)이다.
대여·반납 시각과 대여소 ID는 모델의 lag/rolling 피처와 target을 만드는 이력이며 Gold
직접 소비자가 없다. 추론도 최근 트립을 Silver에서 읽어 집계한다
([predict_single.py](../../ml/inference/predict_single.py#L117-L181)). 따라서 어떤 컬럼도
10개 Gold 테이블로 복제하지 않는다.

### 단기·초단기예보

원천 선언은
[weather_short_term_forecast.yaml](../../collector/sources/weather_short_term_forecast.yaml#L1-L110)과
[weather_ultra_short_forecast.yaml](../../collector/sources/weather_ultra_short_forecast.yaml#L1-L105)이다.
두 source의 34개 격자 목록이 같고 중복이 없다는 회귀 검사는
[test_weather_grids_consistency.py](../../collector/tests/test_weather_grids_consistency.py#L8-L30)가
담당한다.

각 source는 Silver에서 독립 snapshot과 발표 이력을 유지한다. Gold publisher는 둘의
최신 완전 snapshot을 함께 읽고 다음 순서로 한 번만 게시한다.

1. `(nx, ny, fcstDate, fcstTime)`을 `(weather_grid_id, forecast_dttm)`으로 바꾸고 정시가
   아닌 대상은 제외한다.
2. source 내부에서는 같은 대상시각의 가장 최신 `baseDate/baseTime` 발표만 후보로 둔다.
3. 같은 격자·정시에 유효한 초단기 후보가 있으면 그 행 전체를 선택하고, 없을 때만
   단기 후보 행 전체를 선택한다. 컬럼별로 두 제품을 섞지 않는다.
4. 선택된 제품을 `source_product_cd`(`ultra_short`/`short_term`)와 `base_dttm`에 남긴다.
5. 활성 station이 참조하는 distinct 격자와 다음 13개 정각의 모든 키가 만들어졌는지
   검증한 뒤 한 transaction으로 교체한다.

| 단기 | 초단기 | `weather_forecast` | 변환 |
| --- | --- | --- | --- |
| `nx`, `ny` | `nx`, `ny` | `weather_grid_id` | `nx || '_' || ny` |
| `baseDate`, `baseTime` | 동일 | `base_dttm` | KST 발표시각 → offset 포함 instant |
| `fcstDate`, `fcstTime` | 동일 | `forecast_dttm` | KST 대상시각 → 정시 instant |
| `TMP` | `T1H` | `temperature` | °C |
| `POP` | `POP` | `precipitation_prob` | % |
| `PCP` | `RN1` | `precipitation_amount` | 공통 parser로 mm 하한값 |
| `REH` | `REH` | `humidity` | % |
| `WSD` | `WSD` | `wind_speed` | m/s |
| `SKY` | `SKY` | `sky_condition_cd` | `1=clear`, `3=mostly_cloudy`, `4=cloudy` |
| `PTY` | `PTY` | `precipitation_type_cd` | 제품별 숫자를 공통 의미 코드로 변환 |

현행 loader는 두 DAG가 같은 물리 테이블에 독립 upsert하도록 alias를 둔다
([loader/config.py](../../loader/config.py#L19-L31),
[loader/tables.yaml](../../loader/tables.yaml#L44-L86)). 이것은 source 선택과 원자 게시를
보장하지 못하므로 목표 publisher에서는 사용하지 않는다.

### 초단기실황

원천 선언은 [weather_ultra_short_live.yaml](../../collector/sources/weather_ultra_short_live.yaml#L1-L92)이다.
`T1H`, `RN1`, `REH`, `WSD`, `PTY` 등은 Silver와 ML 입력에서 유지한다. 대시보드가
요구한 것은 정시 단위 예보이므로 `weather_current` 또는 Gold 실황 이력은 만들지 않는다.
실황과 예보의 같은 이름 필드를 합치지도 않는다.

### 문화행사

원천 선언은 [cultural_event.yaml](../../collector/sources/cultural_event.yaml#L1-L56)이다.

| 원천 | 목표 | 변환·게시 규칙 |
| --- | --- | --- |
| `TITLE`, `PLACE`, `STRTDATE`, `END_DATE` | `source_event_id` | versioned canonical JSON의 `v1:{SHA-256}` |
| source + source ID | `event_id` | `cultural_event:{source_event_id}` |
| `TITLE` | `event_name` | 비어 있지 않아야 함 |
| `CODENAME` | 없음 | 화면 미사용, Silver 보존 |
| `PLACE` | `event_spot_nm` | 표시용 장소명, 독립 master를 만들지 않음 |
| `LOT`, `LAT` | `event_point` | 둘 다 DDL 안전 box의 유효 Point일 때만 Gold 게시 |
| `STRTDATE`, `END_DATE` | `event_start_dt`, `event_end_dt` | KST 달력 `DATE`, 종료일이 시작일 이상 |
| `IS_FREE` | 없음 | 화면 미사용, Silver 보존 |

원천에 안정 ID가 없으므로 제목+장소+시작일을 구분자 없이 합치는 현행 해시
([loader/transform.py](../../loader/transform.py#L226-L261))를 쓰지 않는다. 의미 payload가
아니라 위 네 식별 필드를 문자열 Unicode NFC·trim·연속 공백 한 칸, 빈 선택 장소는
null, 날짜는 ISO `YYYY-MM-DD`로 바꾼다. 순서가 고정된 배열을 RFC 8785 JSON으로
직렬화해 UTF-8 bytes를 hash한다. 비 ASCII 문자는 `ensure_ascii` 방식의 `\uXXXX`로
강제 escape하지 않고 RFC 8785가 요구하는 escape만 쓴다. 회귀 vector
`["서울 축제",null,"2026-08-20","2026-08-21"]`의 SHA-256은
`ca93c5fd090e1423f1923d31cca0b27cc5811e4c8e5710fc12f2861cb3f44e06`이다. 식별 필드가
수정되면 새 ID가 되며, 완전 source snapshot을 같은 transaction에서 reconcile해 사라진
이전 ID를 제거한다. Point가 없거나 무효인 행은 source로서는 정상일 수 있으므로 Silver에
보존하되, 지도 포커싱이 불가능하므로 Gold `event`에는 넣지 않는다.

같은 canonical ID가 중복되면 Gold payload가 byte-equivalent인 경우만 dedupe한다. Point,
날짜 또는 표시값이 다르면 임의 승자를 고르지 않고 source snapshot 전체를 reject하며
collision metric과 payload를 quarantine에 남긴다.

### 체육시설 공연행사

원천 선언은 [performance_event.yaml](../../collector/sources/performance_event.yaml#L1-L69)이다.

| 원천 | 목표 | 변환·게시 규칙 |
| --- | --- | --- |
| `SCH_SEQ` | `source_event_id` | source 내 안정 ID |
| source + source ID | `event_id` | `performance_event:{SCH_SEQ}` |
| `TITLE` | `event_name` | 비어 있지 않아야 함 |
| `CODE_TITLE_A` | 없음 | 화면 미사용, Silver 보존 |
| `CODE_TITLE_B` | `event_spot_nm` | 사람이 읽는 시설명 |
| `SDATE`, `EDATE` | 시작·종료일 | KST 달력 `DATE` |
| `SCH_CODE_B` | `event_point` 조회 키 | 검수된 시설 좌표 seed와 결합 |
| `USE_PAY` | 없음 | 화면 미사용, Silver 보존 |

원천이 Point를 주지 않으므로 [stadium_coords.json](../../loader/assets/stadium_coords.json)을
시설 코드로 조인하고 `event_point_source_cd`, `location_accuracy_cd`에 근사 좌표임을
남긴다. 좌표 seed에 없는 시설의 행사는 Silver에 남기고 Gold 게시에서 제외한다. 현행
변환처럼 좌표 없는 행사를 RDS에 먼저 넣는 방식
([loader/transform.py](../../loader/transform.py#L265-L335))은 사용하지 않는다.

현재 seed를 `stadium-coordinates-v1`로 고정한다. 기준 bytes는 git commit
`5432a84b891b1dfb959883a1dcb37f37eac9e250`, SHA-256
`0e0c047bd08f77e82bbccda969c0e726af6998ceaa92979081506cb2140a969b`이며 2026-08-19
원천 2,397건에서 11개 코드↔명칭이 1:1임을 검수한 OSM Nominatim 근사값이다.
`event:performance_event` manifest input fingerprint에 asset version·immutable URI·byte
hash를 넣는다. 코드가 없으면 해당 행은 Silver-only, 코드가 있지만 명칭이 seed와 다르면
잘못된 좌표를 붙이지 않고 source snapshot을 거부한다. seed가 바뀌면 새 version과
correction revision으로 performance event 전체를 다시 reconcile한다. v1에는 개별 OSM
object ID가 없으므로 이후 좌표 추가·보정부터 query 시각·OSM object ID·검수자를 seed
변경 기록에 필수로 남긴다.

### 생활인구 250m 격자와 실시간 POI 인구

원천 선언은
[living_population_grid.yaml](../../collector/sources/living_population_grid.yaml#L1-L88)과
[population_realtime.yaml](../../collector/sources/population_realtime.yaml#L1-L41)이다.
두 source는 Normalizer가 `living_population_normalized` Silver를 만들고
([normalizer/main.py](../../normalizer/main.py#L122-L160)) ML이 인구 피처로 소비한다.

`H_DNG_CD`, `CELL_ID`, 성·연령별 인구, POI 코드·혼잡도는 대시보드 서빙 계약이 없으므로
Gold에 넣지 않는다. 특히 `H_DNG_CD`가 원천에 있다는 이유만으로 행정동 master/FK를
만들지 않는다. `population_realtime`의 유효 POI 범위가 131개인데 현재 설정은 121까지인
품질 부채도 [source-config-audit.md](../collector/source-config-audit.md#5-19-population_realtime--poi-순회-상한이-실제보다-15개-작다--2차-재점검-신규)에
기록돼 있다. 이는 Silver/ML 품질 수정 대상이지 Gold 스키마를 넓힐 이유는 아니다.

## 정적 seed와 파생 산출물

| 입력 | 목표 | 계약 |
| --- | --- | --- |
| 두 예보 YAML의 공통 34개 `(nx,ny)` | `weather_grid` | ID는 `{nx}_{ny}`. 34개·중복 0·source 간 동일 집합을 seed 전 검증 |
| [dispatch-center-seed.yaml](dispatch-center-seed.yaml) | `dispatch_center` | 11개 안정 slug, 표시명, Point, 정확도, 출처 commit/hash, 검증일을 함께 버전 관리 |
| `stadium-coordinates-v1` (`stadium_coords.json`) | `event` | 공연 시설 11개 Point; commit·byte hash·코드/명칭 일치 검증을 performance manifest에 포함 |
| `station.sta_point` + 활성 센터 | `station.dispatch_center_id` | geography 최근접 센터를 결정론적으로 계산해 materialize |
| `station.sta_point` + 공통 LCC 함수 | `station.weather_grid_id` | `latlon_to_grid` 결과의 자연키가 `weather_grid`에 있어야 함 |
| ML inference parquet | `station_demand_forecast` | 최신 완전 예측 snapshot만 게시 |
| urgency parquet | `station_urgency` | 최신 완전 urgency snapshot만 게시 |
| route/route_stops parquet | `rebalance_route`, `rebalance_route_stop` | 헤더+모든 stop을 aggregate로 원자 게시 |

지역센터의 현재 조사값과 정확도 한계는 [core/regions.py](../../libs/core/src/core/regions.py#L1-L28)에
기록돼 있다. 이를 [dispatch-center-seed.yaml](dispatch-center-seed.yaml)에 안정 ID,
원천 commit/file hash, EPSG와 좌표 순서까지 고정했다. 현장 검증값이 아니므로
`location_verified_dt`는 null이고 10개는 landmark 근사, 영남은 행정동 중심 근사다.
목표 전환에서는 이 versioned seed를 게시한 뒤 DB를 유일한
기준으로 삼는다. 대여소의 센터 배정은 Point 또는 센터 seed 버전이 바뀔 때 전체를
재계산하고, 모든 활성 station이 정확히 한 활성 센터를 참조하는지 검증한 뒤 station
transaction 안에서 반영한다. API와 route producer가 각자 최근접 센터를 다시 계산하면
안 된다.

## 필드 소유권

| 의미 | 유일한 소유자 | fallback/파생 | 금지 규칙 |
| --- | --- | --- | --- |
| 대여소 ID·주소 | `bike_station_master` | 없음 | `stationName`을 주소로 복제 금지 |
| 대여소 표시명·정원·현재 존재 | `bike_station_realtime` | 없음 | master `ADDR1/ADDR2`를 표시명으로 승격 금지 |
| 대여소 Point | master `LAT/LOT` | 무효일 때만 realtime 좌표 | 두 좌표 평균·무조건 realtime 덮어쓰기 금지 |
| 날씨 격자 | station Point의 공통 LCC 변환 | 없음 | 자치구/행정동으로 기상 조인 금지 |
| 배차 센터 | `dispatch_center` seed + DB materialized station FK | Point/seed 변경 시 일괄 재계산 | API·배치별 최근접 재계산 금지 |
| 현재 재고 | realtime `parkingBikeTotCnt` | 없음 | 과거 관측이 최신 행 덮어쓰기 금지 |
| 시간별 날씨 | resolver가 선택한 source 행 전체 | 초단기 우선, 단기 fallback | 제품별 독립 upsert·필드별 혼합 금지 |
| 문화행사 ID | canonical identity hash | source-qualified prefix | 제목 단독·무구분 문자열 해시 금지 |
| 공연행사 ID | `SCH_SEQ` | source-qualified prefix | 시설 코드나 제목을 행사 ID로 사용 금지 |
| 행사 Point | 문화 원천 좌표 / 공연 좌표 seed | 없음 | Point 없는 event Gold 게시 금지 |
| 수요 예측 | inference batch | 수량은 반올림 | 이전 batch와 horizon 혼합 금지 |
| 긴급도 판단 | urgency batch | 예측+재고에서 파생 | API 요청 시 재계산 금지 |
| route 작업 수량 | route producer가 읽는 urgency Silver의 `bike_qty` | dispatched 수량 netting | `station_urgency` RDS에 중복 저장 금지 |
| urgency→route 작업 | `retrieval_needed→pickup`, `supply_needed→dropoff` | `normal`·`bike_qty<=0` 제외 | 방향 반전·normal route 생성 금지 |
| route 상태 일시 | API 상태 전이 | DB 제약/trigger | publisher가 실행 완료 이력 덮어쓰기 금지 |

## 테이블별 게시·stale 계약

### `weather_grid`, `dispatch_center`

- seed 전체를 staging에서 검증하고 한 transaction으로 게시한다.
- 이미 참조 중인 ID를 이름이나 좌표 순번 변화만으로 재사용하지 않는다.
- 센터 삭제는 참조 station/route가 있으면 금지하고 `is_active=false`로 전환한다.
- seed 변경과 station FK 재배정은 한 릴리스 단위로 검증한다.

### `station`

- 완전 master와 완전 realtime snapshot을 `sta_id`로 결합한다.
- ID·주소·유효 Point, 표시명, `hold_cnt>0`, 존재하는 weather grid와 dispatch center를
  모두 갖춘 행만 처음 게시한다.
- master Point를 우선하고 realtime Point는 명시적 fallback으로만 사용하며
  `sta_point_source_cd`에 남긴다.
- 한 번 게시된 station이 일시적인 source 누락으로 삭제되지 않게 한다. 서로 다른 완전
  realtime window 세 개에서 연속 미관측 또는 serving-invalid일 때 `is_active=false`로
  바꾸고 route FK를 위해 행은 보존한다. 같은 window correction은 중복 횟수가 아니며
  PARTIAL·실패는 세지 않는다.
- master 누락·invalid에는 last-known-good 주소/Point를 유지한다. source 좌표 또는 기존
  Point 대비 geography 거리 100m 초과 relocation은 자동 덮지 않고 review 후 correction
  revision과 immutable relocation approval artifact로만 반영한다. 정확히 100m는 허용한다.
- 최신 authoritative realtime window 최대 3개와 prior station projection을
  [publication-contract-v1.md](publication-contract-v1.md)의 immutable 입력으로 남겨
  3-window 상태와 LKG 판단을 재현한다.
- `gu`·`dong` 컬럼은 생성하지 않는다.

### `station_stock`

- 대여소별 최신 한 행만 둔다.
- 과거 logical time은 no-op이고 새 logical time은 `incoming.base_dttm > current.base_dttm`일
  때만 갱신한다. 같은 logical time·revision은 no-op이다. 같은 logical time의 더 큰
  correction revision을 `claim_publication()`이 승인한 경우에만 equal-base 행을
  authoritative correction 값으로 교체한다.
- 완전 realtime snapshot의 모든 게시 가능 station을 한 transaction에서 upsert한다.
- 이력과 snapshot 원문은 Silver가 소유한다.

Authoritative realtime window는 `station`과 `station_stock` 두 publication key를 문자열
순서로 claim하고 한 transaction에서 게시한다. 관측된 station의 `last_seen_dttm`과 같은
행의 stock `base_dttm`은 그 window로 같아야 한다. parking 값이 없어 stock을 만들 수 없는
station은 identity가 active여도 이번 현재 목록에서는 제외되며, 이름·정원만 먼저 새 window,
재고만 이전 window인 혼합 commit은 허용하지 않는다. master-only correction은 station key만
claim하고 realtime `last_seen_dttm`을 임의로 전진시키지 않는다.

### `station_demand_forecast`

현행 운영 추론은 모델 지원 전체 station에 12시간 horizon을 생성한다
([inference_task.py](../../airflow/orchestration/inference_task.py#L25-L37)). 게시자는 다음을
검증한다.

- 한 파일에 `base_dttm` 하나만 존재한다.
- 기대 집합은 publication 직전 잠근 `active Gold station ∩ rental model category ∩ return
  model category`이고, model version·지원 ID digest가 manifest와 같다.
- 각 station의 원천 `horizon=1..12`와 target이 `base+(h-1)시간`인지 확인한 뒤 Gold
  `predicted_dttm=base+h시간`으로 바꾼다.
- `(sta_id,predicted_dttm)` 중복이 없고 정확히 `base+1..12시간`이다.
- rent/rtn 예측 float64가 finite·0 이상인지 검사하고 IEEE-754 `roundTiesToEven`으로
  정수화한다. 이는 Python `round(x)`와 같고 PostgreSQL `round(numeric)`의 half-away-from-zero를
  대신 쓰지 않는다. publisher version과 urgency도 같은 규칙을 사용한다.
- expected/actual count, failed station 0, 두 모델·입력 artifact checksum을 success manifest가
  증명한다.

현 CLI는 partial parquet을 canonical key에 먼저 쓴 뒤 실패 sidecar를 남기므로 운영 게시
전에 unique temporary key→검증→success manifest last-write 방식으로 바꿔야 한다. 검증 후
transaction에서 기존 projection 전체를 새 snapshot으로 교체한다. stale/same/correction은
publication state로 판정하고 부분 upsert는 금지한다. 예측 이력은
`predictions/...` S3에 남고, RDS는 최신 완전 snapshot만 가진다. 원천 컬럼 변환은
`station_id→sta_id`, `date/hour/minute→predicted_dttm`,
`roundTiesToEven(rental_pred_mean)→predicted_rent_cnt`,
`roundTiesToEven(return_pred_mean)→predicted_rtn_cnt`다
([loader/transform.py](../../loader/transform.py#L338-L360)).

### `weather_forecast`

- resolver 하나가 두 제품을 함께 읽고 `weather_grid_id,forecast_dttm`당 승자 한 행만
  만든다.
- 활성 station의 distinct 격자와 다음 13개 정각의 완전 기대 키 집합을 staging에서
  검사한 뒤 selection과 publication을 같은 transaction으로 수행한다.
- topology shared lock을 얻은 뒤 활성 격자 집합과 입력 manifest 두 개가 여전히
  최신인지 재검증한다. 13번째 시각은 다음 정각 직후에도 API 미래 12행을 유지하는
  rollover buffer다.
- 동일 publisher run의 행 일부만 반영하지 않는다.
- 지난 발표·제품별 원문·정시 밖 자료는 Silver에 유지한다.

### `event`

- `cultural_event`와 `performance_event`를 source별 staging으로 분리 검증한다.
- 표시 가능한 이름·날짜·유효 Point가 있는 현재·예정 행사만 게시한다.
- source별 완전 snapshot일 때만 `(event_source_cd,source_event_id)`를 reconcile한다.
  `PARTIAL`은 upsert·삭제 근거로 사용하지 않는다.
- `last_seen_dttm`은 완전 snapshot 관측시각이며, KST 오늘보다 종료일이 과거인 행은
  같은 reconcile에서 정리한다.
- API는 `now-36시간 <= last_seen_dttm <= now+5분`인 source 행만 노출한다. 일일 수집이
  36시간 넘게 멈추면 오래된 취소·변경 전 행을 LKG라는 이름으로 무기한 서빙하지 않는다.
- 원천 payload, Point 없는 행사, 파싱 불가 가격표는 Silver/serving-rejection 지표에
  남긴다. `event_spot` FK는 없다.

### `station_urgency`

- 목표 urgency 계산은 topology shared lock에서 Gold `station WHERE is_active`의 ID·정원·
  Point·센터 배정을 읽고, S3에서는 exact-anchor 재고 이력과 완전 prediction snapshot을
  읽는다. 현재 `_known_station_ids()`는 구 `stations` 전체를 active 필터 없이 읽고,
  S3 realtime 좌표가 null이면 station history를 버리므로 목표 계약과 다르다
  ([urgency.py](../../rebalance/urgency.py#L214-L265),
  [reader.py](../../rebalance/reader.py#L58-L83)).
- 재고 입력은 기존 `read_recent_stock(anchor, lookback_minutes=25)`의 시간 의미를 그대로
  고정한다. `stock_history_manifest_01..05`는 각각 anchor-25·20·15·10·5분의
  authoritative source snapshot이고 oldest→newest 순서다. 현재 anchor는 이미 commit된
  `stock_publication_manifest`가 소유한다. `stock_window_count="6"`은 과거 5개와 현재
  1개를 합친 전체 계산 window 수이며, `scoring_config_version`은
  `urgency-scoring-v1`이다. source snapshot 전체가 완전하다면 신규 station이 과거 일부
  window에 없을 수는 있지만, 과거 window manifest 자체의 누락·PARTIAL·순서 교환은
  허용하지 않는다.
- 기대 집합은 `active station ∩ anchor와 정확히 같은 current stock ID ∩ 성공 prediction
  support ID`다. 모든 기대 ID가 한 번씩 만들어졌는지 확인한 뒤 최신 projection 전체를
  교체한다.
- publisher는 같은 anchor의 `station_stock` publication이 Gold에 commit됐는지 확인한다.
  S3 current만 있고 Gold stock이 실패한 anchor의 urgency는 게시하지 않는다.
- stock/prediction artifact digest, station publication dependency, 기대·실제 ID digest와 row count를
  success manifest에 남기고 stale/same/correction은 publication state로 판정한다.
- 과거 logical time과 exact same version은 no-op이다. 같은 anchor의 더 큰 correction
  revision은 corrected stock/prediction 결과로 projection 전체를 다시 교체한다.
- RDS에는 API가 읽는 score, critical minutes, need code만 둔다. route 수량 계산용
  `bike_qty`는 동일 urgency parquet에만 남긴다.

### `rebalance_route`, `rebalance_route_stop`

route producer는 urgency parquet의 `bike_qty`, Gold `station.dispatch_center_id`, 아직
입력 재고에 반영되지 않은 route stop의 netting 수량을 읽는다. coverage는 모든
`dispatched`와 `completed_dttm > urgency가 사용한 stock anchor`인 completed route다.
completion 이후의 stock snapshot이 확인되면 그 completed route는 coverage에서 빠진다.
현 코드는 dispatched만 제외하므로([routes.py](../../rebalance/routes.py#L47-L84]) 이를
고쳐야 한다. 목표 전환에서는
`core.regions.nearest_region` 대신 station FK를 사용한다.
urgency 판단의 `retrieval_needed`는 차량 `pickup`, `supply_needed`는 `dropoff`로만
변환한다. `normal` 또는 `bike_qty<=0` 행은 route 후보에서 제외한다.

한 계산 batch의 헤더와 stop은 하나의 aggregate다.

1. route·stop 두 산출물의 route ID 집합, stop 최소 1개, 연속 `visit_no`, active station,
   header와 같은 station center, action, 양수 수량을 staging에서 검증한다.
2. 차량 초기 적재량은 0이다. visit 순서대로 pickup은 더하고 dropoff는 빼며 매 단계
   running load가 `0..TRUCK_CAPACITY`인지 검증한다. 마지막 양수 잔량은 다음 cycle용으로
   허용한다. 현 producer의 `capacity - picked` dropoff 한도는 음수 적재 경로를 만들 수
   있으므로 `dropoff 합계 <= picked 합계`로 수정한다.
3. 한 transaction에서 기존 `proposed` route만 삭제하고 cascade로 stop을 지운다.
4. 새 헤더와 모든 stop을 함께 삽입한다. 빈 batch도 유효하며 기존 proposed를 비운다.
5. `dispatched`, `completed` 이력은 publisher가 수정·삭제하지 않는다.
6. API 상태 전이는 `proposed→dispatched→completed`와 일시를 지킨다.

현재 producer도 헤더와 stop을 별도 S3 객체로 쓴다
([routes_main.py](../../rebalance/routes_main.py#L29-L45)). RDS loader는 두 객체를 읽더라도
반드시 [publication-contract-v1.md](publication-contract-v1.md)의 두 output artifact와
station/demand/stock/urgency dependency, route coverage, truck capacity parameter를 가진 성공 manifest
하나로 묶는다. route ID는 같은
logical/revision·정렬 센터·route ordinal의 UUIDv5다. namespace는
`d0d59897-9e72-541f-bb05-bd3d113c2639`로 고정한다. UUID name은 `publication_key`, UTC
logical time, integer revision, `dispatch_center_id`, 1-based `route_ordinal`을 위 공통
규칙으로 직렬화한 JSON bytes다. center는 ID 오름차순, pickup/dropoff 후보는 각각
`(urgency_score DESC, sta_id ASC)`, 최근접 선택은 `(distance, sta_id ASC)`로 동률을 깨고
그 결과의 생성 순서로 center 내 ordinal을 부여한다. publisher는 topology shared→route
operation lock 안에서 publication dependency와 coverage를 재계산하고 다르면
reject/recompute한다.
route가 직접 고정한 station·demand·stock state와 urgency publication input의 동명
dependency도 정확히 같아야 한다. 새 stock/demand/station correction 뒤 urgency가 아직
재게시되지 않았으면 기존 urgency output으로 route를 만들지 않는다.
route INSERT/UPDATE/DELETE와 stop mutation은 BEFORE STATEMENT trigger가 row lock보다 먼저
같은 순서의 advisory lock을 잡는다. API dispatch도 shared topology 안에서 활성 센터와
모든 active/same-center stop을 다시 확인한다. station/센터 topology transaction은 첫
DML 전에 exclusive topology helper를 호출하거나 topology statement를 먼저 실행한 뒤,
영향 proposed aggregate를 같은 transaction에서 정리한다. row trigger에서 tuple lock 뒤
advisory lock을 얻는 구현은 교착 위험 때문에 금지한다.

## Gold 소비 계약

| 소비자 | 읽는 Gold | 제공 값·alias | 목표 변경 |
| --- | --- | --- | --- |
| `GET /stations` | `station` + `station_stock` + `dispatch_center` | Point→`lat/lon`, 센터명→`region`, 재고·정원→`shared_rate` | `gu` 제거, 활성·신선 재고만 반환 |
| `GET /stations/{sta_id}` | 위와 동일 | 주소 추가 | `gu` 제거 |
| `GET /stations/{sta_id}/forecast` | `station` + `station_stock` + `station_demand_forecast` | `predicted_rtn_cnt→predicted_return_cnt`; `predicted_bikes`·action은 API 파생 | 모델 미지원은 404, fresh stock 없으면 503, 정확히 future 12행 |
| `GET /status` | `station_demand_forecast` | 공통 `base_dttm` | projection이 없으면 `503 forecast_not_ready` |
| `GET /stations/{sta_id}/events` | `station` + `event` | Point 거리→2자리 `distance_km`; API config→`radius_km`; 이름·장소·날짜 alias | missing/inactive station 404; KST 오늘·36시간 freshness |
| `GET /stations/{sta_id}/weather?hours=12` | `station.weather_grid_id` + `weather_forecast` | 정시별 기온·강수·습도·풍속·상태 | 미래 12행 미만 또는 `min(updated_dttm)`이 now-45분..now+5분 밖이면 503; 실황·source lineage 미노출 |
| `GET /regions` | `dispatch_center` | 센터명→`region`, Point→`lat/lon` | Python 상수 제거, 활성 센터만 반환 |
| `GET /alerts` | `station_urgency` + same-anchor `station_stock` + active `station` + active `dispatch_center` | need code→`action_type`, critical minutes alias | stock과 urgency base 일치; `(urgency_score DESC, sta_id ASC)` |
| route 조회·상태 변경 API | `rebalance_route` + `rebalance_route_stop` + `station` + `dispatch_center` | UUID→문자열, 표준 컬럼→기존 응답 alias | 같은 snapshot; `(proposed_dttm DESC, route_id ASC)` 페이지 정렬 |
| route producer | `station` + `dispatch_center` + dispatched route/stop | 센터 배정, 이미 처리 중인 수량 | station FK를 SSOT로 사용 |

`station.is_active`는 검증된 대여소 identity가 현재 운영 중이라는 뜻이지 모든 5분 tick에
재고가 있다는 뜻은 아니다. `/stations`와 상세는
`station.last_seen_dttm = station_stock.base_dttm`이고
`now-10분 <= station_stock.base_dttm <= now+5분`인 stock을 inner join해 현재
서빙 가능한 station만 노출한다. forecast는 해당 station이 model support에 없으면 404,
현재 양방향 freshness 범위의 stock이 없거나 stock `base_dttm`이 demand batch `base_dttm`과 정확히 같지
않으면 `503 stock_forecast_not_aligned`를 반환하며 0이나 다른 anchor 결과로 대체하지
않는다. demand base 자체도 `now-10분..now+5분` 범위를 벗어나면 forecast와 `/status` 모두
`503 forecast_not_ready`다. demand publisher의 기대 집합은 identity/model support 기준이고, API의 누적재고
계산은 같은 anchor stock을 별도 전제로 한다. station 선택 요청을 시작할 때 Web은 이전
station의 forecast를 즉시 지우고, 404는 “예측 미지원”, 503은 “갱신 중”으로 구분한다.
`/status` 실패 때도 이전 성공 시각을 현재값처럼 남기지 않는다.

예측 재고는 [core/forecast.py](../../libs/core/src/core/forecast.py)의 공통 함수가 같은 anchor
stock에서 정시 순으로 `max(0, 이전 재고 + 예상 반납 - 예상 대여)`를 누적한다. 정원 상한은
두지 않고 각 정수 결과를 `points[].predicted_bikes`로 반환한다. DB 컬럼은 아니다.
`SUPPLY_LOW_STOCK_RATIO * hold_cnt` 이하이면 `supply_needed`, `hold_cnt` 이상이면
`retrieval_needed`, 나머지는 `normal`이다. ratio는
[scoring_config.py](../../libs/core/src/core/scoring_config.py)의 versioned config를 API와
urgency가 함께 사용한다.

`/alerts`는 urgency와 `station_stock`을 같은 station·같은 `base_dttm`으로 inner join하고
두 base 모두 freshness 범위인지 확인한다. 또한 `urgency.updated_dttm >=
station_stock.updated_dttm` 및 `urgency.updated_dttm >= station.updated_dttm`이어야 한다.
같은 anchor stock correction이나 station topology correction 뒤 urgency가 재게시되기 전에는
옛 판단을 숨긴다. 따라서 `/stations`에 없는 stale-stock station을 alert 첫 행으로 선택하지
않는다. 결과는 `urgency_score DESC, sta_id ASC`의 결정적 순서로 반환한다. route 목록은 선택적 `region`과
`proposed|dispatched|completed|cancelled` status만 허용하고 기본 `limit=100`, 최대 `500`,
`offset>=0`의 bounded pagination 및 `(proposed_dttm DESC, route_id ASC)` 정렬을 사용한다.
상태 전이 API는 없는 route를 404, 현재 상태가 요청의 expected status와 다르면 409로
반환한다. `UPDATE ... WHERE route_id=:id AND status=:expected RETURNING`과 동일 transaction의
aggregate 재조회로 guarded update와 응답 snapshot을 묶고 DB trigger 예외를 500으로
그대로 노출하지 않는다. FastAPI path parameter는 `UUID`로 선언해 malformed ID를 DB cast
전에 422로 거부하며 응답 `route_id`는 계속 JSON 문자열이다.

Web 폴링은 오류를 성공 데이터처럼 붙들지 않는다. `/stations` 또는 `/alerts` 요청이
실패하면 이전 배열을 지우고 갱신 실패 상태를 표시하며, 새 station 목록에 기존 선택 ID가
없으면 선택·forecast·detail을 함께 해제한다. station forecast·detail·events·weather는
선택 변경 직후 이전 값을 지우고 주기적으로 재조회하며, 404/503/네트워크 실패나 freshness
만료에는 이전 station 결과를 현재값처럼 남기지 않는다. 지도 검색 원은 행사 Point가 아니라 검색에
사용한 선택 station Point를 중심으로 `radius_km`를 그린다.

`EventsResponse.radius_km`는 Web이 지도 검색 원을 그리는 실제 소비값이므로 유지한다. DB
컬럼이 아니라 API의 단일 반경 config를 쿼리와 응답에 함께 사용한 파생값이다. 반대로
`ForecastResponse.reasons`는 항상 빈 배열이고 생산 파이프라인이 없으므로 목표 API,
Pydantic, TypeScript와 DetailPanel에서 함께 제거한다.

현재 API가 대여소·재고, 예측, 알림, route, 행사를 각각 어떻게 읽는지는
[queries.py](../../apps/api/queries.py#L47-L299), Web이 실제 폴링하고 필터링하는 계약은
[api.ts](../../apps/web/src/api.ts#L74-L90)과 [App.tsx](../../apps/web/src/App.tsx#L49-L108)에
나타난다. 행사 클릭은 Point를 즉시 지도 중심으로 사용하므로
([DetailPanel.tsx](../../apps/web/src/components/DetailPanel.tsx#L146-L171)) `event_point NOT
NULL`은 서빙 필수 조건이다. 날씨 탭은 현재 placeholder지만
([DetailPanel.tsx](../../apps/web/src/components/DetailPanel.tsx#L177-L181)), #129에서는
1시간 간격 선택 예보만 연결한다.

## Silver·quarantine 소유 경계

| 데이터 | 영구/이력 소유 | Gold에는 무엇만 남는가 |
| --- | --- | --- |
| 원천 응답과 모든 발표 revision | Bronze/Silver | 없음 |
| 대여·반납 트립 | Silver | 없음 |
| 대여소 master/realtime 과거 snapshot | Silver | 게시 가능 station 속성과 최신 stock |
| 단기·초단기 제품별 예보 | Silver | 정시별 선택 결과 한 행 |
| 초단기실황 | Silver | 없음 |
| 생활인구·행정동 코드·POI·정규화 인구 | Silver | 없음 |
| Point 없는 행사·원천 부가필드 | Silver + serving-rejection 지표 | Point 있는 현재·예정 최소 필드 |
| 모델 피처·예측 이력 | Silver/predictions | 최신 완전 예측 snapshot |
| urgency 계산의 `bike_qty`, lat/lon | urgency parquet | API용 최신 score/판단만 |
| 과거 route 제안 batch | routes parquet | 최신 proposed + 실행 상태 이력 |
| schema/type/range 정책 폐기 행 | Collector quarantine | 없음 |

Gold에서 제외했다는 것은 폐기가 아니라 저장 책임이 Silver에 있다는 뜻이다. 반대로
Gold의 행이 완전 snapshot으로 교체됐더라도 원천 이력 삭제 권한을 뜻하지 않는다.

## 구현 전환 차단 목록

다음 항목이 해결되기 전에는 이 매핑을 운영 DB에 게시하지 않는다.

1. `bikeList` probe pagination과 실제 전체 cardinality 검증을 구현한다.
   `rackTotCnt`·`parkingBikeTotCnt`의 200 상한과 realtime/cultural의 좁은 좌표 outlier
   범위를 제거하고 원문 보존+DDL 안전 box+품질 지표로 처리한다.
2. loader가 manifest 자격을 확인하지 않고 Silver 파일만 읽는 경로
   ([reader.py](../../loader/reader.py#L50-L65))를 authoritative snapshot publisher로
   교체하고 `gold_meta.claim_publication()`의 권한·transaction 계약을 적용한다.
3. collector·inference·urgency·route producer를 revision/content-addressed immutable
   객체(또는 고정 bucket version ID)→검증→success manifest last-write로 바꾸고, 같은 URI
   overwrite와 partial canonical parquet 게시를 없앤다.
4. 단기·초단기 독립 upsert를 단일 weather resolver, 13시간 buffer와 원자 publisher로
   바꾼다.
5. station publisher를 master/realtime 결합으로 바꾸고 주소·Point LKG·
   활성화 dependency 계약을 적용한다. realtime `stationName` 결손 행의 ID를 Silver에
   보존해 3-window serving-invalid 판정을 가능하게 한다.
6. `gu`와 미사용 event 필드의 DB/API/Pydantic/TypeScript 계약을 함께 제거하고 weather
   endpoint, forecast 404/503, `/status` 503 및 Web stale-state clear를 구현한다.
7. [dispatch-center-seed.yaml](dispatch-center-seed.yaml)과
   `station.dispatch_center_id`를 먼저 게시하고 API·route의 `core.regions` 계산을 제거한다.
8. event를 Point 필수 평탄 테이블로 바꾸고 source별 snapshot reconciliation을 적용한다.
9. urgency가 Gold active station의 정원·Point·센터를 SSOT로 읽도록 구
   `_known_station_ids`와 optional realtime 좌표 필터를 교체한다.
10. route producer의 dropoff 한도를 `picked` 이하로 고치고, anchor 이후 completed까지
    포함한 netting, [publication-contract-v1.md](publication-contract-v1.md)의 결정적 UUID·
    coverage와 statement-level lock 계약을 구현한다.
11. 현 `postgres:16` Compose·매 기동 `002/003` init·구 `seed_gold.py`를 후속 전환 PR에서
    PostGIS clean baseline/manifest fixture로 교체한다. 기존 volume은 자동 삭제하지 않는다.
12. 격리된 PostGIS에서 DDL, seed, 정상 게시, stale/correction/미래시각 거부, partial 롤백,
    공간 조회, route 상태 전이와 2-session lock 경쟁을 검증한다.

이 목록은 구현 순서를 정하는 의존성이지 운영 배포 지시가 아니다. #129 설계 작업에서는
ERD·DDL·데이터 사전·이 매핑의 합치와 격리 검증까지만 수행한다.
