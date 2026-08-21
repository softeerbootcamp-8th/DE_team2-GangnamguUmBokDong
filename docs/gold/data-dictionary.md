# Gold 데이터 사전

## 적용 원칙

이 문서는 대시보드 API와 재배치 운영이 직접 읽고 쓰는 `public` Gold RDS 10개
서빙 테이블과 `gold_meta` publication 제어 테이블 하나의 표준 이름과 의미를 정의한다.
실행 가능한 타입·키·제약의 기준은
`target-schema.sql`이다. 원천 재현과 학습용 이력은 S3 Bronze/Silver가 소유한다.

- 테이블명은 단수형이다.
- 날짜는 `_dt`, 일시는 `_dttm`이다.
- 모든 일시는 유한한 `TIMESTAMPTZ` UTC instant이고, PostgreSQL `infinity`는 거부한다.
  행사 날짜와 센터 좌표 검증일처럼
  시각이 없는 업무 달력 값만 유한한 `DATE`다.
- 식별자는 `_id`, 명칭은 `_nm`, 번호는 `_no`, 수량은 `_cnt`, 코드는 `_cd`다.
- Point의 SRID는 4326이며 X=경도, Y=위도다.
- `created_dttm`, `updated_dttm`은 DB가 관리하며 UPDATE로 생성 일시를 바꿀 수 없다.
- 화면에 제공할 수 없는 불완전 행은 Gold에 억지로 넣지 않고 Silver/quarantine에 둔다.
- de-project ERD는 표준 단어와 검증된 관계를 참고하되 Gold 테이블 범위를 결정하지 않는다.

자치구·행정동, 날씨 실황, 행사 장소 마스터, 대여·반납 이력, 인구 이력은 현재
대시보드 소비 계약이 없으므로 Gold 대상이 아니다.

## 공통 표준 단어

| 단어 | 의미 | 사용하지 않는 이름 |
| --- | --- | --- |
| `sta` | 따릉이 대여소 | 컬럼 접두어 `station` |
| `rtn` | 반납 | `return` |
| `rent` | 대여 | 컬럼 접두어 `rental` |
| `base` | 관측·발표·계산 batch의 논리 기준 | 실행시각·대상시각과 혼용 |
| `predicted` | 수요 모델의 예측 대상 | 기상 `forecast`와 혼용 |
| `forecast` | 기상 예보 대상 | 수요 `predicted`와 혼용 |
| `precipitation` | 강수 | `rainfall`, `precip` |
| `dispatch_center` | 재배치 라우팅 기준점 | 모호한 `region` DB 컬럼 |
| `rebalance_need_type` | 대여소 공급·회수 판단 | route 작업 `action`과 혼용 |
| `route_action_type` | 차량의 pickup·dropoff 작업 | urgency 판단 코드와 혼용 |
| `route_status` | 재배치 경로 상태 | 단독 `status` |
| `publication` | 검증된 projection 한 version의 원자 게시 | collector 실행·행별 upsert와 혼용 |
| `revision` | 같은 publication logical time의 명시적 교정 번호 | upstream revision의 임의 max |

## 시간 컬럼

| 컬럼 | 의미 |
| --- | --- |
| `base_dttm` | 재고 관측, 수요예측 batch, 기상 발표 또는 긴급도 계산 기준 일시 |
| `master_base_dttm` | station 속성에 사용한 master snapshot 기준 일시 |
| `last_seen_dttm` | 검증된 완전 source snapshot에서 마지막으로 확인한 일시 |
| `predicted_dttm` | 직전 1시간 수요 구간을 누적 적용한 종료 일시 |
| `forecast_dttm` | 대시보드가 표시할 정시 기상 대상 일시 |
| `proposed_dttm` | 재배치 경로가 제안된 일시 |
| `dispatched_dttm` | 운영자가 경로 실행을 확정한 일시 |
| `completed_dttm` | 경로 실행이 완료된 일시 |
| `logical_dttm` | publication의 최신성 비교 기준이 되는 논리 시각 |
| `created_dttm` | DB 행이 처음 생성된 일시 |
| `updated_dttm` | DB 행이 마지막으로 변경된 일시 |

publisher와 API DB session은 UTC를 사용한다. 행사 유효일 판정은
`(now() AT TIME ZONE 'Asia/Seoul')::date`로 수행한다.
publication logical time과 관측·계산·발표·제안·last-seen 시각은 DB 현재시각보다 5분을
넘게 미래일 수 없다. API freshness도 과거 cutoff뿐 아니라 `now+5분` 상한을 적용한다.

## gold_meta.publication_state

API가 읽는 도메인 테이블이 아니라 publication key별 마지막 commit version을 기억하는
제어 테이블이다. 정상적인 EMPTY 뒤에도 watermark가 남아 stale artifact가 과거 행을
되살리지 못하게 한다.

| 컬럼 | 타입 | Null/키 | 원천·변환 | 의미 |
| --- | --- | --- | --- | --- |
| `publication_key` | `TEXT` | PK, NOT NULL | 고정 publication registry | 원자 게시 단위 |
| `logical_dttm` | `TIMESTAMPTZ` | NOT NULL | key별 manifest anchor | 최신성 비교 시각 |
| `revision_no` | `INTEGER` | NOT NULL, `>=0` | publisher correction ordinal | 같은 logical time의 교정 번호 |
| `manifest_uri` | `TEXT` | NOT NULL, nonblank | immutable success manifest | 게시 근거 위치 |
| `artifact_set_sha256` | `TEXT` | NOT NULL, lowercase SHA-256 | 정렬한 출력 객체 목록 | 출력 artifact 집합 fingerprint |
| `input_fingerprint_sha256` | `TEXT` | NOT NULL, lowercase SHA-256 | 입력·설정·의존상태 canonical 문서 | 계산 입력 fingerprint |
| `published_row_cnt` | `BIGINT` | NOT NULL, `>=0` | manifest | 해당 publication의 주 target 행 수 |
| `created_dttm` | `TIMESTAMPTZ` | NOT NULL | DB default | 최초 게시 일시 |
| `updated_dttm` | `TIMESTAMPTZ` | NOT NULL | DB trigger | 마지막 교정·게시 일시 |

허용 `publication_key`는 `weather_grid`, `dispatch_center`, `station`, `station_stock`,
`station_demand_forecast`, `weather_forecast`, `event:cultural_event`,
`event:performance_event`, `station_urgency`, `rebalance_route`다. 마지막 key는 header와 stop
aggregate 하나를 소유하며 row count는 proposed header 수다. route manifest에는 stop
수를 별도로 둔다.

event와 route는 정상 EMPTY를 허용한다. demand는 active∩두 model support, weather는 active
grid, urgency는 계산 가능 기대 집합이 0임을 각각 잠근 topology/upstream manifest가
증명한 경우에만 row count 0을 허용한다.

`artifact_set_sha256`과 `input_fingerprint_sha256`의 정확한 JSON key·배열 정렬·UTF-8 bytes,
Point EWKB와 SHA 회귀값은
[publication-contract-v1.md](publication-contract-v1.md)의 versioned byte contract를 따른다.
topology·support는 임의 JSON이 아니라 잠근 선행 publication state tuple로 식별한다.

`gold_meta.claim_publication()`은 key별 advisory transaction lock 뒤 상태를 잠근다. 더
오래된 version은 false, 같은 version·같은 fingerprint는 false(no-op), 같은 version인데
내용이 다르면 오류, 같은 logical time의 큰 revision 또는 더 최신 logical time은 true다.
target mutation과 claim은 같은 transaction에 있어야 한다. key 변경, version 퇴행·동일
revision 직접 UPDATE와 DELETE는 trigger가 거부한다. API role에는 schema 권한을 주지 않고
publisher role에 claim 함수 실행과 필요한 최소 target DML만 부여한다.

여러 target을 함께 바꾸면 관련 publication key를 문자열 순으로 모두 claim한다. dependency
state tuple과 coverage artifact의 byte 범위는
[publication-contract-v1.md](publication-contract-v1.md)를 따른다. DML advisory lock은
BEFORE STATEMENT에서 row lock보다 먼저 얻는다.

## weather_grid

station과 시간별 날씨가 공유하는 기상청 격자 한 개가 한 행이다.

| 컬럼 | 타입 | Null/키 | 원천·변환 | 의미 |
| --- | --- | --- | --- | --- |
| `weather_grid_id` | `TEXT` | PK, NOT NULL | `x || '_' || y` | 안정적인 격자 ID |
| `weather_grid_x_no` | `SMALLINT` | UK(X/Y), NOT NULL | KMA `nx` | LCC X 격자 번호 |
| `weather_grid_y_no` | `SMALLINT` | UK(X/Y), NOT NULL | KMA `ny` | LCC Y 격자 번호 |
| `created_dttm` | `TIMESTAMPTZ` | NOT NULL | DB default | 행 생성 일시 |
| `updated_dttm` | `TIMESTAMPTZ` | NOT NULL | DB trigger | 행 변경 일시 |

현재 collector 설정에 등장하는 34개 `(nx, ny)`를 seed한다. 화면이 읽지 않는 격자
Polygon과 대표 Point는 저장하지 않는다. X/Y는 양수이고 ID는 정확히 `{x}_{y}`다.

## dispatch_center

대시보드 지역 필터와 라우팅에 사용하는 기준점 한 개가 한 행이다. 공식 관할구역이 아니다.

| 컬럼 | 타입 | Null/키 | 원천·변환 | 의미 |
| --- | --- | --- | --- | --- |
| `dispatch_center_id` | `TEXT` | PK, NOT NULL | 고정 영문 slug seed | 센터 ID |
| `dispatch_center_nm` | `TEXT` | UK, NOT NULL | 현 `core.regions` 명칭 | 화면 센터명 |
| `dispatch_center_point` | `geometry(Point,4326)` | NOT NULL | 검수된 seed | 라우팅 기준 위치 |
| `location_accuracy_cd` | `TEXT` | NOT NULL | seed manifest | 좌표 정확도 |
| `location_source_desc` | `TEXT` | NOT NULL | seed manifest | 좌표 조사 근거 |
| `location_verified_dt` | `DATE` | NULL | 수동 검증일 | 마지막 검증일 |
| `is_active` | `BOOLEAN` | NOT NULL, default true | 운영 설정 | 신규 배정 사용 여부 |
| `created_dttm` | `TIMESTAMPTZ` | NOT NULL | DB default | 행 생성 일시 |
| `updated_dttm` | `TIMESTAMPTZ` | NOT NULL | DB trigger | 행 변경 일시 |

`location_accuracy_cd`는 `verified_site`, `landmark_approximation`,
`administrative_centroid` 중 하나다. 활성 station은 활성 센터만 참조한다. 센터를
비활성화할 때 영향 station을 같은 transaction에서 먼저 재배정하고 proposed route를
정리해야 한다. Point는 non-empty이며 안전 bounding box `lon 126.5..127.5`,
`lat 37.0..38.0`을 통과한다. 화면이 소비하지 않는 자치구 Polygon은 별도 필수 master로
두지 않는다.

최초 11행은 [dispatch-center-seed.yaml](dispatch-center-seed.yaml) `dispatch-center-v1`이다.
원천 commit/file hash와 EPSG·좌표 순서를 고정했지만 현장 측량값은 아니다. 10개는
`landmark_approximation`, 영남은 `administrative_centroid`이고 검증일은 null이다.

## station

서빙 품질 조건을 통과해 Gold에 발행된 대여소 한 개가 한 행이다.

| 컬럼 | 타입 | Null/키 | 원천·변환 | 의미 |
| --- | --- | --- | --- | --- |
| `sta_id` | `TEXT` | PK, NOT NULL | master `RNTLS_ID` | `ST-숫자` 대여소 ID |
| `sta_nm` | `TEXT` | NOT NULL | realtime `stationName` | 화면 대여소명 |
| `sta_addr` | `TEXT` | NOT NULL | master `ADDR1` | 화면 주소 |
| `hold_cnt` | `INTEGER` | NOT NULL, `>0` | realtime `rackTotCnt` | 거치 가능 수량 |
| `sta_point` | `geometry(Point,4326)` | NOT NULL | master 좌표, 조건부 realtime fallback | 대여소 위치 |
| `sta_point_source_cd` | `TEXT` | NOT NULL | 변환 결과 | 사용 좌표 원천 |
| `weather_grid_id` | `TEXT` | FK, NOT NULL | Point→KMA 공식 LCC 변환 | 시간별 날씨 조인 키 |
| `dispatch_center_id` | `TEXT` | FK, NOT NULL | 활성 센터 중 최근접 | 화면·route 공통 지역 키 |
| `master_base_dttm` | `TIMESTAMPTZ` | NOT NULL | master snapshot | master 기준 일시 |
| `last_seen_dttm` | `TIMESTAMPTZ` | NOT NULL | 완전 realtime snapshot | 마지막 관측 일시 |
| `is_active` | `BOOLEAN` | NOT NULL, default true | 검증된 부재 정책 | 현재 서빙 대상 여부 |
| `created_dttm` | `TIMESTAMPTZ` | NOT NULL | DB default | 최초 발행 일시 |
| `updated_dttm` | `TIMESTAMPTZ` | NOT NULL | DB trigger | 마지막 변경 일시 |

`sta_point_source_cd`는 `bike_station_master` 또는
`bike_station_realtime_fallback`이다. master의 주소·좌표와 realtime의 명칭·거치수량을
조합하고 필수값이 모두 있는 행만 발행한다. 센터 거리가 같으면
`dispatch_center_id` 오름차순으로 결정한다. station·center 좌표나 center 활성 상태가
바뀌면 영향 station 배정을 한 transaction에서 다시 계산한다.

신규 master-only 행은 realtime에서 확인될 때까지 Gold에 발행하지 않는다. 이미 발행한
station은 검증된 완전 realtime의 서로 다른 `window_start` 세 개에서 연속 미관측 또는
관측됐지만 이름이 비었거나 `rackTotCnt`가 null/0 이하인 serving-invalid일 때
비활성화하고, 유효하게 재등장하면 즉시 활성화한다. 같은 window correction은 판정만
교체하고 횟수를 늘리지 않으며 PARTIAL·실패 snapshot은 세지 않는다. Point는 DDL의
SRID·non-empty·안전 bounding box를 통과해야 하며 자치구 Polygon은 게시 조건이 아니다.
이 판정과 LKG는 [publication-contract-v1.md](publication-contract-v1.md)의 realtime
window-set과 prior station projection input으로 재현한다.

기존 station은 master 누락·주소/Point 오류에도 realtime이 serving-valid이면 마지막 검증
master 속성과 `master_base_dttm`을 유지한다. master/realtime 또는 기존 Gold/new master
Point의 geography 거리가 100m를 초과하면 자동 갱신하지 않고 review 뒤 correction
revision과 [publication-contract-v1.md](publication-contract-v1.md)의 immutable relocation
approval artifact로만 반영한다. 정확히 100m는 허용한다. 신규
station에는 유효한 master 주소가 필수다. inactive→active 때는 13시간 weather와, model
지원 station이면 같은 anchor demand coverage를 먼저 보장하거나 활성화를 미룬다.

## station_stock

대여소별 최신 재고 한 건이다. QR형 자전거 때문에 재고가 거치 수량보다 클 수 있어
상한 CHECK는 두지 않는다.

| 컬럼 | 타입 | Null/키 | 원천·변환 | 의미 |
| --- | --- | --- | --- | --- |
| `sta_id` | `TEXT` | PK/FK, NOT NULL | realtime `stationId` | 대여소 ID |
| `base_dttm` | `TIMESTAMPTZ` | NOT NULL | collection window | 관측 기준 일시 |
| `parking_bike_tot_cnt` | `INTEGER` | NOT NULL, `>=0` | 동명 원천 필드 | 이용 가능 자전거 수 |
| `created_dttm` | `TIMESTAMPTZ` | NOT NULL | DB default | 최초 생성 일시 |
| `updated_dttm` | `TIMESTAMPTZ` | NOT NULL | DB trigger | 최신 재고 반영 일시 |

일반 upsert는 `EXCLUDED.base_dttm > current.base_dttm`일 때만 갱신한다. 같은 logical
time의 더 큰 correction revision을 publication claim이 승인한 경우에만 equal-base 값을
교체하며 exact same version은 no-op이다. API는 active station과
`station.last_seen_dttm = base_dttm`, `now-10분 <= base_dttm <= now+5분`인 재고만 노출한다.
Authoritative realtime window는 station·stock 두 key를 함께 claim한 한 transaction에서
게시해 이름·정원과 재고가 서로 다른 window로 섞이지 않게 한다.

## station_demand_forecast

최신 완전 예측 snapshot의 대여소·대상시각 한 건이다.

| 컬럼 | 타입 | Null/키 | 원천·변환 | 의미 |
| --- | --- | --- | --- | --- |
| `base_dttm` | `TIMESTAMPTZ` | NOT NULL | inference batch | snapshot 기준 일시 |
| `sta_id` | `TEXT` | PK/FK, NOT NULL | prediction `station_id` | 대여소 ID |
| `predicted_dttm` | `TIMESTAMPTZ` | PK, NOT NULL | KST 입력→UTC | 예측 대상 일시 |
| `predicted_rent_cnt` | `INTEGER` | NOT NULL, `>=0` | float64 `roundTiesToEven(rental_pred_mean)` | 예상 대여 수 |
| `predicted_rtn_cnt` | `INTEGER` | NOT NULL, `>=0` | float64 `roundTiesToEven(return_pred_mean)` | 예상 반납 수 |
| `created_dttm` | `TIMESTAMPTZ` | NOT NULL | DB default | 행 생성 일시 |
| `updated_dttm` | `TIMESTAMPTZ` | NOT NULL | DB trigger | 행 변경 일시 |

PK는 `(sta_id, predicted_dttm)`이고 모든 행의 `base_dttm`은 같은 snapshot을 가리킨다.
기대 station은 `active Gold ∩ rental model support ∩ return model support`다. 원천 horizon
`h=1..12`는 `base+(h-1)시간` 구간 시작을 나타내지만 Gold는 누적 적용 종료인
`predicted_dttm=base+h시간`으로 바꾼다. 따라서 station마다 정확히 `base+1..12h` 12개,
`predicted_dttm > base_dttm`이어야 한다. producer는 두 model version·support digest,
expected/actual count·artifact checksum을 가진 성공 manifest를 마지막에 쓴다. publisher는
topology shared lock에서 완전한 최신 snapshot만 전 행 교체한다. model-supported station의
활성화는 이 projection에 포함하거나 다음 publication까지 미룬다.
p10/p50/p90과 입력 원천 등 설명·감사용 메타데이터는 S3 결과가 소유한다.
정수화는 finite·비음수 float64에 Python `round(x)`와 같은 IEEE-754 ties-to-even을 쓰며
PostgreSQL `round(numeric)`의 다른 tie 규칙을 섞지 않는다.
forecast API는 동일 station의 latest stock과 `base_dttm`이 정확히 같고 그 base가
`now-10분..now+5분`일 때만 누적 재고를 계산한다. 불일치·stale/future이면 partial chart
대신 503을 반환한다. active인데 row가 없는 station은 완전 projection 계약상 model
미지원이므로 404다.
같은 anchor stock에서 정시 순으로 `max(0, 이전+predicted_rtn_cnt-predicted_rent_cnt)`를
누적한 정수는 API `points[].predicted_bikes`다. 정원 상한은 두지 않고 DB 컬럼도 만들지
않는다.

## weather_forecast

격자·정시별로 resolver가 선택한 대시보드용 시간 날씨 한 건이다. 원천 발표 이력이 아니다.

| 컬럼 | 타입 | Null/키 | 원천·변환 | 의미 |
| --- | --- | --- | --- | --- |
| `weather_grid_id` | `TEXT` | PK/FK, NOT NULL | source `nx/ny` lookup | 격자 ID |
| `forecast_dttm` | `TIMESTAMPTZ` | PK, NOT NULL | `fcstDate/fcstTime` KST→UTC | 정시 대상 일시 |
| `source_product_cd` | `TEXT` | NOT NULL | resolver | 선택된 제품 lineage |
| `base_dttm` | `TIMESTAMPTZ` | NOT NULL | 선택 source 발표시각 | 선택된 발표 기준 일시 |
| `sky_condition_cd` | `TEXT` | NOT NULL | `SKY` 공통 의미 변환 | 하늘 상태 |
| `precipitation_type_cd` | `TEXT` | NOT NULL | 제품별 `PTY` 변환 | 강수 형태 |
| `temperature` | `DOUBLE PRECISION` | NOT NULL | ultra `T1H` / short `TMP` | 기온, °C |
| `precipitation_prob` | `DOUBLE PRECISION` | NULL | `POP` | 강수확률, % |
| `precipitation_amount` | `DOUBLE PRECISION` | NULL | ultra `RN1` / short `PCP` | 시간 강수량 하한, mm |
| `humidity` | `DOUBLE PRECISION` | NULL | `REH` | 상대습도, % |
| `wind_speed` | `DOUBLE PRECISION` | NULL | `WSD` | 풍속, m/s |
| `created_dttm` | `TIMESTAMPTZ` | NOT NULL | DB default | 행 생성 일시 |
| `updated_dttm` | `TIMESTAMPTZ` | NOT NULL | DB trigger | 선택 결과 변경 일시 |

`source_product_cd`는 `ultra_short`, `short_term`이다. 다음 정각부터 13개 정각에 대해
최신 검증 snapshot 안의 같은 제품 최신 발표를 고른 뒤, 필수값이 완전한 초단기 exact
target이 있으면 이를 사용하고 없으면 단기를 사용한다. 필요한 모든
`active station의 distinct weather_grid × 13시간` 조합이 완전할 때만 전체 buffer를
한 transaction에서 교체한다. 13번째 시각은 정각 rollover 중 미래 12행을 보장한다.
writer는 topology shared lock을 얻은 뒤 입력 manifest와 활성 격자 fingerprint가 여전히
최신인지 다시 확인하므로 오래 계산한 job이 나중 snapshot을 덮을 수 없다.
API는 반환할 미래 12행의 `min(updated_dttm)`을 publication freshness로 사용하고
`now-45분..now+5분` 범위 밖이면 503을 반환한다. 오래된 행 하나가 섞여도 fail-closed다.

수치 CHECK는 `temperature -50..50°C`, `precipitation_prob 0..100%`,
`precipitation_amount 0 이상`, `humidity 0..100%`, `wind_speed 0..50m/s`다. 대상시각은
유한한 정각이고 선택한 `base_dttm`보다 뒤여야 한다.

하늘 상태 변환:

| KMA SKY | `sky_condition_cd` |
| ---: | --- |
| 1 | `clear` |
| 3 | `mostly_cloudy` |
| 4 | `cloudy` |

강수 형태 변환:

| 제품 | KMA PTY | `precipitation_type_cd` |
| --- | ---: | --- |
| 공통 | 0 | `none` |
| 공통 | 1 | `rain` |
| 공통 | 2 | `rain_snow` |
| 공통 | 3 | `snow` |
| short | 4 | `shower` |
| ultra | 5 | `raindrop` |
| ultra | 6 | `raindrop_snow_flurry` |
| ultra | 7 | `snow_flurry` |

## event

현재·예정 행사 중 대시보드 인근 행사 화면에 바로 표시할 수 있는 행사 한 건이다.
장소명과 Point를 같은 행에 평탄화한다.

| 컬럼 | 타입 | Null/키 | 원천·변환 | 의미 |
| --- | --- | --- | --- | --- |
| `event_id` | `TEXT` | PK, NOT NULL | `{source}:{source_event_id}` | 외부 행사 ID |
| `event_source_cd` | `TEXT` | UK(source/source ID), NOT NULL | collector source ID | 행사 원천 |
| `source_event_id` | `TEXT` | UK(source/source ID), NOT NULL | 아래 식별 규칙 | 원천 범위 행사 ID |
| `event_name` | `TEXT` | NOT NULL | `TITLE` / 공연명 | 화면 행사명 |
| `event_spot_nm` | `TEXT` | NULL | `PLACE` / `CODE_TITLE_B` | 화면 장소명 |
| `event_point` | `geometry(Point,4326)` | NOT NULL | 원천 또는 시설 좌표 asset | 인근 조회 위치 |
| `event_point_source_cd` | `TEXT` | NOT NULL | 변환 결과 | 좌표 출처 |
| `location_accuracy_cd` | `TEXT` | NOT NULL | 변환 결과 | 좌표 정확도 |
| `event_start_dt` | `DATE` | NOT NULL | `STRTDATE` / `SDATE` | 시작일(KST) |
| `event_end_dt` | `DATE` | NOT NULL | `END_DATE` / `EDATE` | 종료일(KST) |
| `last_seen_dttm` | `TIMESTAMPTZ` | NOT NULL | 완전 source snapshot | 마지막 확인 일시 |
| `created_dttm` | `TIMESTAMPTZ` | NOT NULL | DB default | 최초 발행 일시 |
| `updated_dttm` | `TIMESTAMPTZ` | NOT NULL | DB trigger | DB 변경 일시 |

`event_source_cd`는 `cultural_event`, `performance_event`다. performance는 `SCH_SEQ`다.
cultural은 문자열을 Unicode NFC·trim·연속 공백 한 칸, 빈 선택 장소는 null, 날짜는
ISO `YYYY-MM-DD`로 정규화한다. 명시적 null을 포함한 `[행사명, 장소명, 시작일, 종료일]`
RFC 8785 UTF-8 JSON의 SHA-256 앞에 `v1:`을 붙여 `source_event_id`로 사용한다. escape와
회귀값은 [source-target-mapping.md](source-target-mapping.md)의 cultural v1 계약을 따른다.
`event_id`는 정확히
`event_source_cd || ':' || source_event_id`다. cultural 식별 필드가 바뀌면 새 ID로
간주하고 같은 완전 snapshot reconcile에서 이전 ID를 제거한다.

같은 cultural canonical ID가 중복되면 Gold payload 전체가 동일한 경우만 dedupe한다.
Point·날짜·표시값이 다르면 snapshot 전체를 거부하고 충돌 지표와 payload를 quarantine에
남긴다. source가 진짜 0건이거나 유효 Point·현재 일정 필터 뒤 0건이어도 정상 EMPTY로
reconcile하고 `publication_state`에 row count 0을 기록한다.

원천 좌표는 `source_reported/source_reported`, OSM Nominatim으로 검수한 체육시설
대표좌표는 `curated_osm_nominatim/approximate` 조합만 허용한다. Point가 없거나 날짜를
파싱할 수 없거나 종료일이 시작일보다 앞서거나 snapshot KST 날짜보다 2년을 넘는 행은
Silver/quarantine에 남긴다. Gold는 `event_end_dt >= KST 오늘`인 행만 원천별 완전
snapshot transaction으로 reconcile한다. 화면이 쓰지 않는 category, 무료 여부, 요금,
URL, 이미지는 Silver가 소유한다.
Point는 non-empty이고 DDL 안전 bounding box를 통과해야 한다. 서울시 event 원천과 실제
station 거리 조회가 업무 범위를 제한하므로 자치구 Polygon을 게시 gate로 두지 않는다.
API는 `now-36시간 <= last_seen_dttm <= now+5분`인 행만 노출한다. 행이 없는 최신/오래된
EMPTY는 모두 빈 배열이고, 이 cutoff는 남은 stale 행을 무기한 노출하지 않기 위한 규칙이다.

공연 좌표 asset은 `stadium-coordinates-v1`, git `5432a84`, SHA-256
`0e0c047bd08f77e82bbccda969c0e726af6998ceaa92979081506cb2140a969b`다. performance
manifest는 exact asset identity를 포함한다. 코드↔시설명이 다르면 source snapshot을
거부하고, 미등재 코드는 Silver-only다. seed correction은 performance event 전체를
새 revision으로 reconcile한다.

## station_urgency

대여소별 최신 재배치 판단 한 건이다.

| 컬럼 | 타입 | Null/키 | 원천·변환 | 의미 |
| --- | --- | --- | --- | --- |
| `sta_id` | `TEXT` | PK/FK, NOT NULL | urgency `sta_id` | 대여소 ID |
| `base_dttm` | `TIMESTAMPTZ` | NOT NULL | batch window | 계산 기준 일시 |
| `urgency_score` | `DOUBLE PRECISION` | NOT NULL, 0..100 | 산출값 | 우선순위 점수 |
| `critical_remaining_min` | `INTEGER` | NOT NULL, `>=0` | `minutes_until_critical` | 위험까지 남은 분 |
| `rebalance_need_type_cd` | `TEXT` | NOT NULL | `action_type` | 공급·회수 판단 |
| `created_dttm` | `TIMESTAMPTZ` | NOT NULL | DB default | 최초 생성 일시 |
| `updated_dttm` | `TIMESTAMPTZ` | NOT NULL | DB trigger | 최신 결과 반영 일시 |

판단 코드는 `normal`, `supply_needed`, `retrieval_needed`다. 기대 집합은 `active station ∩
anchor와 정확히 같은 stock 관측 ∩ 게시된 prediction 지원 집합`이며 성공 manifest로
입력 digest와 완전성을 증명한 뒤 projection 전체를 교체한다. producer는 topology shared
lock에서 Gold active station의 ID·정원·Point·센터를 읽는다. 과거 logical time과 exact
same version은 no-op이고 같은 anchor의 더 큰 correction revision은 전체를 다시 교체한다.
publisher는 같은 anchor Gold `station_stock` commit을 선행 확인한다. `/alerts`는 urgency와
stock을 같은 station·같은 `base_dttm`으로 inner join하고 `now-10분..now+5분` 밖 결과와
inactive station/center를 노출하지 않는다. 같은-anchor stock/station correction 뒤에는
`urgency.updated_dttm >= station_stock.updated_dttm`과 `>= station.updated_dttm`인 재계산
결과만 노출하고
`urgency_score DESC, sta_id ASC`로 정렬한다. route producer가 쓰는 `bike_qty`는 S3 urgency
batch가 소유하므로 Gold에 중복 저장하지 않는다. route producer는
`retrieval_needed→pickup`, `supply_needed→dropoff`만 허용하고 `normal`·`bike_qty<=0`은
후보에서 제외한다.

## rebalance_route

배차센터에서 출발하는 차량 회차 한 개다.

| 컬럼 | 타입 | Null/키 | 원천·변환 | 의미 |
| --- | --- | --- | --- | --- |
| `route_id` | `UUID` | PK, NOT NULL | 고정 namespace의 batch/center/ordinal UUIDv5 | 결정적 경로 ID |
| `dispatch_center_id` | `TEXT` | FK, NOT NULL | station과 같은 SSOT | 출발 센터 ID |
| `route_status_cd` | `TEXT` | NOT NULL | producer/API 전이 | 경로 상태 |
| `proposed_dttm` | `TIMESTAMPTZ` | NOT NULL | route batch anchor | 제안 일시 |
| `dispatched_dttm` | `TIMESTAMPTZ` | NULL | 운영 API | 실행 확정 일시 |
| `completed_dttm` | `TIMESTAMPTZ` | NULL | 운영 API | 완료 일시 |
| `cancelled_dttm` | `TIMESTAMPTZ` | NULL | 운영 API | 승인 후 취소 일시 |
| `created_dttm` | `TIMESTAMPTZ` | NOT NULL | DB default | 행 생성 일시 |
| `updated_dttm` | `TIMESTAMPTZ` | NOT NULL | DB trigger | 상태 변경 일시 |

상태는 `proposed`, `dispatched`, `completed`, `cancelled`다. INSERT는 활성 센터의
`proposed`만 허용한다. 전이는 `proposed→dispatched→completed` 또는
`proposed→dispatched→cancelled`만 허용하며 각 전이 일시는 한 번만 설정한다. ID,
센터, 제안일시, 이미 설정한 lifecycle 일시는 불변이다. 삭제는 `proposed`만 가능하다.
모든 일시는 유한하고 완료 시 `proposed_dttm <= dispatched_dttm <= completed_dttm`,
취소 시 `proposed_dttm <= dispatched_dttm <= cancelled_dttm` 순서다.
UUIDv5 namespace는 `d0d59897-9e72-541f-bb05-bd3d113c2639`다. name의 정확한 canonical JSON과
회귀 UUID는 [publication-contract-v1.md](publication-contract-v1.md)를 따른다. center ID와
후보의 동률 정렬 규칙은 원천-목표 매핑 문서를 따른다.
route 목록은 `proposed|dispatched|completed|cancelled`만 status filter로 받고 기본 100·최대 500의
`limit`, 0 이상의 `offset`, `(proposed_dttm DESC, route_id ASC)` 정렬을 사용한다. 상태 변경은
expected status guarded UPDATE이고 없는 ID는 404, 상태 충돌은 409다. path ID는 API UUID
타입으로 먼저 검증해 malformed 값은 422, 응답 UUID는 문자열이다.
route publication은 현재 station·demand·stock tuple과 urgency input의 동명 tuple이 같을
때만 허용해 correction 뒤 오래된 urgency로 새 route를 만들지 않는다.

## rebalance_route_stop

한 경로의 방문 순서 한 개다.

| 컬럼 | 타입 | Null/키 | 원천·변환 | 의미 |
| --- | --- | --- | --- | --- |
| `route_id` | `UUID` | PK/FK, NOT NULL | route producer | 경로 ID |
| `visit_no` | `SMALLINT` | PK, NOT NULL | `visit_order` | 방문 순서 |
| `sta_id` | `TEXT` | FK, NOT NULL | route producer | 방문 대여소 ID |
| `route_action_type_cd` | `TEXT` | NOT NULL | `action` | 차량 작업 유형 |
| `bike_cnt` | `INTEGER` | NOT NULL, `>0` | 산출값 | 이동 자전거 수 |
| `created_dttm` | `TIMESTAMPTZ` | NOT NULL | DB default | 행 생성 일시 |

작업 코드는 `pickup`, `dropoff`다. commit 시 각 route에는 `1..N`으로 연속된 stop이
하나 이상 있어야 한다. stop은 active station이어야 하고 parent header와 같은
`dispatch_center_id`에 속해야 하며, parent가 `proposed`일 때만 변경할 수 있다. route를
삭제하면 stop은 cascade 삭제된다. 기존 proposed aggregate 삭제와 새 header·stop
삽입은 반드시 한 transaction에서 수행한다.

publisher staging은 manifest의 차량 초기 적재량 0과 `TRUCK_CAPACITY` config version을
사용한다. visit 순 pickup은 더하고 dropoff는 빼며 running load가 매 단계 `0..capacity`여야
한다. 마지막 양수 잔량은 허용한다. route coverage는 dispatched 전부와 urgency stock
anchor 이후 완료되어 아직 후속 stock에 반영되지 않은 completed route 및 정렬 stop을
포함한다. route/stop DML은 BEFORE STATEMENT에서 topology shared→route lock을 잡고,
dispatch 전이는 활성 센터와 active/same-center stop을 다시 검증한다.

## 보존·publication 요약

| 테이블 | 보존·갱신 정책 |
| --- | --- |
| `weather_grid` | collector 격자 설정과 함께 명시적 seed 변경 |
| `dispatch_center` | 삭제 대신 갱신·비활성화, 영향 station 원자 재배정 |
| `station` | 완전 snapshot만 갱신·비활성화, 이력은 Silver |
| `station_stock` | station별 최신 1행, `base_dttm` guard |
| `station_demand_forecast` | 최신 완전 12시간 snapshot 한 개를 원자 교체 |
| `weather_forecast` | 선택 완료된 다음 13개 정각 rollover buffer를 원자 교체 |
| `event` | 원천별 현재·예정, 완전 snapshot reconcile |
| `station_urgency` | 계산 가능 station 전체의 최신 완전 projection 원자 교체 |
| `proposed` route·stop | 다음 성공 batch에서 aggregate 단위 원자 교체 |
| terminal route·stop | archive 정책 구현 전까지 불변 보존 |
| `gold_meta.publication_state` | key별 마지막 version/tombstone, 전진만 허용하고 삭제 금지 |

## 현행 이름·계약 교체

| 현행 | 목표 |
| --- | --- |
| `stations` | `station` |
| `lat`, `lon` | 저장은 `*_point`, 응답에서 `ST_Y/X AS lat/lon` |
| 미사용 `gu`, `gu_master`, `dong_master` | Gold·`StationSummary`에서 제거 |
| `grid_nx`, `nx` | `weather_grid.weather_grid_x_no` |
| `grid_ny`, `ny` | `weather_grid.weather_grid_y_no` |
| `observed_at`, `batch_run_at` | 의미에 맞는 `base_dttm` |
| `predicted_return_cnt` | 저장은 `predicted_rtn_cnt`, 응답 alias 가능 |
| `forecast_points` | `station_demand_forecast` |
| `weather_current` | Gold 제외, Silver/ML 유지 |
| 독립 단기·초단기 Gold 행 | resolver가 고른 `weather_forecast` 한 행 |
| `rainfall`, `precip_amount` | `precipitation_amount` |
| `precip_prob` | `precipitation_prob` |
| `pty_type` | `precipitation_type_cd` |
| `sky_cond` | `sky_condition_cd` |
| `cultural_events`, 별도 `event_spot` | 평탄화한 `event` |
| `title`, `place` | `event_name`, `event_spot_nm` |
| 미사용 `category`, `is_free` | Gold·dashboard API에서 제거 |
| 항상 빈 forecast `reasons` | producer가 없으므로 API·Web에서 제거 |
| `start_date`, `end_date` | `event_start_dt`, `event_end_dt` |
| `minutes_until_critical` | `critical_remaining_min` |
| urgency `action_type` | `rebalance_need_type_cd` |
| route stop `action` | `route_action_type_cd` |
| urgency `bike_qty` | Gold 제외, route producer는 S3 batch 사용 |
| `region` | 저장은 `dispatch_center_id`, 응답은 센터명 alias |
| `status` | `route_status_cd` |
| `visit_order` | `visit_no` |
| route UUID→Pydantic `str` | SQL에서 `route_id::text`, 배열은 `::uuid[]` |
| `proposed_at`, `dispatched_at`, `completed_at` | 대응하는 `*_dttm` |

인근 행사 응답의 `radius_km`는 Gold 컬럼이 아니라 API의 단일 nearby-radius config다. SQL
`ST_DWithin`에 쓴 값과 같은 값을 응답해 Web 지도 원의 반경을 일치시킨다.
