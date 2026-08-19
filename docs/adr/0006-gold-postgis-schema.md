# Gold RDS는 소비자 중심 PostGIS 서빙 모델로 구성한다

## 상태

제안

## 배경

현행 Gold는 서비스 구현 과정에서 빠르게 만들어져 좌표를 `lat`/`lon`으로 나누어
저장하고, 인근 행사 거리를 애플리케이션에서 계산한다. 반대로 원천마다 생기는 이력과
행정 계층을 모두 옮기면 Gold는 대시보드용 서빙 저장소가 아니라 Silver의 또 다른
복제본이 된다.

저장소의 생산자와 소비자를 조사한 결과, Gold를 직접 읽는 주체는 API와 재배치
워크플로다. ML 학습·추론과 Normalizer는 S3 Silver를 읽는다. 대시보드가 실제로
필요로 하는 것은 다음과 같다.

- 대여소와 최신 재고
- 최신 완전 수요 예측과 긴급도
- 대여소 주변의 현재·예정 행사
- 다음 12개 정각을 끊김 없이 제공하기 위한 13개 정각의 시간별 날씨 buffer
- 배차 센터와 실행 가능한 재배치 경로

행정구·행정동 필터, 생활인구, 대여 이력, 원천 제품별 날씨 이력은 확인된 대시보드
계약이 아니다. 행사 장소를 독립적으로 조회하거나 재사용하는 소비자도 없다. 현재
구현에 잔존한 필드만을 근거로 테이블을 유지하지 않고, 실제 조회·화면·운영 계약을
Gold 경계로 삼는다.

운영 RDS는 아직 배포하지 않았다. 따라서 임시 로컬 스키마의 변경 이력을 운영
migration으로 쌓지 않고, 반복 검증을 통과한 목표 DDL 하나를 최초 운영 베이스라인으로
사용할 수 있다.

## 결정

### 1. Gold에는 10개 서빙 테이블과 제어 테이블 하나만 둔다

- 기준 데이터: `weather_grid`, `station`, `dispatch_center`
- 최신·단기 서빙 데이터: `station_stock`, `station_demand_forecast`,
  `weather_forecast`, `event`, `station_urgency`
- 운영 집계: `rebalance_route`, `rebalance_route_stop`

이 10개는 `public`의 API·운영 서빙 테이블이다. 별도 `gold_meta` schema에는
`publication_state` 한 개만 둔다. 이 테이블은 원천 데이터나 이력을 보관하지 않고,
정상적인 빈 결과까지 포함해 어떤 publication version이 현재 projection에 반영됐는지
기억하는 transactional watermark다. API role은 읽지 않고 publisher만 제한적으로
접근한다.

`gu_master`, `dong_master`, `weather_observation`, `event_spot`은 두지 않는다. 생활인구와
대여 이력도 Silver에 남긴다. 자치구·행정동은 현재 화면의 필터·집계 계약이 없고,
초단기실황과 원천 제품별 발표 이력은 ML이 이미 Silver에서 사용한다. 행사 장소는
행사와 생명주기가 같고 독립 소비자가 없으므로 `event`에 평탄화한다. 새 소비 요구가
생기면 쿼리, 그레인, 보존 기간을 먼저 확정한 뒤 Gold 추가 여부를 결정한다.

`de-project` ERD는 팀이 합의한 표준 단어와 검증된 관계를 찾는 참고 자료다. 현재
대시보드가 소비하지 않는 엔터티까지 그 골격을 복제하는 설계 원본은 아니다.

### 2. 날씨는 13개 정각을 게시하고 다음 12개를 제공한다

단기예보와 초단기예보는 수집과 정규화 단계까지 Silver에서 분리한다. 같은 대상 시각에
두 제품을 모두 Gold에 적재하면 대시보드가 다시 선택 정책을 알아야 하므로,
`weather_forecast`에는 `(weather_grid_id, forecast_dttm)`당 승자 한 행만 둔다.

발행기는 다음 규칙을 적용한다.

1. 기준 시각 다음의 13개 정각을 대상 구간으로 정한다. 마지막 한 시간은 정각 경계에서
   다음 resolver가 완료되기 전에도 API가 미래 12개를 유지하기 위한 rollover buffer다.
2. 각 격자·대상 시각에 유효한 최신 초단기예보가 있으면 선택한다.
3. 초단기예보가 없으면 유효한 최신 단기예보를 선택한다.
4. 원천 코드를 공통 의미 코드로 정규화하고, `source_product_cd`와 `base_dttm`으로
   선택 근거를 남긴다.
5. 활성 station이 참조하는 distinct `weather_grid`와 13개 시각의 후보가 완전한
   경우에만 전체 스냅샷을 한 transaction으로 교체한다.

Gold에는 제품별 PK와 과거 발표본을 두지 않는다. 발표본 재현과 원천 품질 추적은
Silver의 책임이다. `weather_grid`는 현재 34개 격자의 안정적인 `x_y` 자연키와 기상청
X/Y 번호만 관리한다. 화면이 격자 경계나 중심점을 조회하지 않으므로 파생 geometry는
저장하지 않는다.

### 3. Point를 좌표의 단일 진실 공급원으로 사용한다

- 대여소, 행사, 배차 센터는 `geometry(Point, 4326)`로 저장한다.
- 영속 `lat`/`lon` 컬럼은 두지 않는다. API 호환 값은 `ST_Y(point) AS lat`,
  `ST_X(point) AS lon`으로 파생한다. X는 경도, Y는 위도다.
- 반경과 최근접 검색은 Point를 `geography`로 변환한 뒤 `ST_DWithin`과
  `ST_Distance`를 사용해 미터 단위로 계산한다.
- 반복되는 거리 조회를 위해 동일한 `point::geography` 표현식에 GiST 인덱스를 둔다.
- 좌표가 없거나 SRID·비어 있음·안전 bounding box 검증을 통과하지 못한 대여소·행사는
  Gold에 발행하지 않고 Silver/quarantine에 남긴다.

서울시 station/event 원천 자체가 업무 범위를 소유하고, 실제 event 소비는 선택 station과의
미터 거리로 한 번 더 제한한다. 소비자가 쓰지 않는 자치구 Polygon을 필수 publication
master로 만들거나 행정 경계선 위·밖 여부로 행을 버리지 않는다. 출처 메타가 없는 로컬
GeoJSON은 품질 비교에만 쓸 수 있고 Gold 합격·탈락이나 fingerprint 입력이 아니다.

PostGIS의 목적은 실제 소비되는 점 좌표와 거리 연산이다. 현재 소비자가 없는 자치구·
행정동 Polygon이나 날씨 격자 geometry를 PostGIS 사용의 명분으로 추가하지 않는다.

### 4. 대여소와 배차 센터의 책임을 한곳에 고정한다

대여소 컬럼 소유권은 다음과 같다.

- ID·주소·기본 Point: `bike_station_master`
- 이름·거치대 수·활성 관측: `bike_station_realtime`
- Point fallback: 마스터 좌표가 무효일 때만 실시간 좌표
- 날씨 격자: 공통 LCC 변환 결과
- 배차 센터: 발행 시 확정한 `station.dispatch_center_id`

Gold의 활성 대여소는 이름, 주소, 유효 Point, `hold_cnt > 0`, 날씨 격자, 활성 배차
센터를 모두 가져야 한다. 불완전한 행은 Silver/quarantine에 남긴다. 한 번 발행한
대여소는 검증된 완전 realtime snapshot의 서로 다른 `window_start` 세 개에서 연속으로
serving-valid하지 않을 때 `is_active=false`로 바꾸고 행은 보존해 과거 경로 FK를
유지한다. 미관측뿐 아니라 관측됐어도 이름이 비었거나 `rackTotCnt`가 null/0 이하인
경우를 포함한다. 같은 window의 correction은 최신 revision으로 판정을 교체할 뿐 횟수를
늘리지 않으며, `PARTIAL`·실패 snapshot은 연속 횟수에 넣지 않는다. 재등장해 모든 필수값을
만족하면 즉시 활성화 후보가 되고 아래 downstream coverage를 갖춘 publication에서 반영한다.
현 collector처럼 이름 결손 행 자체를 drop하면 ID별 invalid 판정이 불가능하므로 전환
source는 station ID를 보존한 nullable 이름을 Silver에 넘겨야 한다.
후보 grid의 13시간 weather가 있어야 하며, 두 model이 지원하는 후보는 같은 anchor demand
projection에 포함해 활성화하거나 다음 demand publication까지 기다린다. model 미지원
station은 demand row 없이 활성화할 수 있고 forecast API는 명시적으로 404를 반환한다.

기존 station의 생존은 realtime이 판단한다. 완전 master에서 ID가 빠지거나 주소·Point가
일시적으로 무효여도 realtime이 serving-valid이면 마지막으로 검증된 master 주소·Point와
그때의 `master_base_dttm`을 유지하고 품질 지표를 낸다. master/realtime 또는 기존
Gold/new master Point의 geography 거리가 100m를 초과하면 자동 덮어쓰지 않고 review
대상으로 보낸다. 정확히 100m는 허용한다. 승인된 relocation만 같은 logical time의 명시적 station correction revision으로
반영한다. 신규 station은 유효한 master 주소가 반드시 있어야 한다.

`dispatch_center`는 공식 행정 관할 마스터가 아니라 현재 라우팅에 사용하는 기준점이다.
고정 ID, Point, 정확도, 출처 설명을 저장한다. API와 재배치 생산자가 서로 다른
최근접 계산을 하지 않도록 대여소의 배차 센터 귀속은 `station.dispatch_center_id`를
단일 진실 공급원으로 사용한다. 대여소나 센터가 변경될 때 같은 발행 transaction에서
귀속을 다시 계산하며, 활성 대여소는 비활성 센터를 참조할 수 없다.

최초 11개 기준점은 [dispatch-center-seed.yaml](../gold/dispatch-center-seed.yaml)에 고정
ID·Point·정확도·원천 commit/hash와 함께 둔다. 이는 현장 측량 좌표가 아니라 기존
주소 조사 기반의 landmark 근사이며, 영남은 행정동 중심 근사다. 따라서 검증일은 null로
두고 품질 코드를 숨기지 않는다. 좌표를 보정할 때 기존 ID를 재사용하되 seed version과
publication revision을 올리고 전체 station 귀속을 다시 계산한다.

`station_stock`은 대여소별 최신 행 하나만 유지한다. 더 오래된 `base_dttm`의 재실행은
현재 값을 덮어쓰지 못하며, exact same publication은 no-op이다. 같은 logical time의 더
큰 correction revision이 claim된 경우에만 equal-base authoritative 값을 교체한다. 재고
이력은 Silver가 소유한다. 같은 authoritative realtime window의 station 이름·정원·활성과
stock은 두 publication key를 함께 claim한 한 transaction에서 게시한다. API는
`station.last_seen_dttm = station_stock.base_dttm`도 확인해 서로 다른 window를 섞지 않는다.

### 5. 행사는 화면에 표시 가능한 최소 단위로 평탄화한다

`event`는 이름, 선택적 장소명, 필수 Point, 시작·종료일과 원천 식별·좌표 품질 메타만
저장한다. 독립 `event_spot` 테이블은 만들지 않는다. 현재 화면과 API가 사용하지 않는
카테고리, 무료 여부, 상세 일정, 이용료, URL, 이미지 필드도 Gold에 두지 않는다.

행사는 `event_source_cd`와 `source_event_id`를 보존하고, 외부용 `event_id`는
`{source}:{source_event_id}` 형식으로 만든다. 공연행사는 원천의 `SCH_SEQ`를 사용한다.
안정 ID가 없는 문화행사는 `v1:`이 붙은 canonical payload SHA-256을 사용한다. 문자열은
Unicode NFC, 앞뒤 제거, 연속 공백 한 칸으로 정규화하고 선택적 빈 장소는 JSON `null`로
바꾼다. 파싱한 날짜는 ISO `YYYY-MM-DD`로 만들고 `[이름, 장소, 시작일, 종료일]`을
RFC 8785 UTF-8 JSON으로 직렬화해 hash한다. non-ASCII escape와 회귀값은
[source-target-mapping.md](../gold/source-target-mapping.md)의 cultural v1 계약을 따른다.
완전한 원천 스냅샷을 source
단위로 reconcile하여 종료·삭제·수정 전 행이 남지 않게 한다.

행사 Point가 없으면 인근 행사라는 소비 계약을 만족할 수 없으므로 Gold에 발행하지
않는다. 원천 좌표와 큐레이션한 근사 좌표는 `event_point_source_cd`와
`location_accuracy_cd`로 구분한다.
API는 일일 source의 `last_seen_dttm`이 `now-36시간..now+5분`인 행만 노출해 stale 행사를
무기한 보여주지 않는다.

### 6. publication manifest와 영속 watermark로 원자 발행한다

모든 seed·collector·파생 publisher는 target을 바꾸기 전에 불변 publication manifest를
마지막 산출물로 만든다. manifest에는 publication key와 논리시각, 명시적 correction
revision, schema/publisher version, 입력 version·checksum, 정렬한 출력 객체의 URI·바이트
SHA-256·행 수를 둔다. 일부 parquet을 정식 key에 먼저 쓰거나 성공 manifest 없이 적재하는
현행 inference·urgency·route 흐름은 운영 전환 차단 대상이다.

publisher는 `gold_meta.claim_publication()`으로 key별 transaction lock과 상태행 lock을
잡는다. `(logical_dttm, revision_no)`가 더 최신이면 게시하고, 과거면 아무것도 바꾸지
않는다. 같은 version·같은 fingerprint는 no-op, 같은 version·다른 fingerprint는
unversioned mutation으로 실패하며, 같은 logical time의 더 큰 revision만 명시적 교정을
허용한다. target 변경과 watermark 갱신은 같은 DB transaction에서 함께 commit·rollback한다.
`publication_state` 행은 key 변경·시간 퇴행·동일 revision 갱신·삭제를 trigger로 막는다.
logical time과 관측·계산·발표·제안·last-seen 시각은 유한해야 하고 DB 현재시각보다 5분을
넘게 미래이면 거부한다. API freshness도 과거 cutoff와 `now+5분` 상한을 함께 적용한다.

한 transaction이 여러 projection을 바꾸면 관련 key를 문자열 오름차순으로 모두 claim한다.
센터 seed 변경과 station 재배정은 `dispatch_center`+`station`, 격자 seed 변경과 station
재배정은 `weather_grid`+`station`을 함께 전진시킨다. 새 active grid에 13시간 날씨가
없다면 `weather_forecast`까지 같은 release에서 게시하거나 station 활성화를 미룬다.
실제로 바꾸지 않은 dependent projection의 watermark를 거짓으로 전진시키지는 않는다.

- `station_demand_forecast`의 기대 집합은 `active Gold station ∩ rental/return 모델 공통
  지원 집합`이다. 원천 horizon `h=1..12`의 구간 시작은 `base+(h-1)시간`, Gold의
  `predicted_dttm`은 구간 종료인 `base+h시간`이다. 12개가 완전할 때만 전체 교체한다.
  수량은 finite·비음수 float64를 IEEE-754 ties-to-even으로 정수화한다. forecast 재고는
  같은 anchor stock에서 `max(0, 이전+반납-대여)`로 누적하며 정원 상한을 두지 않고 공통
  scoring config로 supply/retrieval/normal을 결정한다. 누적 정수는 API
  `points[].predicted_bikes`이며 DB 컬럼은 아니다.
  기대 집합이 0임을 topology와 두 model manifest가 증명한 경우에는 EMPTY projection과
  watermark로 이전 행을 정리한다.
- `weather_forecast`는 활성 station의 격자와 다음 13개 정각이 모두 준비된 뒤 전체를
  교체한다. API는 요청시점보다 뒤인 12개를 모두 얻지 못하면 partial 대신 503을 낸다.
- `station_stock`은 station별 최신행 guard를 쓰고, `station_urgency`는 정확한 계산 가능
  집합의 최신 projection 전체를 교체한다. 두 테이블 모두 same logical time의 higher
  correction revision은 target까지 다시 반영하고 exact same version만 no-op이다. urgency는
  같은 anchor stock이 Gold에 먼저 commit된 경우만 게시하고 `/alerts`도 same-anchor stock을
  join한다. 같은-anchor stock/station correction 뒤에는 urgency의 DB 갱신시각이 두 입력보다
  최신인 재계산 결과만 노출해 옛 판단을 fail-closed로 숨긴다.
- `event`는 source별로 reconcile한다. Collector `EMPTY`는 Silver URI·completeness가
  없는 현재 계약을 별도 검증해 받아들이며, 빈 결과도 state의 `published_row_cnt=0`으로
  남겨 과거 행이 되살아나는 것을 막는다.
- `rebalance_route`도 빈 proposed aggregate를 정상 publication으로 기록한다.

검증, 기존 projection 제거, 새 행 삽입과 state 전이는 하나의 transaction에 들어간다.
따라서 소비자는 빈 중간 상태나 서로 다른 배치가 섞인 결과를 볼 수 없다.
최초 bootstrap도 station 후보를 target 밖에서 준비하고, station·stock·demand·weather를
한 release transaction에 넣어 dependency가 완전한 station만 active로 commit한다.

### 7. 재배치 경로는 header와 stop을 하나의 집계로 관리한다

`rebalance_route`와 `rebalance_route_stop`은 따로 발행되는 두 데이터셋이 아니라 하나의
route aggregate다. 기존 `proposed` 경로 제거와 새 header·stop 삽입은 같은 advisory
lock과 transaction 안에서 수행한다. `route_id`는 UUID이고, stop은 `visit_no`가
1부터 끊김 없이 이어지며 한 개 이상이어야 한다. 지연 constraint trigger가 이 조건을
commit 시점에 검사하고, route 삭제 시 stop은 `ON DELETE CASCADE`된다.

route ID는 고정 namespace `d0d59897-9e72-541f-bb05-bd3d113c2639`와 publication logical
time·revision·center ID·결정적 ordinal을 쓰는 UUIDv5다. 정확한 JSON bytes와 회귀 UUID는
[publication-contract-v1.md](../gold/publication-contract-v1.md)가 고정한다. urgency와 거리
동률은 항상 station ID로 깨 같은 입력의 생성 순서가 흔들리지 않게 한다.

새 경로는 활성 배차 센터의 `proposed` 상태로만 생성한다. 각 stop은 활성 station이고
header와 같은 `dispatch_center_id`여야 한다. 허용 전이는
`proposed → dispatched → completed`뿐이다.
route ID, 배차 센터, 제안 시각과 이미 기록한 생명주기 일시는 바꿀 수 없다.
`proposed`가 아닌 route의 stop도 변경할 수 없고, 삭제는 `proposed`에만 허용한다.
종료된 경로는 별도 archive 정책이 생기기 전까지 보존한다.

차량 초기 적재량은 0이다. stop 순서대로 `pickup`은 적재량을 더하고 `dropoff`는 빼며,
모든 중간 적재량은 route manifest가 고정한 `TRUCK_CAPACITY`의 `0..capacity` 안이어야 한다.
마지막 양수 잔량은 다음 cycle용으로 허용한다. 계산에 차감하는 coverage는 모든
`dispatched` route와 urgency 입력 stock anchor보다 뒤에 완료되어 아직 후속 stock 관측에
반영되지 않은 `completed` route다. producer와 publisher는 같은 canonical coverage digest를
재계산한다.
urgency `retrieval_needed`는 `pickup`, `supply_needed`는 `dropoff`로 변환하며 `normal`과
0 이하 수량은 route 후보에서 제외한다.

모든 topology write는 첫 row lock 전에 statement trigger 또는 명시적 helper로 `(129,1)`
exclusive 뒤 `(129,2)` route-operation lock을 잡는다. weather·route publisher는 topology
shared lock을 먼저 잡고 route publisher와 API 상태 전이는 route-operation lock을 공유한다.
row trigger에서 tuple lock 뒤 advisory lock을 잡는 방식은 교착을 만들 수 있으므로 쓰지
않는다. topology와 proposed route를 함께 바꾸는 transaction은 topology helper/statement를
먼저 실행하고 같은 transaction 안에서 route를 정리한다. publisher는 lock 안에서 현재
station/center publication dependency와 route coverage를 다시 확인해 manifest와 다르면
재계산하도록 실패시킨다.
station/센터 변경 transaction은 영향받는 proposed aggregate를 함께 제거해야 하며,
비활성 센터·station 또는 다른 센터 station을 참조한 proposed route는 commit할 수 없다.
`proposed→dispatched` 전이도 shared topology snapshot 안에서 센터와 모든 stop을 다시
검증한다.
route input은 현재 station·demand·stock state를 직접 고정하고 urgency publication이 사용한
동명 dependency와 같음을 재검증한다. correction 뒤 urgency가 갱신되지 않은 구간에는 새
proposed route를 게시하지 않는다.

### 8. 시간과 관리 메타데이터를 분리한다

- 실제 시각은 유한한 `TIMESTAMPTZ`, 저장·비교 기준은 UTC, 화면 표시는 KST다.
- 행사 시작·종료일과 센터 좌표 검증일처럼 시각이 없는 업무 달력 값은 `DATE`와
  `_dt`를 사용하며 무한대 날짜는 허용하지 않는다.
- 시각은 `_dttm`, 날짜는 `_dt`를 사용하며 `_at`, `_date`는 사용하지 않는다.
- `base_dttm`은 관측·발표·계산 배치 시각이고, `predicted_dttm`과
  `forecast_dttm`은 대상 시각이다.
- `created_dttm`과 `updated_dttm`은 DB가 소유한다. 갱신 trigger는
  `created_dttm` 변경을 막고 `updated_dttm`을 새로 기록한다.

### 9. 빈 DB 베이스라인 검증과 운영 배포를 분리한다

`docs/gold/target-schema.sql`은 PostgreSQL 16의 비어 있는 DB를 위한 첫 베이스라인이다.
transaction 안에서 PostGIS extension을 활성화하고, 목표 relation이 하나라도 이미
있으면 즉시 실패한다. 기존 임시 테이블을 `IF NOT EXISTS`로 통과시키거나 변환하는
migration이 아니며 어떤 테이블도 `DROP`하지 않는다.

로컬에서는 격리한 `postgis/postgis:16-3.5` 환경에 베이스라인을 두 번 적용해 첫 실행
성공과 재실행 fail-fast를 확인하고, fixture와 부정 테스트로 제약·trigger·공간 쿼리를
검증한다. RDS에서는 Docker의 PostGIS 버전을 가정하지 않고 지원 버전을 별도로
확인해야 한다. 이번 이슈는 ERD, DDL, 데이터 사전과 검증 근거를 확정하는 작업이며,
실제 RDS 생성·변경과 운영 배포는 수행하지 않는다.

현 로컬 Compose의 `postgres:16`과 매 기동 실행되는 `002/003` 초기화, 구 스키마용
`apps/api/seed_gold.py`/`make seed`는 이 baseline과 호환되지 않는다. 후속 전환 PR은
PostGIS image로 바꾸고 구 init을 목표 baseline 하나로 교체하며, 개발자가 보존 여부를
결정한 뒤 새 local volume에서만 clean-create한다. 기존 volume에 두 baseline을 겹쳐
적용하거나 자동 삭제하지 않는다. 구 seed 명령은 목표 10개 테이블·publication manifest를
사용하는 fixture publisher로 교체하기 전까지 비활성화한다.

## 기각한 대안

### de-project ERD 골격 복제

검증된 표준어와 관계는 재사용할 가치가 있지만, 생활인구·대여이력·행정 계층까지
옮기면 현재 소비자 없는 저장·동기화 책임이 생긴다. `de-project`는 단어와 관계의
참고 자료이고, Gold의 범위는 이 프로젝트의 소비 계약이 정한다.

### 단기·초단기예보를 제품별로 Gold에 보존

원천 충돌은 보존할 수 있지만 선택 책임이 API와 화면으로 새어 나온다. 제품별 원본과
발표 이력은 Silver에 보존하고, Gold는 정책으로 선택한 13시간 buffer만 보유해 API가
항상 미래 12시간 서빙값을 제공하게 한다.

### `lat`/`lon`과 Point 병행 저장

같은 좌표의 두 표현이 어긋날 수 있다. Point만 영속하고 응답용 위도·경도는 쿼리에서
파생한다.

### 행사 장소 정규화

독립 조회·재사용·별도 생명주기가 없는 장소를 분리하면 조인과 reconcile 책임만
늘어난다. 현재 계약에서는 행사에 필요한 장소명과 Point를 함께 저장한다.

## 결과

- 목표 구조는 초기 14개 후보에서 소비 근거가 있는 public 서빙 10개 테이블로 줄고,
  정상적인 빈 결과와 stale 재실행을 막는 제어 테이블 하나만 별도 schema에 둔다.
- 행정 경계 기준시점 문제와 제품별 날씨 충돌을 Gold 구조 밖으로 분리한다.
- PostGIS는 대여소·행사·배차 센터의 Point와 미터 거리 조회에 집중한다.
- 최신 스냅샷과 route aggregate는 부분 게시·stale overwrite·불가능한 상태 전이를
  차단하는 발행 단위가 된다.
- `docs/adr/0002-gold-schema-design.md`의 임시 물리 모델은 이 ADR이 대체한다.

정확한 관계·키·수명주기는 `docs/gold/target-erd.md`, 모든 컬럼 의미는
`docs/gold/data-dictionary.md`, 생산자·소비자와 발행 절차는
`docs/gold/source-target-mapping.md`, 실행 가능한 제약은
`docs/gold/target-schema.sql`을 따른다.
