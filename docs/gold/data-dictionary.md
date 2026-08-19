# Gold 데이터 사전

## 적용 범위

이 문서는 Gold DB, Loader 출력 레코드, 내부 SQL에서 사용하는 논리·물리 이름의
기준이다. 외부 API 응답은 호환을 위해 다른 이름을 유지할 수 있지만, DB 값을 다른
의미의 이름으로 재해석해서는 안 된다.

## 표준 접미사

| 접미사 | 의미 | 예시 |
| --- | --- | --- |
| `_id` | 식별자 | `sta_id`, `weather_grid_id` |
| `_nm` | 사람이 읽는 명칭 | `sta_nm`, `gu_nm` |
| `_no` | 순번·번호 | `visit_no`, `weather_grid_x_no` |
| `_cnt` | 개수·수량 | `bike_cnt`, `predicted_rtn_cnt` |
| `_rate` | 비율 | `precipitation_prob`은 기상 도메인 원어를 유지하는 예외 |
| `_lv` | 단계·수준 | `congestion_lv` |
| `_cd` | 제한된 코드값 | `route_status_cd` |
| `_dt` | 날짜 | `event_start_dt` |
| `_dttm` | 날짜와 시간 | `base_dttm`, `forecast_dttm` |
| `_min` | 분 단위 기간 | `critical_remaining_min` |
| `_dst` | 거리 | `use_dst` |
| `_point` | PostGIS 점 | `sta_point` |
| `_polygon` | PostGIS 면 | `dong_polygon` |

일시는 `_dttm`으로 통일하며 `_at`을 사용하지 않는다. `created_dttm`과
`updated_dttm`은 DB 행의 관리 시각이고, `base_dttm`은 데이터 자체의 기준 시각이다.

## 표준 단어

| 단어 | 의미 | 사용하지 않는 이름 |
| --- | --- | --- |
| `sta` | 따릉이 대여소 | `station`을 컬럼 접두어로 사용 |
| `gu` | 자치구 | 값만 담은 `gu` 컬럼 |
| `dong` | 행정동 | 값만 담은 `dong` 컬럼 |
| `rtn` | 반납 | `return` |
| `rent` | 대여 | `rental`을 컬럼 접두어로 사용 |
| `base` | 관측·계산·발표의 기준 | 실행시각과 대상시각을 혼용 |
| `predicted` | 모델 예측 대상 | `forecast`와 같은 테이블에서 혼용 |
| `forecast` | 기상 예보 대상 | `predicted`와 같은 테이블에서 혼용 |
| `precipitation` | 강수 | `rainfall`, `precip` |
| `dispatch_center` | 재배치 배차 센터 | `region` |
| `route_status` | 재배치 경로 상태 | 단독 `status` |
| `rebalance_action_type` | 재배치 작업 유형 | 단독 `action`, `action_type` |

`event_name`과 `event_type`은 접미사 일반 규칙보다 먼저 확정된 de-project ERD의
도메인 표준어이므로 그대로 사용한다.

## 시간 컬럼 의미

| 컬럼 | 의미 |
| --- | --- |
| `base_dttm` | 관측, 예측 생성, 긴급도 계산 또는 기상 발표의 논리 기준 일시 |
| `predicted_dttm` | 대여·반납 수요 예측의 대상 일시 |
| `forecast_dttm` | 기상 예보의 대상 일시 |
| `proposed_dttm` | 재배치 경로가 제안된 일시 |
| `dispatched_dttm` | 운영자가 경로 실행을 확정한 일시 |
| `completed_dttm` | 재배치 경로 실행이 완료된 일시 |
| `created_dttm` | DB 행이 최초 생성된 일시 |
| `updated_dttm` | DB 행이 마지막으로 변경된 일시 |

## 공간 컬럼 의미

| 컬럼 | 의미 |
| --- | --- |
| `gu_polygon` | 자치구 경계 MultiPolygon, EPSG:4326 |
| `dong_polygon` | 행정동 경계 MultiPolygon, EPSG:4326 |
| `weather_grid_point` | 기상청 격자의 대표 WGS84 Point |
| `sta_point` | 대여소 WGS84 Point |
| `event_spot_point` | 행사 장소 WGS84 Point |
| `dispatch_center_point` | 배차 센터 WGS84 Point |

위도·경도는 영속 컬럼이 아니다. 필요할 때 `ST_Y(point)`와 `ST_X(point)`로
파생하며 X=경도, Y=위도 순서를 지킨다.

## 도메인 코드

| 컬럼 | 허용값/원천 |
| --- | --- |
| `precipitation_type_cd` | 기상청 PTY 코드 |
| `sky_condition_cd` | 기상청 SKY 코드 |
| `rebalance_action_type_cd` (`station_urgency`) | `normal`, `supply_needed`, `retrieval_needed` |
| `rebalance_action_type_cd` (`rebalance_route_stop`) | `pickup`, `dropoff` |
| `route_status_cd` | `proposed`, `dispatched`, `completed`, `cancelled` |

두 테이블의 `rebalance_action_type_cd`는 같은 재배치 도메인이지만 계산 판단과 실제
차량 작업이라는 값 집합이 다르다. 구현 시 CHECK 제약을 테이블별로 둔다.

## 단위와 품질 규칙

| 컬럼 | 단위/규칙 |
| --- | --- |
| `temperature` | 섭씨 |
| `humidity` | %, 0 이상 100 이하 |
| `wind_speed` | m/s, 0 이상 |
| `precipitation_prob` | %, 0 이상 100 이하 |
| `precipitation_amount` | mm, 0 이상 |
| `parking_bike_tot_cnt` | 대, 0 이상. `hold_cnt` 초과는 실제 가능하므로 경고만 발생 |
| `predicted_rent_cnt`, `predicted_rtn_cnt` | 대, 0 이상 |
| `rebalance_bike_cnt`, `bike_cnt` | 대, 0 이상. 경로 stop은 0 초과 |
| `critical_remaining_min` | 분, 0 이상 |

## 현행 이름 교체 목록

| 현행 | 표준 이름 |
| --- | --- |
| `stations` | `station` |
| `lat`, `lon` | 해당 도메인의 `*_point` |
| `gu` | `gu_id` FK 또는 관계를 통한 `gu_nm` 조회 |
| `grid_nx`, `nx` | `weather_grid.weather_grid_x_no` |
| `grid_ny`, `ny` | `weather_grid.weather_grid_y_no` |
| `observed_at` | `base_dttm` |
| `batch_run_at` | `base_dttm` |
| `updated_at` | 실제 갱신 시각이면 `updated_dttm`, 최초 생성 시각이면 `created_dttm` |
| `predicted_return_cnt` | `predicted_rtn_cnt` |
| `forecast_points` | `station_demand_forecast` |
| `weather_current` | `weather_observation` |
| `rainfall`, `precip_amount` | `precipitation_amount` |
| `precip_prob` | `precipitation_prob` |
| `pty_type` | `precipitation_type_cd` |
| `sky_cond` | `sky_condition_cd` |
| `cultural_events` | `event` + `event_spot` |
| `title` | `event_name` |
| `category` | `event_type` |
| `place` | `event_spot_nm` |
| `start_date`, `end_date` | `event_start_dt`, `event_end_dt` |
| 자유 문자열을 담는 `is_free` | `event_fee_info`; 판별값은 nullable boolean `is_free` |
| `minutes_until_critical` | `critical_remaining_min` |
| `action_type`, `action` | `rebalance_action_type_cd` |
| `bike_qty` | `rebalance_bike_cnt` |
| `region` | `dispatch_center_id` FK |
| `status` | `route_status_cd` |
| `visit_order` | `visit_no` |
| `proposed_at` | `proposed_dttm` |
| `dispatched_at` | `dispatched_dttm` |
| `completed_at` | `completed_dttm` |
