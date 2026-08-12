# 검증 상태 분류 기준

## 1. 분류 원칙

컬럼은 현재 Gold 생성 또는 모델 추론에 직접 필요한지에 따라 `필수`와 `일반`으로 구분한다.

- **필수**: 값이 없거나 사용할 수 없으면 현재 Gold 생성 또는 모델 추론 결과에 직접 영향을 주는 컬럼
- **일반**: 현재 Gold/추론에는 반드시 필요하지 않지만 향후 분석·Feature Mart·모델 개선에 활용할 수 있는 컬럼
- **시스템**: API에서 수집하는 값이 아니라 ETL이 생성·관리하는 컬럼. API 데이터 품질 문제와 분리하여 ETL 오류로 다룬다.

수집 자체가 성공한 뒤 검증 과정에서 결측·이상·타입오류가 발견되어도 API 재시도는 수행하지 않는다. API 호출 실패, 타임아웃, HTTP/원천 RESULT 오류 등 수집 자체가 진행되지 못한 경우에만 수집 단계에서 재시도한다.

검증 결과는 다음 7개 상태로 구분한다.

| 상태 | 의미 |
| --- | --- |
| 정상 | 값이 존재하고 타입 변환에 성공하며 정상 범위/규칙을 만족 |
| 필/결 | 필수 컬럼의 값이 수집되지 않음 |
| 필/이 | 필수 컬럼의 값이 타입은 맞지만 정상 범위 또는 정합성 규칙을 위반 |
| 필/타 | 필수 컬럼의 값이 존재하지만 정의된 타입으로 해석할 수 없음 |
| 일/결 | 일반 컬럼의 값이 수집되지 않음 |
| 일/이 | 일반 컬럼의 값이 타입은 맞지만 정상 범위 또는 정합성 규칙을 위반 |
| 일/타 | 일반 컬럼의 값이 존재하지만 정의된 타입으로 해석할 수 없음 |

판정 순서는 `결측 → 타입오류 → 이상치 → 정상` 순서로 한다. 같은 상태라도 특정 컬럼의 비즈니스 의미에 따라 별도 처리 정책을 둘 수 있으며, 컬럼별 정책이 공통 상태 정책보다 우선한다.

### 결측 판정 공통 규칙
- `NULL`, Python `None`, 빈 문자열 `""`은 기본적으로 결측으로 본다.
- 공백만 있는 문자열은 trim 후 빈 문자열이면 결측으로 본다.
- 원천 API가 별도 결측 sentinel 값을 사용하는 경우 해당 소스 config에 명시하여 결측으로 변환한다.
- 숫자 `0`, 문자열 `"0"`, boolean `false`는 해당 컬럼에서 정상값이 될 수 있으므로 결측으로 간주하지 않는다.

### 타입오류 판정 공통 규칙
- 원천이 문자열로 숫자를 제공하는 경우(`"15"`, `"31.6"`) 정의된 숫자 타입으로 안전하게 캐스팅되면 정상 타입으로 본다.
- 캐스팅할 수 없는 문자열(`"abc"` 등), 잘못된 날짜/시간 문자열, 잘못된 geometry는 타입오류로 본다.
- 타입 변환이 성공한 뒤 범위나 컬럼 간 관계를 위반하면 타입오류가 아니라 이상치로 분류한다.

---

# 컬럼별 검증 기준

## weather_grid

| 컬럼 | 중요도 | 결측 | 이상치 기준 | 타입오류 기준 | 근거 |
| --- | --- | --- | --- | --- | --- |
| weather_grid_id | 필수 | 값 없음 | `<= 0`, PK 중복 | smallint로 변환 불가 | 날씨 데이터와 대여소를 격자에 연결하는 식별자 |
| weather_grid_x_no | 필수 | 값 없음 | 57~63 밖이거나 `(x,y)` 조합이 서울 격자 목록에 없음 | smallint 변환 불가 | 서울권 기상 격자 매핑에 필요 |
| weather_grid_y_no | 필수 | 값 없음 | 124~129 밖이거나 `(x,y)` 조합이 서울 격자 목록에 없음 | smallint 변환 불가 | 서울권 기상 격자 매핑에 필요 |
| created_dttm | 시스템 | 생성되지 않음 | 유효하지 않은 생성 시각 | timestamp 변환 불가 | ETL/Master 생성 시각 |
| updated_dttm | 시스템 | 생성되지 않음 | `updated_dttm < created_dttm` | timestamp 변환 불가 | Master 변경 추적 |

추가 행 규칙: `(weather_grid_x_no, weather_grid_y_no)` 조합 중복은 이상치로 분류한다.

## weather_forecast

| 컬럼 | 중요도 | 결측 | 이상치 기준 | 타입오류 기준 | 근거 |
| --- | --- | --- | --- | --- | --- |
| forecast_dttm | 필수 | 값 없음 | `forecast_dttm < base_dttm` | timestamp 변환 불가 | 어떤 시점의 날씨 Feature인지 결정 |
| weather_grid_id | 필수 | 값 없음 | weather_grid Master에 없는 ID | smallint 변환 불가 | 대여소와 날씨를 공간적으로 연결 |
| temperature | 일반 | 값 없음 | 현재 Hard range 없음. 별도 Soft Warning 범위 사용 가능 | double precision 변환 불가 | 현재 필수 식별정보가 아니며 향후/모델 Feature로 활용 가능 |
| precipitation_prob | 일반 | 값 없음 | `< 0` 또는 `> 100` | double precision 변환 불가 | 강수확률의 이론적 백분율 범위 |
| precipitation_amount | 일반 | 값 없음 또는 원천 결측 표현 | `< 0` | 정의된 강수량 변환 규칙으로 double precision 변환 불가 | 강수량은 음수가 될 수 없음. 원천 특수 표현은 normalize 정책 필요 |
| humidity | 일반 | 값 없음 | `< 0` 또는 `> 100` | double precision 변환 불가 | 상대습도의 이론적 범위 |
| wind_speed | 일반 | 값 없음 | `< 0` | double precision 변환 불가 | 풍속의 크기는 음수가 될 수 없음 |
| base_dttm | 필수 | 값 없음 | `base_dttm > forecast_dttm` | timestamp 변환 불가 | 어떤 기준시각에 생성된 예보인지 식별 |
| created_dttm | 시스템 | 생성되지 않음 | 유효하지 않은 적재 시각 | timestamp 변환 불가 | Silver 생성 시각 추적 |

## main_spot

| 컬럼 | 중요도 | 결측 | 이상치 기준 | 타입오류 기준 | 근거 |
| --- | --- | --- | --- | --- | --- |
| main_spot_id | 필수 | 값 없음/빈 문자열 | 중복 ID | text로 정규화할 수 없음 | 주요장소 인구 및 공간 Mapping의 PK |
| main_spot_nm | 일반 | 값 없음/빈 문자열 | 별도 범위 없음 | text 처리 불가 | 현재 계산에는 ID와 공간정보가 핵심이며 이름은 표시·분석용 |
| main_spot_point | 필수 | 값 없음 | 유효하지 않은 EPSG:4326 Point 또는 polygon과 불일치 | Point geometry 파싱 불가 | 공간 매핑 기준 위치 |
| main_spot_area | 필수 | 값 없음 | `<= 0` | double precision 변환 불가 | 인구 밀도와 중첩 계산에 사용 |
| main_spot_polygon | 필수 | 값 없음 | invalid polygon | Polygon geometry 파싱 불가 | 인구 격자 중첩 계산에 직접 필요 |
| created_dttm | 시스템 | 생성되지 않음 | 유효하지 않은 생성 시각 | timestamp 변환 불가 | Master 생성 추적 |
| updated_dttm | 시스템 | 생성되지 않음 | `< created_dttm` | timestamp 변환 불가 | Master 변경 추적 |

## main_spot_living_population

| 컬럼 | 중요도 | 결측 | 이상치 기준 | 타입오류 기준 | 근거 |
| --- | --- | --- | --- | --- | --- |
| predicted_dttm | 필수 | 값 없음 | `< base_dttm` | timestamp 변환 불가 | 추론에 사용할 인구의 대상 시각 |
| main_spot_id | 필수 | 값 없음 | main_spot Master에 없는 ID | text 처리 불가 | 주요장소와 인구 연결 |
| congestion_lv | 일반 | 값 없음 | 서울시가 정의한 허용값 집합을 적용하기로 한 경우 집합 밖의 값 | text 처리 불가 | 현재 핵심 수치 Feature가 아니며 원천 분류값 보존 목적 |
| pop_min | 필수 | 값 없음 | `< 0` 또는 `pop_min > pop_avg` | integer 변환 불가 | 주요장소 인구 수준 산정에 직접 사용 |
| pop_max | 필수 | 값 없음 | `< 0` 또는 `pop_max < pop_avg` | integer 변환 불가 | 주요장소 인구 수준 산정에 직접 사용 |
| pop_avg | 필수 | 값 없음 | `< 0` 또는 `pop_avg < pop_min` 또는 `pop_avg > pop_max` | integer 변환 불가 | 주요장소 인구의 대표 Feature |
| male_rate | 일반 | 값 없음 | `< 0` 또는 `> 100` | double precision 변환 불가 | 미래 예측 행에는 미제공될 수 있고 향후 Feature 후보 |
| female_rate | 일반 | 값 없음 | `< 0` 또는 `> 100` | double precision 변환 불가 | 미래 예측 행에는 미제공될 수 있고 향후 Feature 후보 |
| age_0_rate | 일반 | 값 없음 | `< 0` 또는 `> 100` | double precision 변환 불가 | 연령 Feature 후보 |
| age_10_rate | 일반 | 값 없음 | `< 0` 또는 `> 100` | double precision 변환 불가 | 연령 Feature 후보 |
| age_20_rate | 일반 | 값 없음 | `< 0` 또는 `> 100` | double precision 변환 불가 | 연령 Feature 후보 |
| age_30_rate | 일반 | 값 없음 | `< 0` 또는 `> 100` | double precision 변환 불가 | 연령 Feature 후보 |
| age_40_rate | 일반 | 값 없음 | `< 0` 또는 `> 100` | double precision 변환 불가 | 연령 Feature 후보 |
| age_50_rate | 일반 | 값 없음 | `< 0` 또는 `> 100` | double precision 변환 불가 | 연령 Feature 후보 |
| age_60_rate | 일반 | 값 없음 | `< 0` 또는 `> 100` | double precision 변환 불가 | 연령 Feature 후보 |
| age_70_over_rate | 일반 | 값 없음 | `< 0` 또는 `> 100` | double precision 변환 불가 | 연령 Feature 후보 |
| resident_rate | 일반 | 값 없음 | `< 0` 또는 `> 100` | double precision 변환 불가 | 거주 특성 Feature 후보 |
| non_resident_rate | 일반 | 값 없음 | `< 0` 또는 `> 100` | double precision 변환 불가 | 거주 특성 Feature 후보 |
| base_dttm | 필수 | 값 없음 | `> predicted_dttm` | timestamp 변환 불가 | 인구 관측/예측의 기준 시각 |
| created_dttm | 시스템 | 생성되지 않음 | 유효하지 않은 적재 시각 | timestamp 변환 불가 | Silver 생성 추적 |

추가 행 규칙: `pop_min <= pop_avg <= pop_max`. 성별·연령·거주 비율 합계 검증은 해당 구성 컬럼들이 모두 제공된 행에 한해 허용 오차를 둔 Soft Validation으로 수행한다.

## station

| 컬럼 | 중요도 | 결측 | 이상치 기준 | 타입오류 기준 | 근거 |
| --- | --- | --- | --- | --- | --- |
| sta_id | 필수 | 값 없음 | `<= 0`, PK 중복 | integer 변환 불가 | 모든 대여소 재고·대여이력·Gold의 연결 키 |
| sta_nm | 일반 | 값 없음/빈 문자열 | 별도 범위 없음 | text 처리 불가 | 서비스 표시에는 유용하지만 계산 자체는 ID로 가능 |
| hold_cnt | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 거치 규모 Feature/분석용이며 현재 재고값 자체는 별도 존재 |
| sta_point | 필수 | 값 없음 | invalid Point, 서울 대상 영역 밖, 공간 Mapping 불일치 | Point geometry 파싱 불가 | 날씨·인구·행정구역 Mapping에 필수 |
| sta_addr | 일반 | 값 없음 | 별도 Hard range 없음 | text 처리 불가 | 좌표/ID로 핵심 처리 가능 |
| is_active | 필수 | 값 없음 | true/false 이외의 값 | boolean 변환 불가 | 비운영 대여소를 추론/서빙 대상에서 제외하는 기준 |
| created_dttm | 시스템 | 생성되지 않음 | 유효하지 않은 생성 시각 | timestamp 변환 불가 | Master 생성 추적 |
| updated_dttm | 시스템 | 생성되지 않음 | `< created_dttm` | timestamp 변환 불가 | Master 변경 추적 |
| weather_grid_id | 필수 | 값 없음 | weather_grid에 없는 ID 또는 공간상 불일치 | smallint 변환 불가 | 날씨 Feature 연결에 필요 |
| pop_grid_id | 필수 | 값 없음 | population_grid에 없는 ID 또는 공간상 불일치 | text 처리 불가 | 생활인구 Feature 연결에 필요 |
| dong_id | 일반 | 값 없음 | dong_master에 없는 ID 또는 공간상 불일치 | text 처리 불가 | 행정구역 표시·분석에 활용. 현재 핵심 추론 Feature 여부가 변경되면 필수로 승격 |

## station_stock

| 컬럼 | 중요도 | 결측 | 이상치 기준 | 타입오류 기준 | 근거 |
| --- | --- | --- | --- | --- | --- |
| base_dttm | 필수 | 값 없음 | 유효하지 않은 관측 시각 | timestamp 변환 불가 | 현재 재고의 시점 식별 |
| sta_id | 필수 | 값 없음 | station에 없는 ID | integer 변환 불가 | 어느 대여소의 재고인지 식별 |
| parking_bike_tot_cnt | 필수 | 값 없음 | `< 0` | integer 변환 불가 | 현재 자전거 재고는 추론 및 Gold 핵심값 |
| shared_rate | 일반 | 값 없음 | `< 0` | double precision 변환 불가 | 보조 정보이며 100 초과가 실제로 가능하므로 상한을 Hard rule로 두지 않음 |
| created_dttm | 시스템 | 생성되지 않음 | 유효하지 않은 적재 시각 | timestamp 변환 불가 | Silver 생성 추적 |

`parking_bike_tot_cnt > hold_cnt`는 실제 운영상 발생할 수 있으므로 Hard 이상치로 처리하지 않고 필요하면 Soft Warning으로 기록한다.

## event_spot

| 컬럼 | 중요도 | 결측 | 이상치 기준 | 타입오류 기준 | 근거 |
| --- | --- | --- | --- | --- | --- |
| event_spot_id | 필수 | 값 없음/빈 문자열 | 중복 ID | text 처리 불가 | event와 장소를 연결하는 PK |
| event_spot_nm | 일반 | 값 없음/빈 문자열 | 별도 범위 없음 | text 처리 불가 | 표시·분석용 |
| event_spot_point | 필수 | 값 없음 | invalid EPSG:4326 Point 또는 서비스 대상 지역 밖 | Point geometry 파싱 불가 | 가까운 대여소와 행사를 연결해 Gold/대시보드에 제공하기 위해 필요 |
| created_dttm | 시스템 | 생성되지 않음 | 유효하지 않은 생성 시각 | timestamp 변환 불가 | Master 생성 추적 |
| updated_dttm | 시스템 | 생성되지 않음 | `< created_dttm` | timestamp 변환 불가 | Master 변경 추적 |

## event

| 컬럼 | 중요도 | 결측 | 이상치 기준 | 타입오류 기준 | 근거 |
| --- | --- | --- | --- | --- | --- |
| event_id | 필수 | 값 없음 | `<= 0`, 중복 | integer 변환 불가 | 행사 식별 PK |
| event_spot_id | 필수 | 값 없음 | event_spot에 없는 ID | text 처리 불가 | 행사와 공간을 연결 |
| event_name | 일반 | 값 없음/빈 문자열 | 별도 범위 없음 | text 처리 불가 | 표시·분석용이며 공간·일정 Feature 계산 자체에는 다른 키 사용 가능 |
| event_start_dt | 필수 | 값 없음 | `> event_end_dt` | date 변환 불가 | 특정 시점에 행사 영향이 있는지 판단하는 핵심값 |
| event_end_dt | 필수 | 값 없음 | `< event_start_dt` | date 변환 불가 | 행사 유효기간 판단의 핵심값 |
| event_schedule | 일반 | 값 없음 | 별도 Hard range 없음 | text 처리 불가 | 상세 일정은 비정형이며 향후 정교한 Feature에 사용 가능 |
| event_type | 일반 | 값 없음 | 허용 코드집합을 명시한 경우 집합 밖의 값 | text 처리 불가 | 행사 유형 Feature/분석 후보 |
| event_url | 일반 | 값 없음 | URL 검증을 적용하는 경우 유효하지 않은 URL | text 처리 불가 | 부가 서비스 정보 |
| event_image_url | 일반 | 값 없음 | URL 검증을 적용하는 경우 유효하지 않은 URL | text 처리 불가 | 부가 서비스 정보 |
| created_dttm | 시스템 | 생성되지 않음 | 유효하지 않은 적재 시각 | timestamp 변환 불가 | Silver 생성 추적 |

## gu_master

| 컬럼 | 중요도 | 결측 | 이상치 기준 | 타입오류 기준 | 근거 |
| --- | --- | --- | --- | --- | --- |
| gu_id | 필수 | 값 없음/빈 문자열 | 중복, 서울 자치구 Master와 불일치 | text 처리 불가 | 행정구역 Mapping 식별자 |
| gu_nm | 일반 | 값 없음/빈 문자열 | 서울 자치구명 목록과 불일치 | text 처리 불가 | 표시·검증용 이름 |
| created_dttm | 시스템 | 생성되지 않음 | 유효하지 않은 생성 시각 | timestamp 변환 불가 | Master 생성 추적 |
| updated_dttm | 시스템 | 생성되지 않음 | `< created_dttm` | timestamp 변환 불가 | Master 변경 추적 |
| gu_polygon | 필수 | 값 없음 | invalid Polygon | Polygon geometry 파싱 불가 | 행정구역 공간 연산에 필요 |

## dong_master

| 컬럼 | 중요도 | 결측 | 이상치 기준 | 타입오류 기준 | 근거 |
| --- | --- | --- | --- | --- | --- |
| dong_id | 필수 | 값 없음/빈 문자열 | 중복 | text 처리 불가 | 행정동 Mapping 식별자 |
| dong_nm | 일반 | 값 없음/빈 문자열 | 별도 Hard range 없음 | text 처리 불가 | 표시·검증용 이름 |
| created_dttm | 시스템 | 생성되지 않음 | 유효하지 않은 생성 시각 | timestamp 변환 불가 | Master 생성 추적 |
| updated_dttm | 시스템 | 생성되지 않음 | `< created_dttm` | timestamp 변환 불가 | Master 변경 추적 |
| dong_polygon | 필수 | 값 없음 | invalid Polygon 또는 gu_polygon과 심각한 공간 불일치 | Polygon geometry 파싱 불가 | 대여소 행정구역 Mapping에 사용 |
| gu_id | 필수 | 값 없음 | gu_master에 없는 ID | text 처리 불가 | 동-구 관계 유지 |

## population_grid

| 컬럼 | 중요도 | 결측 | 이상치 기준 | 타입오류 기준 | 근거 |
| --- | --- | --- | --- | --- | --- |
| pop_grid_id | 필수 | 값 없음/빈 문자열 | 중복 | text 처리 불가 | 격자 생활인구와 대여소를 연결하는 PK |
| pop_grid_point | 일반 | 값 없음 | invalid Point 또는 polygon 바깥 | Point geometry 파싱 불가 | 대표 위치·시각화용. 중첩 계산은 polygon 사용 |
| pop_grid_polygon | 필수 | 값 없음 | invalid Polygon | Polygon geometry 파싱 불가 | 주요장소 중첩 보정과 대여소 Mapping에 직접 사용 |
| created_dttm | 시스템 | 생성되지 않음 | 유효하지 않은 생성 시각 | timestamp 변환 불가 | Master 생성 추적 |
| updated_dttm | 시스템 | 생성되지 않음 | `< created_dttm` | timestamp 변환 불가 | Master 변경 추적 |

## population_grid_main_spot

| 컬럼 | 중요도 | 결측 | 이상치 기준 | 타입오류 기준 | 근거 |
| --- | --- | --- | --- | --- | --- |
| pop_grid_id | 필수 | 값 없음 | population_grid에 없는 ID | text 처리 불가 | 중첩 보정 대상 격자 식별 |
| main_spot_id | 필수 | 값 없음 | main_spot에 없는 ID | text 처리 불가 | 중첩 보정 대상 주요장소 식별 |
| overlap_rate | 필수 | 값 없음 | `<= 0` 또는 `> 1` | double precision 변환 불가 | 주요장소/격자 중첩 보정 계산에 직접 사용 |
| created_dttm | 시스템 | 생성되지 않음 | 유효하지 않은 생성 시각 | timestamp 변환 불가 | Mapping 생성 추적 |

## living_population_per_population_grid

| 컬럼 | 중요도 | 결측 | 이상치 기준 | 타입오류 기준 | 근거 |
| --- | --- | --- | --- | --- | --- |
| base_dttm | 필수 | 값 없음 | 유효하지 않은 관측 시각 | timestamp 변환 불가 | 4주 평균 및 시간대별 인구 계산 기준 |
| pop_grid_id | 필수 | 값 없음 | population_grid에 없는 ID | text 처리 불가 | 격자별 인구 식별 |
| living_pop_tot | 필수 | 값 없음 | `< 0` | integer 변환 불가 | 현재 격자 인구 추정/예측의 핵심값 |
| male_00_09 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_10_14 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_15_19 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_20_24 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_25_29 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_30_34 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_35_39 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_40_44 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_45_49 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_50_54 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_55_59 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_60_64 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_65_69 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_70_over | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_00_09 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_10_14 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_15_19 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_20_24 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_25_29 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_30_34 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_35_39 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_40_44 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_45_49 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_50_54 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_55_59 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_60_64 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_65_69 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_70_over | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| created_dttm | 시스템 | 생성되지 않음 | 유효하지 않은 적재 시각 | timestamp 변환 불가 | Silver 생성 추적 |

추가 행 규칙: 모든 성·연령 세부값이 존재하는 경우 세부합과 `living_pop_tot`의 차이를 허용 오차 기반 Soft Validation으로 검사한다.

## predicted_living_population_per_population_grid

| 컬럼 | 중요도 | 결측 | 이상치 기준 | 타입오류 기준 | 근거 |
| --- | --- | --- | --- | --- | --- |
| predicted_dttm | 필수 | 값 없음 | `< base_dttm` | timestamp 변환 불가 | 추론에서 사용할 인구의 대상 시각 |
| pop_grid_id | 필수 | 값 없음 | population_grid에 없는 ID | text 처리 불가 | 격자별 예측 인구 식별 |
| base_dttm | 필수 | 값 없음 | `> predicted_dttm` | timestamp 변환 불가 | 예측 생성 기준 시각 |
| living_pop_tot | 필수 | 값 없음 | `< 0` | integer 변환 불가 | 현재 추론에 사용하는 격자 예측 인구 핵심값 |
| male_00_09 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_10_14 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_15_19 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_20_24 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_25_29 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_30_34 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_35_39 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_40_44 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_45_49 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_50_54 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_55_59 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_60_64 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_65_69 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| male_70_over | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_00_09 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_10_14 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_15_19 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_20_24 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_25_29 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_30_34 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_35_39 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_40_44 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_45_49 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_50_54 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_55_59 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_60_64 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_65_69 | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| female_70_over | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 성·연령 Feature 후보 |
| created_dttm | 시스템 | 생성되지 않음 | 유효하지 않은 적재 시각 | timestamp 변환 불가 | Silver 생성 추적 |

추가 행 규칙: 모든 성·연령 세부값이 존재하는 경우 세부합과 `living_pop_tot`의 차이를 허용 오차 기반 Soft Validation으로 검사한다.

## rental

| 컬럼 | 중요도 | 결측 | 이상치 기준 | 타입오류 기준 | 근거 |
| --- | --- | --- | --- | --- | --- |
| rent_id | 필수 | 값 없음 | `<= 0`, 중복 | bigint 변환 불가 | 대여 이력 식별 및 중복 제거에 필요 |
| bike_id | 일반 | 값 없음/빈 문자열 | 별도 Hard range 없음 | text 처리 불가 | 개별 자전거 분석에는 필요하지만 현재 대여소 수요 Feature는 시간·대여소 중심으로 생성 가능 |
| rent_dttm | 필수 | 값 없음 | `rent_dttm > rtn_dttm` | timestamp 변환 불가 | 대여 수요의 발생 시각 핵심값 |
| rtn_dttm | 필수 | 값 없음 | `< rent_dttm` | timestamp 변환 불가 | 현재 사용하는 원천은 반납 완료 후 확정되는 대여이력이며 반납 기반 집계/검증에 필요 |
| use_min | 일반 | 값 없음 | `< 0` | integer 변환 불가 | 향후 이용행태 Feature/분석용 |
| use_dst | 일반 | 값 없음 | `< 0` | bigint 변환 불가 | 향후 이용행태 Feature/분석용 |
| usr_cls_cd | 일반 | 값 없음 | 정의된 사용자분류 코드셋 밖의 값 | text 처리 불가 | 사용자 특성 Feature 후보 |
| sex_cd | 일반 | 값 없음 | 정의된 성별 코드셋 밖의 값 | text 처리 불가 | 인구통계 Feature 후보 |
| birth_year | 일반 | 값 없음 | 대여연도보다 미래이거나 실제 데이터 분석 후 정한 비현실적 출생연도 범위 밖 | smallint 변환 불가 | 인구통계 Feature 후보이며 원천 특수값 가능 |
| bike_type_cd | 일반 | 값 없음 | 정의된 자전거유형 코드셋 밖의 값 | text 처리 불가 | 자전거 유형 Feature 후보 |
| created_dttm | 시스템 | 생성되지 않음 | 유효하지 않은 적재 시각 | timestamp 변환 불가 | Silver 생성 추적 |
| rent_sta_id | 필수 | 값 없음 | 유효한 station 식별 규칙을 만족하지 않음. 물리 FK 적용 여부는 과거 폐쇄 대여소 확인 후 결정 | integer 변환 불가 | 수요가 발생한 대여소를 집계하는 핵심값 |
| rtn_sta_id | 필수 | 값 없음 | 유효한 station 식별 규칙을 만족하지 않음. 물리 FK 적용 여부는 과거 폐쇄 대여소 확인 후 결정 | integer 변환 불가 | 공급/반납 흐름 분석의 핵심값 |

`use_min`과 `rtn_dttm - rent_dttm`의 차이는 원천 계산·반올림 차이가 있을 수 있으므로 Hard 이상치가 아니라 Soft Validation 후보로 둔다. `rent_sta_id == rtn_sta_id`는 정상적으로 가능한 값이다.

## code_group

| 컬럼 | 중요도 | 결측 | 이상치 기준 | 타입오류 기준 | 근거 |
| --- | --- | --- | --- | --- | --- |
| code_group_id | 필수 | 값 없음 | `<= 0`, 중복 | integer 변환 불가 | 공통 코드 그룹 식별 PK |
| code_group_nm | 일반 | 값 없음/빈 문자열 | 별도 Hard range 없음 | text 처리 불가 | 사람이 코드를 관리·해석하기 위한 설명값 |

## code

| 컬럼 | 중요도 | 결측 | 이상치 기준 | 타입오류 기준 | 근거 |
| --- | --- | --- | --- | --- | --- |
| code_id | 필수 | 값 없음 | `<= 0`, 중복 | integer 변환 불가 | 내부에서 코드 Row를 식별하는 인공키 |
| code_group_id | 필수 | 값 없음 | code_group에 없는 ID | integer 변환 불가 | 코드가 속한 그룹 연결 |
| code_value | 필수 | 값 없음/빈 문자열 | 같은 `(code_group_id, code_value)` 중복 | text 처리 불가 | 원천 코드와 Silver 값을 매핑하는 실제 코드값 |
| code_nm | 일반 | 값 없음/빈 문자열 | 별도 Hard range 없음 | text 처리 불가 | 사람이 원천 코드를 해석하기 위한 표시명 |

`code_id`는 원천 코드값이 아니라 내부 인공키로 사용한다. 원천 코드값은 `code_value`에 저장하며 `(code_group_id, code_value)`는 UNIQUE를 권장한다.

