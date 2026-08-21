# Airflow 데이터 흐름: 외부 API에서 대시보드까지

`loader/local_e2e.py`가 만드는 로컬 fixture 경로와 운영 DAG 경로는 다르다. 
운영 경로의 외부 관측값은 Collector가 API에서 받고, 모델 예측·긴급도·재배치 경로만 내부에서 계산한다.

## 1. 전체 흐름

| 단계 | 저장 위치 | 데이터가 받는 변화 | 다음 소비자 |
| --- | --- | --- | --- |
| 외부 원천 | 서울 열린데이터광장, 서울 실시간 도시데이터, 기상청 API Hub | 원천 JSON/XML 응답 | Collector |
| Bronze | S3/MinIO source-window별 JSON 조각 | 응답을 가능한 한 원형대로 보존한다. 페이지·격자별 조각과 실행 manifest를 함께 기록한다. | Collector 내부 재개·검증 |
| Silver | S3/MinIO `dt`/`hh` 파티션 Parquet | 타입 변환, 기상 category pivot, 인구 forecast flatten, 결측·범위 검증, 정책에 따른 drop/null 처리 | Normalizer, Nowcaster, 추론, Gold publisher, compaction |
| Derived Silver/Archive | S3/MinIO Parquet | 생활인구 추정·공간 보정, 일별 중복 제거·압축 | 추론과 향후 모델 학습 |
| Serving plan·inference authority | S3/MinIO content-addressed manifest/Parquet | 사용할 source snapshot과 모델을 URI+SHA로 고정하고 station별 12개 horizon 대여·반납량을 예측 | Gold finalize |
| Gold serving | RDS/PostgreSQL(PostGIS) | 검증된 projection을 현재 운영 테이블에 원자적으로 게시 | FastAPI |
| API | `/status`, `/stations`, `/forecast`, `/weather`, `/alerts`, `/routes` 등 | freshness와 anchor 정합성을 확인한 뒤 화면용 JSON으로 변환 | React 대시보드 |
| 화면 | 브라우저 메모리 | 지도, 예측 그래프, 긴급도 목록, 경로, 주변 행사로 표시 | 운영자 |

`Bronze → Silver`는 모든 Collector source가 공유하는 공통 과정이다. Collector는 fetch가 일부 실패하면 허용 누락률을 검사하고, 검증 실패 행은 제외하거나 quarantine에 남긴다. 품질 게이트를 넘지 못한 window에는 Silver를 쓰지 않는다.

## 2. DAG별 역할과 실행 결과

아래 주기는 `collector/sources/*.yaml`의 설명값이 아니라 **실제로 Airflow가 호출하는 주기**다.

| DAG | 실제 주기(KST) | 주요 태스크 | 만들어지는 결과 | RDS 직접 변경 |
| --- | --- | --- | --- | --- |
| `station_master` | 매일 03:00 | `collect_bike_station_master` | 대여소 ID, 주소, 위경도 Silver/source authority | 아니오. 이후 `realtime_5min` finalize가 `station`에 게시 |
| `weather_10min` | 10분마다 | 초단기 실황, 초단기예보 수집 | 격자별 실황·예보 Silver/source authority | 아니오. 이후 `realtime_5min` finalize가 예보를 선택·병합해 게시 |
| `weather_3h` | 3시간마다 정시 | 단기예보 수집 | 격자별 장기 범위 예보 Silver/source authority | 아니오 |
| `daily_population_and_events` | 매일 03:00 | 생활인구 수집→nowcast, 문화행사 수집→Gold, 체육시설 공연 수집→Gold | 생활인구 archive/nowcast와 `event` | 행사는 예, 생활인구는 아니오 |
| `realtime_5min` | 5분마다 | 대여이력·실시간 재고·실시간 인구 수집, 인구 보정, plan, 추론, Gold finalize, 긴급도, 경로 | 현재 serving projection 전체 | 예 |
| `daily_compaction` | 매일 04:30 | D-6 대여이력 24시간 재수집, recovery compaction | 학습·재현용 일별 Archive | 아니오 |

`daily_compaction`은 실시간 대시보드를 갱신하지 않는다. 대여이력의 장기대여 누락을 보강하고 작은 Silver 파일을 일별 Archive로 만드는 학습·재현 경로다.

## 3. 원천별 데이터 변화

| source ID | 외부 API와 실제 데이터 | Silver에서의 주요 변화 | 운영에서 사용되는 곳 | 최종 화면 |
| --- | --- | --- | --- | --- |
| `bike_station_master` | 서울 `bikeStationMaster`; 대여소 원천 ID, 주소, 좌표 | 타입·좌표 검증. 실시간 대여소 정보와 결합해 활성 대여소 projection의 기준이 됨 | Gold `station`; weather grid·dispatch center 연결 | 지도 위치, 대여소명·주소·거치대 수, 지역센터 |
| `bike_station_realtime` | 서울 `bikeList`; 현재 자전거 수, 거치대 수, 대여소명·좌표 | `stationId` 기준 중복·범위 검증. logical window를 재고 기준 시각으로 사용 | Gold `station_stock`; 최근 5개 source window는 긴급도 변화량 계산에 사용 | 지도 자전거 수, 현재 재고, 예측 재고 시작값, 우선순위 |
| `bike_rental_history` | 서울 `tbCycleRentData`; 대여·반납 완료 이력 | 한 시간 누적 응답을 Parquet으로 저장. 1시간 후 replay와 D-6 전체 replay로 늦은 반납을 보강하고 compaction 시 중복 제거 | 모델의 rolling 대여·반납 feature | 직접 표시되지 않고 대여·반납 예측값에 반영 |
| `population_realtime` | 서울 실시간 도시데이터 `citydata_ppltn`; 121개 POI 현재 인구와 향후 예측 범위 | 중첩 forecast를 `FCST_n_*`으로 펼침. Normalizer가 POI 인구를 250m 격자에 공간 배분 | 추론의 현재·미래 생활인구 feature | 직접 표시되지 않고 수요 예측에 반영 |
| `living_population_grid` | 서울 `Se250MSpopLocalResd`; 250m 격자·시간·연령별 생활인구 | `*` 마스킹을 결측으로 처리. Nowcaster가 실측을 실제 `YMD` 날짜 archive로 옮기고 과거 동일 요일/휴일로 D-3~D+3을 추정 | Normalizer의 격자 baseline | 직접 표시되지 않고 수요 예측에 반영 |
| `weather_ultra_short_live` | 기상청 `getUltraSrtNcst`; 격자별 온도·습도·강수·바람 실황 | category를 한 행으로 pivot하고 숫자·코드 검증 | 추론의 현재 날씨 feature, 일별 Archive | 현재 상태 자체는 직접 노출하지 않으며 예측에 반영 |
| `weather_ultra_short_forecast` | 기상청 `getUltraSrtFcst`; 가까운 미래 예보 | category pivot, 강수 범주를 mm 값으로 변환, 예보시각 정규화 | 가까운 시간대 Gold 날씨와 추론 feature | 대여소 상세의 향후 날씨 |
| `weather_short_term_forecast` | 기상청 `getVilageFcst`; 단기예보 | category pivot, 강수·하늘 코드를 공통 의미로 변환 | 초단기예보가 덮지 못하는 시간대의 Gold 날씨와 추론 feature | 대여소 상세의 향후 날씨 |
| `cultural_event` | 서울 `culturalEventInfo`; 문화행사 장소·기간·좌표 | 날짜·좌표 검증 후 source-scoped Gold publication | Gold `event` | 선택 대여소 반경 1.5km 주변 행사 |
| `performance_event` | 서울 `stadiumScheduleInfo`; 체육시설 행사 일정 | 저장소의 경기장 좌표와 결합해 공간 정보 생성 후 게시 | Gold `event` | 선택 대여소 주변 행사 |

## 4. `realtime_5min` 내부 변환 순서

| 순서 | 태스크 | 읽는 데이터 | 만드는 데이터·변환 | 실패 시 영향 |
| --- | --- | --- | --- | --- |
| 1 | `collect_bike_rental_history` | 해당 시각이 속한 시간대의 대여 완료 이력 API | Bronze/Silver와 source manifest | 추론이 실행되지 않음. 과거 replay side chain은 별도 시도 가능 |
| 1 | `collect_bike_station_realtime` | 현재 대여소 재고 API | 현재 tick의 authoritative 재고 snapshot | serving plan이 만들어지지 않음 |
| 1 | `collect_population_realtime` | 121개 POI 실시간 인구 API | 현재·향후 POI 인구 Silver | Normalizer와 추론이 실행되지 않음 |
| 2 | `run_normalizer` | 생활인구 nowcast baseline + 실시간 POI 인구 | 현재와 향후 최대 12시간의 보정된 격자 인구 Parquet | 추론이 실행되지 않음 |
| 2 | `prepare_serving_plan` | 현재 model release, station master, 현재 재고, 단기·초단기 날씨, 기존 Gold state | 사용할 source/model/대상 station을 immutable URI+SHA로 고정. `station`, `station_stock`, `weather_forecast` 후보도 준비 | 이후 태스크가 실행되지 않음. 준비 후 날씨 correction이 바뀌면 현재 구현의 finalize가 stale로 실패할 수 있음 |
| 3 | `run_inference` | plan에 고정된 모델, station profile, 대여이력, 보정 인구, 날씨 | 공통 지원 station마다 12 horizon의 `rental_pred_mean`, `return_pred_mean` 생성 | Gold는 이전 정상 projection을 유지 |
| 4 | `finalize_serving_release` | plan과 inference manifest의 exact URI+SHA | `station`, `station_stock`, `station_demand_forecast`, `weather_forecast`를 같은 release로 검증·원자 게시 | 네 projection 모두 새 tick으로 전환되지 않음 |
| 5 | `publish_station_urgency` | 새 station/stock/demand exact ref + 과거 재고 source window 5개 | 예측 재고, 부족·과잉 여부, 임계까지 남은 시간, 0~100 긴급도 계산 후 `station_urgency` 게시 | 우선순위와 새 route가 갱신되지 않음. 정확한 과거 window가 없으면 실패 |
| 6 | `publish_rebalance_route` | urgency exact ref + 배차센터·대여소 위치 + 진행 중 배차 상태 | 센터별 공급·회수 station을 묶고 방문 순서와 이동 수량을 계산해 proposed route 게시 | 이전 route 상태는 남고 새 proposed route가 생기지 않음 |
| side | `collect_bike_rental_history_replay_1h` | 한 시간 전 API를 `--force` 재조회 | 늦게 반납된 대여 기록으로 과거 Silver 보강 | 현재 tick serving chain을 막지 않음 |

여기서 **실제 관측값**은 API에서 온 대여소·재고·이력·인구·날씨·행사다. **내부 생성값**은 nowcast 생활인구, 공간 보정 인구, 대여·반납 예측, 예측 재고, 긴급도와 재배치 경로다. 운영 DAG는 `local_e2e.py`의 `capacity // 2`, 20°C, 인구 1000 같은 fixture를 만들지 않는다.

## 5. Gold/RDS에서 API와 화면으로

| Gold/RDS 테이블 | API endpoint | API가 추가로 확인·계산하는 것 | 대시보드 사용처 |
| --- | --- | --- | --- |
| `station` + `station_stock` + `dispatch_center` | `GET /stations` | 활성 station, station의 `last_seen_dttm`과 같은 재고, 10분 freshness, `현재 자전거/거치대` 비율 | 지도 마커·지역 필터·대여소 선택 |
| 위와 동일 | `GET /stations/{sta_id}` | station 존재·활성·fresh stock | 대여소명, 주소, 현재 자전거 수, 갱신 시각 |
| `station_demand_forecast` | `GET /status` | 전체 행이 하나의 공통 base인지, base가 10분 이내인지 | 헤더의 `예측 시각` |
| `station_demand_forecast` + `station_stock` | `GET /stations/{sta_id}/forecast` | station별 정확히 12시간, 공통 base, 미래 target, 같은 base의 fresh stock을 확인하고 누적 대여·반납으로 예측 재고 계산 | 대여·반납 예측 그래프, 재고 예측 그래프 |
| `weather_forecast` + `station.weather_grid_id` | `GET /stations/{sta_id}/weather` | 다음 정시부터 정확히 12행인지, 각 행이 45분 freshness 안인지 확인 | 대여소 상세의 주변 날씨 |
| `event` + `station` PostGIS geometry | `GET /stations/{sta_id}/events` | 반경 1.5km, 종료되지 않은 행사, 36시간 freshness, 거리 계산 | 주변 행사 탭과 지도 포커스 |
| `station_urgency` + station/stock/center | `GET /alerts` | urgency와 stock anchor 일치, 10분 freshness, 최신 correction 순서 확인 | 작업 우선순위 목록과 부족/회수 지도 필터 |
| `rebalance_route` + `rebalance_route_stop` | `GET /routes` | 센터·상태별 필터와 stop 집계 | 재배치 작업 경로 |
| `dispatch_center` | `GET /regions` | 활성 센터 좌표 | 지역센터 필터와 지도 기준점 |

## 6. 실패·freshness와 대시보드 동작

| 상황 | 저장소/RDS 상태 | API 응답 | 현재 화면 동작 |
| --- | --- | --- | --- |
| Collector가 실패 | 해당 source의 새 Silver authority 없음 | 이전 Gold가 freshness 안이면 계속 조회 가능 | 잠시 이전 값이 보일 수 있음 |
| inference 또는 finalize 실패 | 새 Gold release는 게시되지 않고 이전 정상 release가 남음 | 이전 demand/stock base가 10분을 넘으면 `/status`·`/forecast`는 503 또는 조회 불가 | `예측 시각 갱신 실패`; 프론트는 이전 예측을 지움 |
| urgency 실패 | 이전 urgency가 남음 | anchor/freshness 조건을 만족하지 않으면 `/alerts`가 빈 목록 | 작업 우선순위가 비거나 갱신 실패 표시 |
| station polling/API 자체 실패 | 브라우저에는 이전 station 상태가 있었음 | 네트워크/서버 오류 | 현재 프론트는 station·선택·예측·상세를 모두 지움 |
| 날씨가 45분 이상 오래됨 | 이전 weather projection은 RDS에 남음 | `/weather`가 503 `weather_not_ready` | 날씨 패널이 갱신 중 상태로 바뀜 |

현재 화면은 오래된 운영 판단을 막기 위해 fail-closed로 동작한다. 즉 RDS의 마지막 정상 데이터가 물리적으로 삭제된 것은 아니어도, freshness를 넘으면 API가 제공하지 않고 프론트도 이전 성공값을 유지하지 않는다.

## 7. DAG 밖에서 먼저 필요한 기준정보

| 기준정보 | 생성 방법 | 사용처 | 주의점 |
| --- | --- | --- | --- |
| `dispatch_center` | 승인된 `docs/gold/dispatch-center-seed.yaml`을 `gold_cli.py`로 일회성 bootstrap | station의 담당 지역, alerts·route 지역 구분 | `make up`만으로 외부 RDS에 자동 게시되지 않음 |
| `weather_grid` | 승인된 `docs/gold/weather-grid-seed.yaml`을 `gold_cli.py`로 일회성 bootstrap | station 좌표와 기상청 `(nx, ny)` 연결 | AWS에서는 seed version/effective time을 명시적으로 고정해야 함 |
| model serving release | 학습·승격 파이프라인 또는 검증된 기존 bundle 등록 | `prepare_serving_plan`, inference | rental/return 모델, station support, feature contract가 함께 고정되어야 함 |

따라서 새 환경의 최소 순서는 `DB schema → dispatch center/weather grid bootstrap → model serving release 등록 → station/weather/population 선행 수집 → realtime_5min`이다. 선행 DAG가 source authority를 만들고, `realtime_5min`이 그것을 Gold serving projection으로 전환한다.
