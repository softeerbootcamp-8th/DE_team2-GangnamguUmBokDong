# 시스템 전체 데이터 흐름

이 문서는 외부 API 수집부터 대시보드 표시까지 시스템이 사용하는 데이터의 위치,
처리 방식과 사용 이유를 설명한다. 기본 모델 계약은 `w60_e40_t20`이다.

- rolling window: 60분
- embargo: 40분
- 모델 grid 및 학습 anchor: 20분
- target: anchor 이후 60분의 대여/반납 건수
- 운영 호출 주기: 5분

## 0. 시스템 전체 데이터 흐름

이 절은 **외부 API → S3 Bronze/Silver → 추론 → Gold PostGIS → API →
대시보드**까지 운영 시스템이 사용하는 데이터의 위치, 처리 방식, 사용 이유를
한 번에 설명한다. 아래의 시간은 Airflow logical time을 기준으로 하며, 운영의
핵심 체인은 `realtime_5min` DAG다.

```text
서울시 Open API / 기상청 API
        ↓ collector: 원문 보존 + 스키마/품질 검증
S3 Bronze(JSON) ──────────────── 원본 감사·재처리
        ↓ 유효 행 캐스팅/정규화
S3 Silver(Parquet) ───────────── 운영 계산 입력
        ↓ normalizer + serving plan
정류소·재고·날씨 준비 + 모델 입력 고정
        ↓ rental/return LightGBM 추론(정류소별 미래 1~12시간)
S3 inference output(immutable Parquet + manifest)
        ↓ loader finalize: 동일 base_dttm을 한 트랜잭션으로 게시
Gold PostgreSQL/PostGIS
        ↓ FastAPI
React 대시보드(지도·예측·날씨·행사·긴급도·재배치 경로)
```

### 1 외부 API와 수집 데이터

| `source_id` | 외부 API/서비스 | 수집 주기 | 주요 원본 값 | 시스템에서 필요한 이유 |
|---|---|---:|---|---|
| `bike_rental_history` | 서울 열린데이터광장 `tbCycleRentData` | 5분, 최근 1시간 재조회 | 대여/반납 시각·대여소, 자전거 ID | 직전 수요 lag 계산과 과거 대여/반납 target 생성 |
| `bike_station_realtime` | 서울 열린데이터광장 `bikeList` | 5분 | 대여소 ID, 현재 자전거 수, 거치대 수, 좌표 | 현재 재고, 품절 여부, 활성 대여소 판정 및 예측 재고 시작값 |
| `population_realtime` | 서울 실시간 도시데이터 `citydata_ppltn` | 5분 | 121개 POI의 현재·예측 인구 | 250m 생활인구의 최신 시점 보정 |
| `bike_station_master` | 서울 열린데이터광장 `bikeStationMaster` | 1일 | 대여소 ID, 주소, 위·경도 | 정류소의 안정적인 위치·주소 dimension과 공간 매핑 |
| `weather_ultra_short_live` | 기상청 API허브 초단기실황 | 10분 DAG | 격자별 기온·1시간 강수·습도·풍속 | 현재/과거 시점의 날씨 입력과 예보 결측 fallback |
| `weather_ultra_short_forecast` | 기상청 API허브 초단기예보 | 10분 DAG | 가까운 미래 격자 날씨 | 가까운 미래 horizon의 모델 입력 및 대시보드 날씨 |
| `weather_short_term_forecast` | 기상청 API허브 단기예보 | 3시간 | 최대 12시간 이후까지 격자 날씨 | 미래 12개 정시 예측에 필요한 날씨와 대시보드 표시 |
| `living_population_grid` | 서울 열린데이터광장 `Se250MSpopLocalResd` | 1일 | 250m 격자별 시간대 생활인구 | 일별 기준 인구 및 nowcast/profile 생성 |
| `cultural_event` | 서울 열린데이터광장 `culturalEventInfo` | 1일 | 행사명·기간·장소·좌표 | 대여소 반경 1.5km의 현재·예정 행사 표시 |
| `performance_event` | 서울 열린데이터광장 `stadiumScheduleInfo` | 1일 | 체육시설 공연 일정 | 좌표 보강 후 주변 행사 표시 |

수집 설정의 실제 URL 서비스명, 컬럼 타입, 결측/이상치 정책은
`collector/sources/*.yaml`이 단일 기준이다. API 키는 코드나 S3 객체에 넣지 않고
실행 환경의 secret/env로 주입한다.

### 2 Bronze, Silver, Archive의 위치와 역할

| 계층 | S3 key 형식 | 형식·쓰기 방식 | 보관 내용 | 필요한 이유 |
|---|---|---|---|---|
| Bronze | `bronze/<source_id>/dt=YYYY-MM-DD/hh=HH/<HHMM>/part=<chunk>.json.gz` | gzip JSON, 수집 window의 API page/chunk별 저장 | API 응답의 원형에 가까운 레코드 | 파서/정책이 바뀌어도 원본을 다시 검증·정제하고 장애를 감사하기 위해 |
| Silver | `silver/<source_id>/dt=YYYY-MM-DD/hh=HH/<HHMM>/sha256=<digest>.parquet` | Parquet, content-addressed immutable object | 선언된 타입으로 캐스팅하고 품질 정책을 통과한 행 + window metadata | 추론·normalizer·loader가 JSON 파싱 없이 작고 일관된 스키마를 읽기 위해 |
| Source authority | `source_snapshot_manifest/<source_id>/dt=YYYY-MM-DD/hh=HH/logical=<UTC>/revision=<n>.json` | immutable JSON manifest | 정확한 Silver URI, SHA-256, logical time, correction revision | “가장 최신처럼 보이는 파일”이 아니라 검증된 정확한 입력 revision을 재현하기 위해 |
| 일별 Archive | `archive/<source_id>/dt=YYYY-MM-DD.parquet` | 하루치 Parquet, 중복 제거/확정 | 학습·백필용 historical fact | 5분 snapshot 수천 개를 매번 스캔하지 않고 과거 학습 범위를 재현하기 위해 |

collector는 먼저 Bronze를 기록하고, YAML의 컬럼 계약에 따라 required 결측 행 제거,
optional 결측 유지, 범위 밖 값 처리, 타입 변환을 수행해 Silver를 만든다. 성공한
Silver만 source snapshot manifest가 가리킨다. 따라서 downstream은 prefix에서 파일을
임의 선택하지 않고 manifest의 exact URI와 SHA-256을 읽는다.

정제 이후 추가 파생 데이터는 다음과 같다.

| 파생 데이터 | 위치 | 생성 방식 | 쓰임 |
|---|---|---|---|
| `station_master_enriched` | `silver/station_master_enriched/dt=.../hh=.../HHMM.parquet` | master 좌표 + 실시간 대여소 + 250m/기상 격자 매핑 | `station_id → station_no`, capacity, 좌표, `grid_id`, 기상 격자 연결 |
| `living_population_normalized` | `silver/living_population_normalized/dt=.../hh=.../HHMM.parquet` | 일별 250m 기준 인구를 121개 실시간 POI 변화로 보정 | 추론 시점의 `pop_total` |
| 생활인구 nowcast | `silver/living_population_grid/dt=.../hh=00/nowcast.parquet` | 1~4주 전 같은 요일 패턴으로 미공개 날짜 추정 | 최신 공식 생활인구가 늦게 공개되어도 운영 지속 |
| 일별 compact fact | `archive/<source_id>/dt=...parquet` | daily compaction, 대여이력은 누적 API 중복 제거 | feature mart 재생성·재학습·검증 |

### 3 모델 실행 전에 필요한 데이터와 이유

운영 추론은 모델 파일만 있어서는 실행할 수 없다. 모델이 학습 때 사용한 feature
의미와 순서, 정류소 category, fallback 통계를 동일하게 재현해야 한다.

| 사전 데이터/계약 | 대표 위치 | 모델 입력에 넣는 값 | 필요한 이유·결측 시 동작 |
|---|---|---|---|
| rental/return 모델 pair | `models/serving-release/current.json` → immutable release/model manifest | LightGBM booster, conformal correction | 두 모델과 부속 파일이 같은 검증된 release인지 고정한다. rental/return 중 하나만 바뀐 혼합 상태를 금지한다. |
| 정류소 category | release가 가리키는 `station_categories` artifact | `station_id`를 학습 당시 정수 `station_no` category로 변환 | category 순서가 달라지면 같은 숫자가 다른 대여소로 해석되므로 필수다. |
| 정류소 enriched master | 최신 `silver/station_master_enriched/...` 및 serving plan의 station projection | `station_no`, `capacity`, `lat`, `lon`, 인구/기상 격자 | 모델의 공간·규모 feature와 Gold station을 같은 ID 집합으로 맞춘다. |
| 최근 대여/반납 이력 | `silver/bike_rental_history/...` | rental: `[T-100분,T-40분)`에 관측 가능했던 `rental_lag_1h`; return: 1시간 전 `return_lag_1h` | 직전 실적이 가장 직접적인 수요 신호다. 대여는 반납 완료 뒤 API에 나타나는 지연을 고려한다. 없으면 station profile 사용. |
| station fallback profile | release artifact의 `station_hourly_profile.parquet` | `station_no × minute × dow × month` 평균 | 실시간 이력 window가 없을 때 평소 대여/반납량으로 lag를 대체한다. 정상 관측값 0은 결측으로 취급하지 않는다. |
| 날씨 실황/예보 | `silver/weather_*`의 pinned snapshot | `temp`, `precip` | target 시각이 미래면 단기/초단기예보를 우선하고, 없으면 최근 실황으로 fallback한다. |
| 정규화 생활인구 | `silver/living_population_normalized/...` | 정류소의 250m `grid_id`에 대응하는 `pop_total` | 상권·업무지·주거지의 시간대별 유동량 차이를 반영한다. 없으면 인구 profile 사용. |
| population fallback profile | release artifact의 `population_hourly_profile.parquet` | `grid_id × hour × dow` 평균 | 실시간 인구가 없을 때 평소 격자 인구로 대체한다. |
| 실시간 재고 | `silver/bike_station_realtime/...` | 대여 관측 가능량 보정용 `rental_exposure`와 현재 재고 | 품절로 관측된 수요가 실제 잠재수요보다 작아지는 편향을 보정한다. 대시보드 예측 재고의 시작값이기도 하다. |
| 달력 | `ml_core.holidays_kr`와 target 시각 | `minute`, `dow`, `is_holiday`, `day` | 출퇴근·요일·공휴일·장기 계절 추세를 표현한다. |

모델에 실제로 전달되는 feature 순서는 코드 계약
`libs/ml_core/model_contract.py`가 정한다.

| 공통 feature (두 모델 동일) | rental 전용 | return 전용 |
|---|---|---|
| `station_no`, `capacity`, `lat`, `lon`, `temp`, `precip`, `pop_total`, `minute`, `dow`, `is_holiday`, `day`, `horizon` | `rental_lag_1h` (`rental_exposure`는 LightGBM offset 계산용 보조값) | `return_lag_1h` |

`anchor_ts`는 현재 5분 tick이고, `horizon=1..12`는 각각 1~12시간 뒤
`target_ts`를 뜻한다. lag는 모든 horizon에서 같은 anchor 기준으로 한 번 계산하고,
날씨·인구·달력은 각 target 시각 기준으로 다시 넣는다. 이전 예측값을 다음 입력으로
재사용하지 않으므로 재귀 오차가 누적되지 않는다.

### 4 추론 산출물과 Gold 게시

| 단계 | 저장 위치/테이블 | 저장 내용 | 방식과 이유 |
|---|---|---|---|
| Serving plan 준비 | S3의 content-addressed plan JSON | 정확한 source manifest, model release, 대상 station ID 집합, 이전 Gold state | 추론 도중 입력이 갱신되어도 한 실행 안에서는 같은 데이터만 보게 한다. 이 단계는 아직 Gold를 변경하지 않는다. |
| 추론 입력 보존 | `inference/inputs/source-key-sha256=.../sha256=...` | 실제로 GET한 S3 입력 bytes의 immutable copy | 실행 재현과 입력 증거 보존 |
| 추론 결과 | `inference/outputs/sha256=<digest>.parquet` | `station_id`, `date`, `hour`, `minute`, `horizon`, `rental_pred_mean`, `return_pred_mean` | 정류소 수 × 12행이 모두 성공한 완전한 결과만 authority 후보가 된다. |
| 추론 manifest | `inference/manifests/sha256=<digest>.json` | plan/model/input/output SHA, 행 수, revision, 상태 | partial 결과가 Gold로 승격되는 것을 막고 exact replay를 가능하게 한다. |
| Coordinated Gold release | PostgreSQL/PostGIS `station`, `station_stock`, `station_demand_forecast`, `weather_forecast` | 정류소, 현재 재고, 미래 12시간 대여/반납량, 미래 12시간 날씨 | finalize가 같은 `base_dttm`의 네 projection을 한 DB transaction으로 게시한다. source/plan drift가 있으면 전부 중단한다. |
| 운영 파생 Gold | `station_urgency`, `rebalance_route`, `rebalance_route_stop` | 0~100 긴급도, 공급/회수 필요, 센터별 제안 경로·순서·자전거 수 | release 이후 현재 재고와 예측 수요를 계산해 재배치 의사결정으로 바꾼다. |
| 일별 행사 Gold | `event` | 행사 위치·기간·출처 | 대여소와 PostGIS 거리 계산을 하기 위해 저장한다. |

Gold의 `station_demand_forecast`에는 화면에 필요한 평균 예측을 음수가 아닌 정수로
게시한다. API는 같은 `base_dttm`의 `station_stock`과 결합해 현재 재고에서 시간별
예상 대여를 빼고 예상 반납을 더한 **예측 재고**와 거치율을 계산한다. 모델이 내부에서
계산하는 P10/P50/P90은 현재 Gold/API 계약에는 게시하지 않는다.

### 5 서빙 API와 대시보드에 표시되는 데이터

FastAPI(`apps/api`)는 S3를 직접 조회하지 않고 Gold PostgreSQL/PostGIS만 읽는다.
React 대시보드(`apps/web`)는 다음 API를 호출한다.

| 대시보드 기능 | API | 읽는 Gold 데이터 | 최종 표시 값 |
|---|---|---|---|
| 정류소 지도/목록 | `GET /stations` | `station` + 같은 anchor의 `station_stock` + `dispatch_center` | 대여소명·좌표·센터/지역·현재 자전거 수·거치대 수·현재 거치율 |
| 정류소 상세 | `GET /stations/{sta_id}` | 위와 동일 + 주소 | 선택 대여소의 상세 상태 |
| 12시간 수요/재고 차트 | `GET /stations/{sta_id}/forecast` | `station_demand_forecast` + 같은 anchor의 재고 | 시간별 예상 대여·반납 건수, 계산된 예상 자전거 수와 예상 거치율 |
| 12시간 날씨 | `GET /stations/{sta_id}/weather?hours=12` | station의 `weather_grid_id` + `weather_forecast` | 시각, 하늘/강수 유형, 기온, 강수확률·강수량, 습도, 풍속 |
| 주변 행사 | `GET /stations/{sta_id}/events` | `station` + `event` | 반경 1.5km 내 행사명·장소·기간·거리 |
| 긴급 알림 | `GET /alerts` | `station_urgency` + `station` | 긴급도 점수, 위험까지 남은 분, 공급/회수 필요 유형 |
| 재배치 운영 | `GET /routes`, `GET /routes/{id}` 및 상태 변경 POST | `rebalance_route`, `rebalance_route_stop`, `dispatch_center`, `station` | 센터별 경로, 방문 순서, pickup/dropoff 수량, proposed/dispatched/completed/cancelled 상태 |
| 데이터 준비 상태 | `GET /status` | `station_demand_forecast`의 공통 `base_dttm` | 대시보드가 보여주는 예측의 기준 시각 및 freshness 판정 |

API는 stale/misaligned 데이터를 조용히 섞지 않는다. 현재 재고·수요예측은 보통
10분, 날씨는 45분, 행사는 36시간 freshness를 적용하며, 예측과 재고의
`base_dttm`이 다르거나 미래 12개 시점이 완전하지 않으면 503을 반환한다. 즉 화면에
값이 보인다면 최소한 같은 release 기준시각과 완전성 검사를 통과한 데이터다.

### 6 운영 체인과 장애 시 경계

| 순서 | `realtime_5min` task | 성공 조건/장애 시 동작 |
|---:|---|---|
| 1 | rental history, station realtime, population realtime 수집 | source별 Bronze/Silver와 authority manifest 생성. required 품질 기준 실패 시 해당 source 실패 |
| 2 | population normalizer | 일별 격자 인구와 실시간 POI 보정 결과 생성. 현 DAG에서는 성공해야 추론 진행 |
| 3 | weather manifest 대기 | 최대 30초 대기. 새 날씨가 늦으면 이전의 유효한 weather snapshot으로 plan 준비 가능 |
| 4 | serving plan 준비 | station/realtime/weather/model/ID 집합을 exact URI+SHA로 pin. Gold 변경 없음 |
| 5 | inference | 모든 대상 정류소 × 12 horizon 완전 성공 필요. partial이면 authority 게시 금지 |
| 6 | finalize serving release | plan과 입력 drift 재검증 후 4개 Gold projection 원자 게시. 하나라도 틀리면 전체 rollback |
| 7 | urgency → routes | 같은 release의 재고·수요로 긴급도와 센터별 재배치 경로 게시 |

학습 경로와 운영 경로의 차이도 구분해야 한다. 학습은 일별 `archive/`와 최신
station dimension으로 2025 feature mart를 만들지만, 운영은 매 5분 최신 Silver와
미리 게시된 serving model release를 사용한다. 운영 중 매 5분마다 모델을 다시
학습하거나 release를 다시 게시하지 않는다.

S3 key는 AWS와 로컬 MinIO에서 동일하다. 실제 bucket은 실행 환경의 `S3_BUCKET`을
사용하고, 로컬 MinIO만 `S3_ENDPOINT_URL=http://localhost:9000`으로 endpoint를
바꾼다. Raw CSV/ZIP은 이름이나 확장자만 바꿔 S3 계층에 올리지 않고, 반드시
source별 변환 코드로 컬럼·타입·날짜·중복·마스킹을 정규화해야 한다.
