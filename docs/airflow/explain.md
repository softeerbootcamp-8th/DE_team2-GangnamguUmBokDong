# Airflow 운영 구조와 데이터 흐름

이 문서는 현재 코드에 구현된 Airflow DAG를 설명한다. 기준 코드는 `airflow/dags/`, `airflow/config/schedules.py`, `airflow/config/sources.py`와 `airflow/orchestration/`이다.

## 한눈에 보기

```text
외부 API
  → Collector: Bronze JSON + Silver Parquet + source manifest
  → Normalizer/Nowcaster: 추론용 파생 Silver
  → Serving plan: 입력·모델의 URI와 SHA 고정
  → LightGBM inference: 대여·반납 12시간 예측
  → Gold PostgreSQL/PostGIS: 동일 기준시각의 projection 원자 게시
  → 긴급도·재배치 경로
  → FastAPI → React 대시보드
```

Airflow는 계산을 직접 구현하지 않는다. `BashOperator`로 각 애플리케이션의 CLI를 실행하고 순서, 재시도, timeout과 성공·실패 상태를 관리한다. 월별 재학습 DAG만 `PythonOperator`로 AWS EC2 제어와 원격 명령 실행을 오케스트레이션한다.

## DAG와 실제 주기

모든 시간은 KST(`Asia/Seoul`) 기준이며 `catchup=False`, `max_active_runs=1`이다.

| DAG | cron | 역할 |
| --- | --- | --- |
| `realtime_tick` | 매시 5·15·25·35·45·55분 | 실시간 수집과 serving 체인 실행 |
| `realtime_tick_ultra_weather` | 매시 10·20·30·40·50분 | 위 체인 + 초단기실황·초단기예보 수집 |
| `realtime_tick_ultra_weather_on_hour` | 3의 배수가 아닌 시의 정각 | 위 체인 + 초단기실황·초단기예보 수집 |
| `realtime_tick_full_weather` | 0·3·6·9·12·15·18·21시 정각 | 위 체인 + 초단기실황·초단기예보·단기예보 수집 |
| `daily_population_and_events` | 매일 03:00 | 생활인구 수집·nowcast와 두 행사 source 게시 |
| `station_master` | 매일 03:04 | 대여소 마스터 수집 후 공간·격자 정보 보강 |
| `daily_compaction` | 매일 04:30 | D-6 대여이력 재수집 후 일별 Archive 생성 |
| `monthly_retrain_rental` | 매월 1일 03:00 | 대여 모델 평가 및 조건부 재학습 |
| `monthly_retrain_return` | 매월 1일 06:00 | 반납 모델 평가 및 조건부 재학습 |

네 realtime DAG의 실행 시각은 겹치지 않으며 합치면 정확히 5분 간격이다. 별도의 `realtime_5min`, `weather_10min`, `weather_3h` DAG나 날씨 대기 sensor는 현재 없다.

`station_master`의 03:04는 03:00 realtime 실행 중 source authority가 바뀌어 Gold finalize가 중단되는 것을 피하기 위한 운영값이다. realtime 처리시간이 5분에 가까워지면 이 간격을 다시 검토해야 한다.

## 왜 `realtime_5min`과 날씨 DAG를 통합했는가

### 변경 전 구조

```text
weather_10min ─┐
weather_3h ────┴→ S3 weather manifest
                         ↑ 2초 간격, 최대 30초 폴링
realtime_5min → wait_for_weather_manifests → prepare → inference → Gold
```

기존에는 `realtime_5min`과 날씨 수집 DAG가 서로 독립적으로 스케줄되었다. 따라서
realtime DAG는 같은 시각의 날씨 수집이 끝났는지 Airflow 의존성으로 알 수 없었고,
`wait_for_weather_manifests` sensor가 S3 manifest를 반복 조회해야 했다.

이 구조에는 다음 문제가 있었다.

- 날씨 완료 관계가 DAG 그래프에 나타나지 않고 S3 상태와 polling에 의존했다.
- LocalExecutor 병렬 슬롯이 3개인 환경에서 sensor가 대기하는 동안 슬롯 하나를
  점유했다.
- 날씨가 필요한 시각은 고정돼 있는데도 모든 5분 tick에서 런타임 확인이 필요했다.
- 분리 DAG를 유지하면서 realtime DAG에도 날씨 수집을 추가하면 같은 source를 같은
  시각에 중복 수집할 수 있었다.

### 현재 구조

날씨가 필요한 시각에만 weather collector를 realtime 실행 안에 넣고,
`weather_ready_gate → prepare_serving_plan`을 명시적인 task dependency로 연결했다.

```text
필요한 weather collectors ─→ weather_ready_gate ─┐
bike_station_realtime ────────────────────────────┴→ prepare → inference → Gold
```

이제 prepare는 별도 DAG의 완료를 추측하거나 manifest를 polling하지 않는다. 같은 DAG
run의 날씨 태스크가 끝난 직후 실행되므로 의존성과 실행 상태를 Airflow graph에서 바로
확인할 수 있다. 기존 `weather_10min`과 `weather_3h` DAG는 제거하여 중복 수집도 막았다.

단, 날씨 장애가 전체 serving을 막지는 않는다. `weather_ready_gate`는
`TriggerRule.ALL_DONE`이므로 새 날씨 수집이 실패해도 성공하고, prepare는 이전의
유효한 weather snapshot으로 계속 진행할 수 있다. 이는 기존 sensor의 soft-fail
정책을 유지한 것이다. 각 날씨 collector는 `retries=0`, timeout 60초로 제한하여
날씨 장애가 realtime 체인을 장시간 붙잡지 않게 했다.

### 왜 하나가 아니라 4개 realtime DAG인가

운영 관점에서는 하나의 공통 realtime 파이프라인이지만, Airflow에는 다음 4개 DAG로
등록된다. `_build_realtime_tick_dag()`가 동일한 core task를 만들고 cron과 포함할
날씨 source만 다르게 받는다.

| tick 구간 | 포함할 날씨 | DAG |
| --- | --- | --- |
| 10분 경계가 아닌 시각 | 없음 | `realtime_tick` |
| 매시 10·20·30·40·50분 | 초단기실황·초단기예보 | `realtime_tick_ultra_weather` |
| 3시간 경계가 아닌 정각 | 초단기실황·초단기예보 | `realtime_tick_ultra_weather_on_hour` |
| 3시간 경계 정각 | 위 두 source + 단기예보 | `realtime_tick_full_weather` |

분할 기준은 데이터 상태가 아니라 `minute % 10`, `hour % 3`으로 미리 결정되는 발행
주기다. 이를 cron으로 표현하면 sensor 없이 필요한 task만 생성할 수 있다. 정각의
“3시간 경계 여부”를 하나의 cron으로 함께 표현하기 어려워 초단기 weather DAG가 두
종류로 나뉘었다.

### 코드와 검증 근거

- `airflow/dags/realtime_tick.py`: 공통 DAG builder, weather task와 `ALL_DONE` gate의
  직접 의존성
- `airflow/config/schedules.py`: 4개 cron과 분할 사유
- `airflow/tests/test_realtime_tick.py`: 4개 cron이 서로 겹치지 않고, 합집합이 기존
  `*/5 * * * *`의 모든 tick과 정확히 같은지 검증
- `airflow/tests/test_dag_imports.py`: 날씨 source가 해당 realtime DAG에만 포함되고
  prepare의 upstream으로 연결되는지 검증

변경 당시 로컬 Airflow에서도 DAG import 오류가 없고, 초단기 날씨 실행에서 collector
종료 후 gate가 약 0.04초 만에 통과하는 것을 확인했다. 따라서 이 변경의 핵심은 DAG
수를 단순히 줄이는 것이 아니라, **외부 상태 polling을 같은 DAG 내부의 명시적
의존성으로 바꾸고 제한된 worker slot을 돌려주는 것**이다.

## Realtime serving 체인

```text
bike_station_realtime ────────────────────────┐
필요 시 weather collectors → ALL_DONE gate ──┤
                                              ├→ prepare_serving_plan
bike_rental_history ──────────────────────────┼→ run_inference
population_realtime → run_normalizer ─────────┘       ↓
                                          finalize_serving_release
                                                      ↓
                                          publish_station_urgency
                                                      ↓
                                          publish_rebalance_route

bike_rental_history → 1시간 전 replay  (serving과 독립된 side chain)
```

| 단계 | 실제 책임 | 실패 시 경계 |
| --- | --- | --- |
| Collector | API 응답을 Bronze/Silver로 저장하고 검증된 source manifest 게시 | 필수 실시간 source가 실패하면 downstream 중단 |
| 날씨 gate | 날씨 태스크의 성공 여부와 무관하게 종료 | 새 날씨가 없으면 prepare가 이전의 유효한 snapshot 사용 가능 |
| Normalizer | 일별 격자 인구를 실시간 POI 인구로 보정 | 추론 중단 |
| Serving plan | station, stock, weather, model 입력을 immutable URI+SHA로 고정 | Gold는 변경되지 않음 |
| Inference | 공통 지원 대여소별 1~12시간 대여·반납량 생성 | 이전 정상 Gold release 유지 |
| Finalize | station, stock, demand, weather projection을 한 트랜잭션으로 게시 | 입력 drift 또는 불완전 결과면 전체 게시 중단 |
| Urgency·route | 새 release로 긴급도와 센터별 제안 경로 계산 | serving release는 유지되고 파생 정보만 갱신되지 않음 |

날씨 collector는 재시도 없이 60초 안에 끝내도록 제한한다. 실패해도 `ALL_DONE` gate를 통과시켜 이전의 유효한 날씨로 serving을 계속할 수 있게 한다. 반면 재고, 대여이력과 정규화 인구는 inference의 필수 upstream이다.

## 일·월 단위 체인

### 일별 생활인구와 행사

세 branch는 서로 독립이다.

- `living_population_grid → run_nowcasting`
- `cultural_event → event:cultural_event Gold 게시`
- `performance_event → event:performance_event Gold 게시`

생활인구 Collector가 `PARTIAL`이면 authority가 없으므로 actual Archive 승격은 건너뛰되,
기존 Archive 기반 nowcast 추정 로직은 계속 실행한다. Exact authority가 비어 있는 것과
달리 revision·manifest·Silver checksum이 손상되면 Nowcaster는 fail-closed한다. 행사
`PARTIAL`은 새 Gold를 게시하지 않는다. 기존 `publication_state`와 content-addressed
manifest가 일치하면 Gold 행과 state를 변경하지 않고 CLI는 `stale`로 성공하지만, 최초
실행처럼 유지할 state가 없거나 기존 manifest가 손상됐으면 해당 행사 branch가 실패한다.
세 branch는 독립이므로 한 행사 실패가 생활인구나 다른 행사 branch를 막지 않는다.

### 대여소 마스터

`bike_station_master`를 수집한 뒤 `station_master_enriched`를 만든다. realtime plan과 inference가 이 보강 결과를 필수 입력으로 사용한다.

### 일별 compaction

D-6의 대여이력 24개 시간대를 순차적으로 강제 재조회한다. 각 시간대는 `ALL_DONE`으로 다음 시간대를 계속 시도하며, 마지막에는 성공 여부와 무관하게 대여이력 compaction과 Cold recovery를 각각 실행한다. 두 작업은 서로 의존하지 않는다. 대여소 실시간·실시간 생활인구·초단기실황 compaction도 Cold와 독립적이다. Cold worker는 전체 날짜 범위를 훑지 않고 모든 Collector source의 `_cold_pending` marker 중 6일이 지난 날짜만 처리한다. 검증 완료 Hot에만 `cold_compacted=true`를 붙여 30일 Lifecycle을 허용한다. D-36 날짜에서는 모든 source의 30일 지난 non-authority Silver를 정리한다. Archive 대상 4개는 Cold와 Archive를 모두 검증하고, 나머지는 Cold를 검증하며 최신 authority는 항상 유지한다.

### 월별 모델 점검

대여와 반납은 같은 DAG builder를 쓰되 실행 시간을 분리한다.

```text
EC2 시작 → champion 평가 → 평가 EC2 중지 → 재학습 여부 분기
                                      ├→ 조건부 재학습 loop
                                      └→ skip
                                             ↓
                                  모든 관련 EC2 중지 보장
```

재학습은 단일 학습 EC2에서 수행한다. Airflow scheduler 프로세스에서 모델을 직접 학습하지 않는다.

## 데이터 계층과 소비자

| 계층 | 저장소 | 내용 | 주요 소비자 |
| --- | --- | --- | --- |
| Bronze | S3/MinIO JSON | 외부 API 원문에 가까운 page/chunk | 재처리·감사 |
| Silver | S3/MinIO Parquet | 타입·결측·범위 검증을 통과한 source window | normalizer, inference, loader |
| Source manifest | S3/MinIO JSON | 선택된 Silver의 URI, SHA, logical time, revision | 모든 downstream |
| Derived/Archive | S3/MinIO Parquet | 보정 인구, nowcast, 일별 중복 제거 결과 | inference·학습 |
| Serving plan/output | S3/MinIO JSON·Parquet | 고정 입력 계약과 12시간 예측 | Gold finalize |
| Gold | PostgreSQL/PostGIS | 현재 station, stock, forecast, weather, event, urgency, route | FastAPI |

API와 화면은 S3를 직접 읽지 않는다. Gold의 freshness와 공통 기준시각 검사를 통과한 데이터만 제공하며, stale하거나 서로 다른 release의 projection을 섞지 않는다.

## 새 환경의 선행 조건

1. PostgreSQL/PostGIS schema 적용
2. `dispatch_center`, `weather_grid` 기준정보 bootstrap
3. 검증된 rental/return model serving release 등록
4. station master와 생활인구 등 선행 source 생성
5. realtime DAG 활성화

기준정보와 model release가 없으면 DAG 자체는 로드되더라도 serving plan 또는 downstream publication이 성공할 수 없다.

## 운영 확인 위치

- 스케줄·timeout: `airflow/config/schedules.py`
- source 그룹: `airflow/config/sources.py`
- 태스크 명령: `airflow/orchestration/`
- 자원 manifest: `airflow/resource-profiles/<dag_id>/<run_id>/<task_id>/...json`
- DAG 구조 회귀 테스트: `airflow/tests/`
