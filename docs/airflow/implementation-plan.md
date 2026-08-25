# Airflow 구현 계획

> **보관 문서:** 구현 당시의 상세 계획을 보존한 자료다. 아래 본문에 남은
> Backfill·Bronze Compaction 설계는 현재 운영 계약이 아니다. 현행 DAG 이름, 주기와
> 인프라 구성은 실제 `airflow/dags/`, `airflow/orchestration/` 및 Collector 코드를
> 기준으로 판단한다.
>
> 현행 책임과 구현 상태는 다음과 같다.
>
> - 범용 Backfill DAG는 없다. 일일 source의 누락이 합의된 허용치를 넘어서
>   `fetch_error`가 되면 당일 task retry에서 Collector가 기존 부분 Bronze를 비우고
>   전체를 다시 수집한다. 허용치 이내 누락은 기존처럼 `PARTIAL`로 진행하고,
>   저장·품질 실패는 기존 Bronze를 재사용한다.
> - 대여이력은 일반 백필이 아니라 `+1시간`과 `D-6`의 `--force` correction으로
>   늦은 반납 기록을 보강한다.
> - `daily_compaction`은 대여이력 D-6 correction 뒤 Silver를 날짜별 Archive로 묶는다.
>   대여소 실시간·기상 실황 Archive는 이 replay와 독립적으로 처리한다. Bronze를
>   압축하는 작업이 아니다.
> - 영구 원본용 Cold Bronze archive/compaction은 아직 구현하지 않았다.
> - `--backfill`, retry marker와 manifest의 backfill 필드는 기존 코드·manifest 파싱
>   호환을 위해 유지한다. 운영 설정은 marker를 만들거나 발견하지 않으며, 기존
>   `_retry_queue` 객체는 비활성 상태로 별도 정리할 대상이다.

## 1. 목적

이 문서는 프로젝트의 Airflow 구현 방향을 정의한다.

Airflow는 데이터를 직접 수집하거나 저장하지 않는다.

Airflow의 역할은 다음과 같다.

- 정해진 시각에 Collector 실행
- 여러 Collector Task의 병렬 실행
- Task 간 dependency 관리
- Task 단위 retry
- timeout 관리
- Backfill 실행
- 일 단위 Batch 실행
- 실행 성공/실패 상태 관리
- 추론 EC2 실행 및 상태 관리
- 추론 완료 후 Gold RDS 적재 작업 오케스트레이션
- EMR 기반 Feature/Training Job 실행 및 상태 관리
- 수집 → 추론 → 서빙, 학습 파이프라인의 전체 dependency 관리

실제 API 호출, 페이지네이션, Bronze/Silver 저장, 부분 실패 판정은 Collector가 담당한다. 추론 계산은 Inference 애플리케이션이, Gold 적재는 별도 적재 로직이, Feature 생성과 모델 학습은 EMR에서 실행되는 작업이 담당한다. Airflow는 이 작업들을 직접 수행하지 않고 실행 순서와 상태를 오케스트레이션한다.

---

## 2. 전체 구조

```text
Airflow
│
├── realtime_collection DAG
│   │
│   ├── collect_bike
│   ├── collect_population
│   ├── collect_weather
│   └── ...
│
├── backfill DAG
│   │
│   ├── 기술적 실패 조각 복구
│   └── 지연 도착 데이터 재수집
│
└── bronze_compaction DAG
    │
    └── 하루 단위 Bronze Compaction
```

Airflow는 Collector를 독립 프로그램으로 취급한다.

```text
Airflow
   ↓
Collector CLI
   ↓
API / S3 / Validation / Silver
```

Airflow는 Collector 내부 구현을 알지 않는다.

---

## 3. Airflow와 Collector의 책임 분리

### Airflow 책임

- 실행 시각 결정
- 실행 주기 관리
- Task 생성
- source별 병렬 실행
- Task dependency 관리
- Task 단위 retry
- execution timeout
- Collector CLI 호출
- Collector 프로세스 exit code 확인
- Backfill DAG 스케줄링
- Batch DAG 스케줄링

### Collector 책임

- API 인증
- HTTP 호출
- 페이지네이션
- API 호출 retry
- 응답 코드 검증
- 실패 조각 판단
- 부분 실패 허용 비율 판정
- Bronze 저장
- Validation
- Silver 생성
- Backfill 대상 기록
- Backfill 실제 재수집
- 데이터 중복 제거


### 핵심 원칙

Airflow는 Collector가 성공했는지 실패했는지만 판단한다.

```text
Collector SUCCESS
→ exit 0
→ Airflow SUCCESS

Collector PARTIAL
→ exit 0
→ Airflow SUCCESS

Collector FAILED
→ exit != 0
→ Airflow FAILED
→ Airflow retry
```

### Collector 실행 인터페이스 계약

Collector 구현 계획의 CLI를 Airflow의 유일한 호출 계약으로 사용한다.

일반 실시간 수집:

```bash
cd /workspace/collector && \
env -u VIRTUAL_ENV uv run python main.py \
    --source {source_id} \
    --window-start {window_start}
```

Backfill:

```bash
cd /workspace/collector && \
env -u VIRTUAL_ENV uv run python main.py \
    --source {source_id} \
    --window-start {window_start} \
    --backfill
```

호출 규칙:

- `source_id`는 Collector YAML의 실제 `source_id`와 정확히 일치해야 한다. 예를 들어 `bike` 같은 임의 축약 이름을 가정하지 않고 `bike_station_realtime`처럼 Collector가 정의한 식별자를 사용한다.
- `window_start`는 Collector 멱등 키 `(source_id, window_start)`를 구성하는 값이다.
- Airflow retry에서도 동일한 `window_start`를 유지한다.
- Collector는 source의 `schedule.interval`을 기준으로 `window_end`를 내부 계산하므로 일반 CLI에서는 `window_end`를 전달하지 않는다.
- Airflow는 페이지 번호, API URL, fetch round, missing ratio, Bronze/Silver 경로를 전달하지 않는다.
- `--force`는 수동 재처리용이며 일반 스케줄 DAG에서는 사용하지 않는다.
- `--backfill`은 Backfill DAG에서만 사용한다.
- `--force`와 `--backfill`은 동시에 사용하지 않는다.

### Collector 결과 보고 계약

Airflow는 Collector 내부 품질 게이트를 다시 판정하지 않고 **프로세스 종료 코드**를 Task 성공/실패의 1차 계약으로 사용한다.

| Collector status | 프로세스 종료 코드 | Airflow 처리 |
| --- | --- | --- |
| `SUCCEEDED` | `0` | `SUCCESS` |
| `PARTIAL` | `0` | `SUCCESS` |
| `EMPTY` | `0` | `SUCCESS` |
| `SKIPPED` | `0` | `SUCCESS` |
| `FAILED` | non-zero | Task 실패 → Airflow retry |

`PARTIAL`은 일부 quarantine 또는 일부 조각 누락이 존재하더라도 Collector의 `max_missing_ratio`·`max_drop_ratio` 게이트를 통과하고 `stage=completed`까지 간 상태다. Airflow는 이를 다시 실패로 재판정하지 않는다.

Collector 세부 결과의 최종 근거는 S3 `_manifest/{source_id}/.../{HHMM}.json`이다. manifest에는 `status`, `stage`, `failure_reason`, `attempt`, `revision`, `counts`, `missing`, `drop_ratio`, `completeness`, `artifacts`, `backfill_status` 등이 기록될 수 있다.

Airflow는 운영 로그나 후속 판단에 필요할 때 manifest를 참조할 수 있지만, Collector의 품질 게이트나 부분 실패 계산을 Airflow 코드에 중복 구현하지 않는다.

Collector stdout/stderr는 Airflow Task 로그에 그대로 남도록 하며, Airflow가 별도의 페이지별 결과 포맷을 새로 정의하지 않는다.

---

## 4. Collector window_start 규칙

실시간 Collector는 5분 단위 논리 작업으로 실행한다.

Airflow 내부의 DAG Run 식별자인 `run_id`와 Collector의 처리 기준 시각인 `window_start`는 다른 개념으로 구분한다.

실시간 DAG에서는 `CronTriggerTimetable`의 trigger `logical_date`를 KST 기준 `window_start`로 전달한다.

예:

```text
2026-08-13 14:00 KST
→ window_start = 2026-08-13T14:00:00+09:00

2026-08-13 14:05 KST
→ window_start = 2026-08-13T14:05:00+09:00
```

같은 DAG Run에서 실행되는 모든 실시간 Collector Task는 동일한 논리 시각을 기준으로 자신의 Collector `window_start`를 전달한다.

Airflow retry가 발생해도 논리 실행 시각은 동일하므로 `window_start`를 변경하지 않는다.

```text
Airflow Task retry
→ 동일 source_id
→ 동일 window_start
→ Collector manifest / Bronze 재사용 가능
```

Collector 문서에는 `data_interval_start`를 `--window-start`로 넘긴다고 표현되어 있다. 현재 실시간 DAG는 `CronTriggerTimetable`을 사용하므로 이 DAG에서는 동일한 의미의 trigger `logical_date`를 KST 기준 `window_start`로 전달한다.

반면 Bronze Compaction이나 Training처럼 특정 기간을 처리하는 Batch DAG는 실제 `data_interval_start`와 `data_interval_end`를 사용한다.

중요한 계약은 Airflow 내부 객체 이름이 아니라 **Collector에 전달되는 `window_start`가 해당 수집 window의 시작 시각이며 retry 시 변하지 않는 것**이다.

---

## 5. 실시간 수집 DAG

파일:

```text
airflow/dags/realtime_collection.py
```

### 목적

특정 시각마다 외부 API의 최신 상태를 조회하는 Collector들을 실행한다.

이 작업은 특정 시간 범위의 데이터를 처리하는 Batch가 아니라 Polling 작업이다.

따라서 `CronTriggerTimetable`을 사용한다.

### 스케줄

```text
*/5 * * * *
timezone = Asia/Seoul
```

실행 예:

```text
14:00 → Collector 실행
14:05 → Collector 실행
14:10 → Collector 실행
14:15 → Collector 실행
```

### Task 구조

```text
              realtime_collection
                      │
       ┌──────────────┼──────────────┐
       │              │              │
       ▼              ▼              ▼
 collect_bike  collect_population collect_weather
```

각 Collector Task 사이에는 기본 dependency를 두지 않는다.

따라서 Executor와 실행 환경의 자원이 허용하면 병렬로 실행한다.

### DAG Run 중첩

초기 정책:

```text
max_active_runs = 1
```

한 5분 Run이 완료되기 전에 다음 Run이 동시에 실행되어 Collector 자원 사용량이 계속 증가하는 것을 막는다.

향후 실제 처리시간과 EC2 자원을 측정한 후 조정할 수 있다.

---

## 6. Collector Task 공통화

파일:

```text
airflow/orchestration/collector_task.py
```

DAG마다 다음 설정을 중복 작성하지 않는다.

- BashOperator
- Collector 경로
- window_start Jinja Template
- retries
- retry_delay
- execution_timeout

공통 builder를 통해 Task를 생성한다.

개념적으로:

```python
build_collector_task(
    source_id="bike_station_realtime",
)
```

형태로 호출한다.

### 초기 실행 방법

Airflow와 Collector가 동일 EC2에 위치하므로 로컬 CLI 실행 방식을 사용한다.

```text
Airflow
↓
BashOperator
↓
Collector CLI
```

예:

```bash
cd /workspace/collector

env -u VIRTUAL_ENV uv run python main.py \
    --source bike_station_realtime \
    --window-start 2026-08-13T14:00:00+09:00
```

---

## 7. Retry 구조

Retry는 두 계층으로 분리한다.

### 재시도 계층 구분

Collector 내부 재시도와 Airflow Task 재시도는 서로 다른 계층이다.

```text
Airflow Task retry
└── Collector 프로세스 재실행
    └── Collector fetch
        ├── 호출 단위 짧은 backoff
        └── 조각 집합 단위 최대 3라운드(15s → 30s)
```

Airflow는 Collector의 fetch retry round를 대신 구현하지 않는다.

Collector가 최종적으로 `FAILED`를 non-zero exit code로 반환했을 때만 Airflow가 Collector 프로세스 전체를 다시 실행한다. 이때 동일한 `window_start`를 전달하므로 Collector의 manifest 기반 재개 및 Bronze 재사용 로직이 동작할 수 있다.

### Collector 내부 Retry

API 호출 또는 조각 단위의 실패를 처리한다.

```text
Collector
│
├── page A 성공
│
├── page B 실패
│   ├── 내부 retry
│   ├── 내부 retry
│   └── 최종 실패 조각 기록
│
└── page C 계속 진행
```

일부 조각 실패가 허용 비율 이하라면 Collector는 `PARTIAL`로 종료할 수 있다.

이 경우:

```text
exit 0
```

으로 Airflow에는 성공을 반환한다.

### Airflow Retry

Collector 프로세스 전체가 실패했을 때만 실행한다.

초기 정책:

```text
retries = 2
retry_delay = 30초
```

따라서 최초 실행 포함 최대 3번 실행된다.

```text
Collector 실행
↓ 실패

30초 대기
↓
retry 1
↓ 실패

30초 대기
↓
retry 2
↓ 실패

최종 FAILED
```

---

## 8. 부분 실패와 Airflow

Airflow는 API 페이지별 실패를 직접 처리하지 않는다.

예:

```text
전체 페이지 20
성공 18
실패 2
failure_ratio = 10%
```

Collector가 이 비율을 허용한다고 판단하면:

```text
PARTIAL
→ exit 0
```

Airflow에서는 Task SUCCESS로 처리한다.

실패 페이지 자체는 Collector가 별도 Backfill 대상으로 기록한다.

즉:

```text
Airflow SUCCESS
```

가 항상 데이터 100% 완전성을 의미하는 것은 아니다.

Airflow 성공은:

```text
Collector가 정의된 정책에 따라 정상 종료함
```

을 의미한다.

---

## 9. Backfill DAG

파일:

```text
airflow/dags/backfill.py
```

### 목적

Backfill은 두 가지 이유로 필요하다.

### 9.1 기술적 실패 복구

실시간 Collector에서 API 호출 retry를 모두 소진한 조각을 재수집한다.

예:

```text
14:00 Run

page 3 실패
page 17 실패
↓
Backfill 대상 기록
```

이후 Backfill DAG가 해당 작업을 다시 Collector에 요청한다.

Airflow는 실패 페이지를 직접 계산하지 않는다.

### 9.2 지연 도착 데이터 보완

대여이력 API는 대여 시점에 즉시 최종 기록이 생성되지 않는다.

반납이 완료된 후 기록이 생성된다.

대여 후 약 3시간이 지나야 약 95%가 반납되어 조회 가능한 상태가 된다.

따라서 최초 API 호출이 성공하더라도 데이터가 완전히 확정된 것은 아니다.

예:

```text
14:00 대여
↓
14:05 API 조회

아직 반납 전
→ API 호출은 성공
→ 해당 대여이력은 존재하지 않음

17:00 반납
↓
이후 Backfill

→ 새롭게 생성된 대여이력 수집
```

따라서 대여이력은 매일 최근 7일 범위를 재조회한다.

### Backfill 실행

기술적 실패 Backfill은 하나의 Airflow Backfill DAG에서 관리한다.

실행 흐름:

```text
Airflow Backfill DAG
↓
S3 `_retry_queue/` marker LIST
↓
후보별 Collector manifest 확인
↓
`backfill.enabled: true` source만 실행
↓
Collector CLI 호출
```

호출 예:

```bash
cd /workspace/collector && \
env -u VIRTUAL_ENV uv run python main.py \
    --source {source_id} \
    --window-start {window_start} \
    --backfill
```

`_retry_queue/`는 Backfill 후보를 찾기 위한 discovery 용도이며, 실제 상태의 최종 근거는 해당 window의 Collector manifest다.

Airflow는 다음을 직접 계산하지 않는다.

- `missing_parts`
- Backfill expiry
- `revision`
- Silver overwrite/merge 방식
- 누락 비율 또는 품질 게이트

위 판단과 실제 재수집·재저장은 Collector 책임이다.

---

## 10. Bronze Compaction DAG

파일:

```text
airflow/dags/bronze_compaction.py
```

실시간 Collector와 달리 특정 기간에 생성된 데이터를 처리하는 Batch 작업이다.

따라서 Data Interval을 사용한다.

예:

```text
data_interval_start
2026-08-13 00:00

data_interval_end
2026-08-14 00:00
```

이 Run의 의미:

```text
2026-08-13 하루 동안 생성된 Bronze 처리
```

실제 실행은 8월 14일에 이루어진다.

Airflow는 S3 파일을 직접 병합하지 않는다.

```text
Airflow
↓
Compaction 프로그램 실행
↓
S3 Bronze 읽기
↓
병합
↓
일 단위 결과 저장
```

### Data Interval 사용 이유

실행 시각과 처리 대상 날짜를 분리하기 위해 사용한다.

8월 13일 Compaction 작업을 8월 20일에 다시 실행하더라도:

```text
data_interval_start = 8월 13일
data_interval_end   = 8월 14일
```

을 유지하면 동일한 데이터를 재처리할 수 있다.

---

## 11. 3 EC2 구조

초기 개발 및 운영 후보 구조:

```text
EC2 #1
Airflow + Collector

EC2 #2
Inference

EC2 #3
Backend
```

Airflow와 Collector가 같은 EC2에 있으므로:

```text
Airflow
↓
BashOperator
↓
Collector CLI
```

방식으로 실행한다.

### 장점

- 구현 단순
- SSH/SSM 불필요
- 네트워크 실행 계층 없음
- EC2 자원 활용 효율적
- 로컬 개발과 운영 구조가 유사

### 주의

Airflow와 Collector는 같은 EC2를 사용하더라도 논리적으로는 독립 애플리케이션이다.

따라서:

- 서로 다른 uv 프로젝트 유지
- 서로 다른 의존성 유지
- Collector 내부 코드를 Airflow에 import하여 직접 실행하지 않음
- CLI 경계를 유지

한다.

---

## 12. 4 EC2 구조

추후 Collector 전용 EC2를 추가할 수 있다.

```text
EC2 #1 Airflow
EC2 #2 Collector
EC2 #3 Inference
EC2 #4 Backend
```

이 경우:

```text
Airflow
↓
SSH 또는 SSM
↓
Collector EC2
↓
Collector CLI
```

로 바뀐다.

중요한 원칙은 DAG가 Collector의 물리적 위치를 알지 않도록 하는 것이다.

현재:

```text
collector_task.py
→ BashOperator
```

향후:

```text
collector_task.py
→ SSHOperator

또는

collector_task.py
→ SSM Run Command
```

로 변경한다.

DAG 자체는 최대한 변경하지 않는다.

---

## 13. SSH와 SSM

### SSH

장점:

- 구조 단순
- Airflow SSHOperator 사용 가능
- exit status 직접 확인 가능
- 빠른 PoC에 적합

필요 요소:

- SSH Private Key
- 사용자 계정
- known_hosts
- Collector EC2 IP 또는 DNS
- 22번 포트 접근

### SSM

장점:

- SSH Key 불필요
- inbound 22번 포트 불필요
- IAM 기반 권한 관리
- AWS 운영 환경에 적합

필요 요소:

- IAM Role
- SSM Agent
- Managed Node 등록
- SendCommand
- command_id 상태 polling

4 EC2 전환 초기에는 SSH로 검증하고,
운영 안정화 단계에서는 SSM을 우선 검토한다.

---

## 14. 설정 구조

### `config/sources.py`

Airflow가 실행할 Collector source 목록만 관리한다.

예:

```python
REALTIME_SOURCES = (
    "bike_station_realtime",
    "population_realtime",
    "weather_realtime",
)
```

실제 값은 Collector YAML에 정의된 source_id를 그대로 사용하며 Airflow가 별도 별칭을 만들지 않는다.

Collector의 API 세부 설정은 넣지 않는다.

### `config/schedules.py`

Airflow 스케줄 관련 설정을 관리한다.

예:

```python
TIMEZONE = "Asia/Seoul"
REALTIME_CRON = "*/5 * * * *"
```

DAG 파일에 cron 문자열을 반복해서 작성하지 않는다.

---

## 15. Callback

파일:

```text
airflow/callbacks/task_callbacks.py
```

Airflow Task 성공/실패 시 공통으로 필요한 처리를 정의한다.

향후 다음 기능을 이곳에 추가할 수 있다.

- 구조화 로그
- 실패 알림
- 운영 메트릭
- task context 기록

단, API 페이지 실패 정보는 Collector에서 관리한다.

---

## 16. 테스트 전략

Airflow 테스트는 데이터 수집 기능이 아니라 오케스트레이션 구조를 검증한다.

### DAG Import

모든 DAG가 Airflow에서 정상 import되는지 확인한다.

### realtime_collection

확인 항목:

- CronTriggerTimetable 사용
- 5분 주기
- Asia/Seoul
- source별 Task 생성
- source 사이 dependency 없음
- 동일 logical time에서 source별 `window_start` 계약 유지
- retries=2
- retry_delay 설정
- timeout 설정
- max_active_runs=1

### Backfill

확인 항목:

- 일 1회 실행
- realtime DAG와 독립
- Collector backfill CLI 호출
- retry 동작

### Bronze Compaction

확인 항목:

- 일 단위 실행
- Data Interval 사용
- 과거 Run 재실행 시 동일 기간 사용

### 통합 테스트

로컬 Docker 환경에서:

```text
Airflow
↓
BashOperator
↓
Collector CLI
```

실제 subprocess 호출까지 확인한다.

---

## 17. 금지 사항

Airflow 코드에는 다음 로직을 넣지 않는다.

- 외부 API 호출
- API 인증
- 페이지네이션
- API response parsing
- API retry
- Bronze 저장
- Silver 변환
- 데이터 품질 판정
- failed page 계산
- 부분 실패율 계산

이들은 Collector 책임이다.

Airflow 코드가 Collector 구현 세부사항을 알기 시작하면 두 애플리케이션의 책임 경계가 무너진다.

---

## 18. 최종 목표 구조

현재 1차 구현에서는 수집·Backfill·Compaction을 먼저 완성한다.

이후 Airflow의 범위를 추론, 서빙용 Gold 적재, EMR 학습까지 확장한다.

```text
                              Airflow
                                 │
              ┌──────────────────┼──────────────────┐
              │                  │                  │
              ▼                  ▼                  ▼
     realtime pipeline        backfill      bronze_compaction
              │                                     │
              │                                     ▼
              │                              Compaction Job
              │
              ▼
     source별 Collector
              │
              ▼
          S3 Silver
              │
              ▼
       Inference EC2
              │
              ▼
    Prediction Silver
              │
              ▼
         Gold Load
              │
              ▼
            RDS
              │
              ▼
         Backend API


     training pipeline
              │
              ▼
       S3 Silver / Feature
              │
              ▼
             EMR
              │
              ▼
     Feature / Training Job
              │
              ▼
       Model Artifact
```

핵심 원칙은 다음과 같다.

> Airflow는 언제 무엇을 어떤 순서로 실행할지 관리하고, 각 실행 프로그램은 자신의 데이터 처리 책임만 담당한다.

이 경계를 유지하면 Collector, Inference, Gold 적재, EMR 학습이 서로 다른 실행 환경에 있더라도 Airflow는 동일한 방식으로 전체 dependency를 관리할 수 있다.

---

## 19. 2단계 확장 범위

현재 구현 범위는 **Phase 1: 수집 파이프라인 오케스트레이션**이다.

Phase 1이 완료되면 **Phase 2: End-to-End Pipeline Orchestration**으로 확장한다.

```text
Phase 1
├── realtime collection
├── collector retry
├── backfill
├── bronze compaction
└── 3 EC2 → 4 EC2 Collector 실행 전환

Phase 2
├── inference EC2 실행
├── prediction 완료 확인
├── Gold RDS 적재
├── EMR Feature/Training Job 실행
├── 모델 산출물 상태 관리
└── 전체 dependency 및 재처리 정책 통합
```

Phase 1과 Phase 2를 나누는 이유는 수집 파이프라인의 실행 계약을 먼저 안정화한 뒤, 그 결과물인 Silver를 입력으로 사용하는 추론·학습 계층을 연결하기 위해서다.

---

## 20. 실시간 수집 → 추론 → 서빙 파이프라인

실시간 경로의 최종 목표는 수집만 완료하는 것이 아니라, 같은 논리 Run의 Silver를 기반으로 추론하고 Gold를 RDS에 적재하는 것이다.

개념적인 dependency는 다음과 같다.

```text
collect_bike -----------┐
collect_population -----┤
collect_weather --------┼──→ inference ──→ gold_load
collect_event ----------┘
```

### 수집 단계

source별 Collector는 서로 dependency 없이 병렬 실행한다.

같은 DAG Run의 실시간 Collector들은 동일한 논리 시각을 기준으로 `window_start`를 전달한다.

### 추론 단계

추론은 Collector 완료 이후 실행한다.

Airflow는 추론 계산을 직접 수행하지 않고 Inference EC2의 실행 프로그램을 호출한다.

개념적으로:

```text
Airflow
↓
Inference EC2 실행
↓
run_id에 해당하는 Silver 읽기
↓
모델 추론
↓
Prediction Silver 저장
```

실제 원격 실행 방식은 Collector EC2 분리와 동일하게 SSH 또는 SSM 계층으로 추상화한다.

### Gold 적재 단계

추론이 성공한 뒤 서빙 대상 데이터를 Gold RDS에 적재한다.

```text
Prediction Silver
↓
Gold Load Job
↓
RDS Gold
```

Airflow가 RDS 자체를 스케줄링하는 것이 아니라 **RDS에 Gold 데이터를 적재하는 실행 작업을 스케줄링**한다.

Backend는 Gold RDS를 조회하여 서비스한다.

### 성공 조건

초기 원칙은 다음과 같다.

```text
Collection 성공
↓
Inference 실행
↓
Inference 성공
↓
Gold Load 실행
```

Collector의 `PARTIAL_SUCCESS`가 `exit 0`으로 종료되면 Airflow에서는 성공한 upstream Task로 취급한다.

다만 실제 추론 가능 여부는 Collector가 생성한 Silver의 품질 계약을 만족한다는 전제하에 결정한다.

---

## 21. Inference EC2 오케스트레이션

Inference EC2는 모델 추론의 실제 실행 환경이다.

Airflow의 책임:

- 추론 실행 시점 결정
- `run_id` 전달
- 추론 프로세스 실행
- timeout
- Task retry
- 성공/실패 상태 확인
- downstream Gold Load dependency 관리

Inference 애플리케이션의 책임:

- S3 Silver 입력 조회
- run 단위 데이터 병합
- 모델 로드
- 추론
- Prediction Silver 저장
- 추론 결과 검증

Airflow 코드에는 모델 로딩이나 추론 로직을 넣지 않는다.

현재 Airflow+Collector EC2와 Inference EC2가 분리되어 있으므로 원격 실행 계층이 필요하다.

```text
Airflow EC2
↓
SSH 또는 SSM
↓
Inference EC2
↓
Inference CLI
```

Collector 원격 실행과 동일하게 DAG는 물리적 실행 방법을 직접 알지 않도록 한다.

---

## 22. Gold RDS 적재 오케스트레이션

RDS는 서비스용 Gold 데이터 저장소다.

Airflow는 RDS 인스턴스 자체를 주기적으로 실행하거나 제어하는 것이 아니라 Gold 적재 작업의 실행 순서를 관리한다.

```text
inference
↓
Prediction Silver 생성
↓
gold_load
↓
RDS Gold
```

Gold Load Job의 책임:

- 서빙 대상 Prediction Silver 읽기
- Gold 스키마 변환
- 중복/멱등 처리
- RDS transaction
- 적재 결과 검증

Airflow의 책임:

- Inference 성공 이후 실행
- retry
- timeout
- 성공/실패 확인
- 실패 시 downstream 진행 차단

Backend가 RDS를 조회하는 주기와 Airflow DAG 실행 주기는 서로 다른 책임이다.

---

## 23. EMR Feature/Training 오케스트레이션

모델 학습은 5분 실시간 파이프라인과 분리된 별도 DAG로 관리한다.

예:

```text
training DAG
    │
    ▼
S3 Silver / 학습 데이터
    │
    ▼
EMR Job 제출
    │
    ▼
Feature 생성
    │
    ▼
Model Training
    │
    ▼
Model Artifact 저장
```

Airflow의 책임:

- 학습 실행 주기 결정
- EMR Job 제출
- Job 상태 polling
- timeout / retry
- 성공/실패 판정
- 후속 모델 배포 또는 검증 dependency 연결

EMR Job의 책임:

- S3 데이터 읽기
- Spark 기반 대규모 전처리
- Feature 생성
- 모델 학습
- 모델 평가
- 모델 Artifact 저장

### 학습 스케줄

학습은 실시간 5분 Polling과 동일한 주기로 실행하지 않는다.

일/주 단위 또는 충분한 학습 데이터가 누적되는 시점에 실행하도록 별도의 스케줄을 사용한다.

정확한 학습 주기는 모델 운영 정책이 확정된 뒤 결정한다.

### Data Interval

학습 입력이 특정 기간의 데이터를 기준으로 생성되는 경우 Data Interval을 사용한다.

이를 통해 과거 학습 Run을 다시 실행하더라도 동일한 학습 기간을 재현할 수 있어야 한다.

---

## 24. Phase 2 코드 구조 확장

Phase 2에서는 Airflow 폴더를 다음과 같이 확장하는 것을 목표로 한다.

```text
airflow/
├── dags/
│   ├── realtime_collection.py
│   ├── backfill.py
│   ├── bronze_compaction.py
│   ├── realtime_serving.py
│   └── training.py
│
├── orchestration/
│   ├── collector_task.py
│   ├── inference_task.py
│   ├── gold_load_task.py
│   └── emr_task.py
│
├── config/
│   ├── sources.py
│   └── schedules.py
│
├── callbacks/
│   └── task_callbacks.py
│
└── tests/
    ├── test_realtime_collection.py
    ├── test_realtime_serving.py
    ├── test_training.py
    ├── test_backfill.py
    └── test_bronze_compaction.py
```

실제 파일 분리는 구현 시점의 DAG 크기와 재사용 정도를 보고 조정할 수 있다.

중요한 원칙은 DAG가 실행 프로그램의 내부 비즈니스 로직을 갖지 않는 것이다.

---

## 25. Phase 2 테스트 전략

### 실시간 E2E

검증 대상:

```text
Collection
↓
Inference
↓
Gold Load
↓
RDS
```

확인 항목:

- Collection 완료 전 Inference가 실행되지 않는가
- 같은 `run_id`가 Inference까지 전달되는가
- Inference 실패 시 Gold Load가 실행되지 않는가
- Inference retry가 동작하는가
- Gold Load retry가 동작하는가
- 동일 Run 재실행 시 중복 Gold 적재가 발생하지 않는가

### Training

확인 항목:

- 정해진 학습 스케줄에 Run이 생성되는가
- EMR Job이 정상 제출되는가
- Airflow가 EMR Job 완료까지 상태를 추적하는가
- 실패한 EMR Job이 Airflow FAILED로 반영되는가
- 과거 Data Interval 재실행 시 동일 학습 구간을 사용하는가

### 책임 경계 테스트

Airflow 테스트에서는 모델 정확도, Spark Transformation 결과, RDS Gold 데이터 값 자체를 검증하지 않는다.

해당 검증은 각각 Inference, Training, Gold Load 애플리케이션의 테스트 책임이다.

---

## 26. 최종 Airflow 책임 범위

모든 단계가 구현된 후 Airflow는 프로젝트 전체의 중앙 오케스트레이터 역할을 한다.

```text
Airflow
│
├── Collector 실행
│   ├── 실시간 수집
│   ├── retry
│   └── backfill
│
├── 데이터 유지관리
│   └── Bronze Compaction
│
├── Inference EC2 실행
│   └── Prediction Silver 생성
│
├── Serving Pipeline
│   └── Gold RDS 적재
│
└── EMR
    ├── Feature 생성
    └── Model Training
```

Airflow의 최종 책임은 **데이터 처리 자체가 아니라, 서로 다른 실행 환경과 작업을 하나의 재현 가능한 dependency graph로 연결하는 것**이다.
