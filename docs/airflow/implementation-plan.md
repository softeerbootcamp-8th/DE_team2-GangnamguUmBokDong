# Airflow 구현 계획

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

실제 API 호출, 페이지네이션, Bronze/Silver 저장, 부분 실패 판정은 Collector가 담당한다.

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

Collector PARTIAL_SUCCESS
→ exit 0
→ Airflow SUCCESS

Collector FAILED
→ exit != 0
→ Airflow FAILED
→ Airflow retry
```

---

## 4. run_id 규칙

실시간 Collector는 5분 단위 논리 작업으로 실행한다.

예:

```text
2026-08-13 14:00
→ run_id = 20260813T140000

2026-08-13 14:05
→ run_id = 20260813T140500
```

같은 DAG Run에서 실행되는 모든 Collector Task는 동일한 run_id를 사용한다.

```text
14:05 DAG Run

collect_bike
→ 20260813T140500

collect_population
→ 20260813T140500

collect_weather
→ 20260813T140500
```

Airflow retry가 발생해도 logical run은 동일하므로 run_id를 변경하지 않는다.

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
- run_id Jinja Template
- retries
- retry_delay
- execution_timeout

공통 builder를 통해 Task를 생성한다.

개념적으로:

```python
build_collector_task(
    source="bike",
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
    --source bike \
    --run-id 20260813T140000
```

---

## 7. Retry 구조

Retry는 두 계층으로 분리한다.

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

일부 조각 실패가 허용 비율 이하라면 Collector는 `PARTIAL_SUCCESS`로 종료할 수 있다.

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
PARTIAL_SUCCESS
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

하루 1회 새벽 시간에 실행한다.

정확한 시각은 구현 단계에서 확정한다.

실시간 DAG와 Backfill DAG는 서로 dependency가 없다.

```text
02:00

realtime_collection
        +
backfill

동시 실행 가능
```

Backfill 실행 중에도 실시간 수집은 계속 이루어진다.

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
    "bike",
    "population",
    "weather",
)
```

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
- 같은 run_id 전달
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

```text
                          Airflow
                             │
          ┌──────────────────┼──────────────────┐
          │                  │                  │
          ▼                  ▼                  ▼
 realtime_collection      backfill      bronze_compaction
          │                  │                  │
          ▼                  ▼                  ▼
   Collector CLI      Collector CLI      Compaction Job
          │
    ┌─────┼───────────────┐
    ▼     ▼               ▼
  bike population       weather
```

핵심 원칙은 다음과 같다.

> Airflow는 언제 무엇을 실행할지 관리하고,
> Collector는 데이터를 어떻게 수집하고 처리할지 책임진다.

이 경계를 유지하면 Airflow와 Collector가 같은 EC2에 있든 별도 EC2에 있든,
DAG 구조를 크게 변경하지 않고 실행 방식만 교체할 수 있다.