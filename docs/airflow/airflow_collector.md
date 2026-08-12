# Airflow ↔ Collector 연동 설계

## 1. 목적

이 문서는 우리 시스템에서 Airflow가 `collector`를 어떻게 실행하고, 실행 상태를 어떻게 판단하며, 실패와 재시도를 어디에서 책임지는지 정리한다.

핵심 원칙은 다음과 같다.

> **Airflow는 작업의 시점과 순서를 관리하고, Collector는 실제 데이터 수집·Bronze 저장·정제·Silver 저장을 수행한다.**

Airflow DAG 안에 API 수집 로직을 직접 작성하지 않는다. 데이터 소스별 수집·검증·저장 규칙은 `collector/`에 두고, Airflow는 Collector의 실행 인터페이스만 호출한다.

---

## 2. 전체 AWS 시스템에서의 위치

이 프로젝트의 기준 실행 환경은 로컬이 아니라 **AWS**다. 자원이 계획대로 할당되는 경우 각 역할을 다음과 같이 분리한다.

```text
External API
    │
    ▼
┌──────────────────┐
│   Collector EC2  │
│                  │
│ API 호출          │
│ Bronze 저장       │
│ Transform / QC   │
│ Silver 저장       │
└───────┬──────────┘
        │
        ├──────────────▶ S3 Bronze
        │
        └──────────────▶ S3 Silver
                              │
              ┌───────────────┴────────────────┐
              │                                │
              ▼                                ▼
     ┌──────────────────┐            ┌──────────────────┐
     │  Inference EC2   │            │       EMR        │
     │                  │            │                  │
     │ 최신 Silver 조회   │            │ Feature Mart     │
     │ Feature 구성      │            │ Model Training   │
     │ 모델 추론          │            │ Batch Processing │
     └────────┬─────────┘            └────────┬─────────┘
              │                                │
              │                                ▼
              │                           S3 Model /
              │                           Feature Mart
              │
              ▼
        ┌─────────────┐
        │  Gold RDS   │
        └──────┬──────┘
               │
               ▼
        ┌─────────────┐
        │ Backend EC2 │
        │   FastAPI   │
        └──────┬──────┘
               │
               ▼
            Client


                 ┌────────────────────┐
                 │     Airflow EC2    │
                 │                    │
                 │ Schedule           │
                 │ Dependency         │
                 │ Retry              │
                 │ Monitoring         │
                 └─────────┬──────────┘
                           │
          ┌────────────────┼───────────────────┐
          │                │                   │
          ▼                ▼                   ▼
   Collector EC2     Inference EC2           EMR
     수집 실행          추론 실행        피처마트/학습 실행
```

즉 Airflow EC2는 데이터를 직접 처리하지 않고, AWS에 분리되어 있는 실행 컴포넌트를 오케스트레이션한다.

```text
Airflow EC2
= Orchestrator

Collector EC2
= Collection + Bronze + Silver

Inference EC2
= Online/Periodic Inference

EMR
= Feature Mart + Model Training + Heavy Batch

S3 Bronze / Silver
= Data Lake

RDS Gold
= Serving Store

Backend EC2
= FastAPI Serving
```

이 문서에서는 그중 **Airflow EC2가 Collector EC2를 어떻게 실행하고 상태를 전달받는지**를 중심으로 설명한다.

---

## 3. Airflow와 Collector의 책임 분리

### Airflow가 책임지는 것

Airflow는 **언제, 어떤 순서로, 몇 번 실행할 것인지**를 책임진다.

- 5분 단위 수집 스케줄
- 데이터 소스별 DAG 또는 Task 구성
- Task dependency 관리
- Collector 프로세스 실행
- Collector의 종료 코드 확인
- Task-level retry
- timeout 관리
- 실패 로그 확인
- downstream 추론 Task 실행 여부 결정
- backfill / 재처리 실행

예를 들면 다음 흐름이다.

```text
collect_bike
    ├─ success → collect/weather 등 다음 흐름 진행
    └─ failure → Airflow retry / DAG failure
```

### Collector가 책임지는 것

Collector는 **한 번 실행되었을 때 데이터 한 작업을 정확하게 완료하는 것**을 책임진다.

- 외부 API 호출
- API 응답 코드 검증
- pagination 처리
- 페이지별 원본 응답 S3 Bronze 저장
- transform
- 타입·결측·이상치 validation
- S3 Silver Parquet 저장
- API page 단위 재시도
- exponential backoff + jitter
- `run_id`, `page_no`, `attempt_no` 관리
- 수집 결과 요약 로그 출력
- 최종 성공/실패에 따른 process exit code 반환

중요한 점은 **API 재시도와 Airflow Task 재시도가 서로 다른 레벨의 재시도**라는 것이다.

```text
Airflow Task Retry
└── Collector 전체 실행 재시도
    └── Collector 내부 API Page Retry
        └── 동일 1000건 Page 최대 3회 API 호출
```

Collector 내부에서 해결 가능한 API 일시 오류는 Collector가 먼저 처리한다. Collector가 최종적으로 작업을 완료하지 못했을 때만 non-zero exit code를 반환하여 Airflow Task를 실패시킨다.

---

## 4. AWS에서 Airflow EC2와 Collector EC2 연결 방식

운영 기준에서는 Airflow와 Collector가 서로 다른 EC2에 존재한다.

```text
┌──────────────────────┐
│      Airflow EC2     │
│                      │
│ Scheduler            │
│ DAG                  │
│ Task State           │
└──────────┬───────────┘
           │
           │ remote execution
           ▼
┌──────────────────────┐
│     Collector EC2    │
│                      │
│ repository           │
│ collector/.venv      │
│ collector/main.py    │
└──────────────────────┘
```

이때 권장 연결 방식은 **AWS Systems Manager(SSM) Run Command**다.

```text
Airflow EC2
    │
    │ ssm:SendCommand
    ▼
AWS Systems Manager
    │
    ▼
Collector EC2
    │
    │ shell command
    ▼
cd <repository>/collector
uv run python main.py --source ... --run-id ...
```

SSHOperator도 사용할 수 있지만 본 프로젝트의 기본 설계로 두지 않는다.

SSM을 우선하는 이유는 다음과 같다.

- Airflow EC2에 SSH private key를 보관할 필요가 없다.
- Collector EC2의 22번 포트를 Airflow에 열 필요가 없다.
- IAM Role 기반으로 실행 권한을 제한할 수 있다.
- 명령 실행 ID와 실행 상태를 AWS API로 조회할 수 있다.
- 실행 결과를 Airflow Task 성공/실패와 연결하기 쉽다.

따라서 운영 실행 계약은 다음과 같다.

```text
Airflow DAG
   │
   │ run_id + source
   ▼
SSM SendCommand
   │
   ▼
Collector CLI
   │
   ├─ exit 0     → SSM Success → Airflow success
   └─ exit != 0  → SSM Failed  → Airflow failure/retry
```

Airflow가 Collector의 내부 Python 함수를 직접 import하지 않는다. Collector는 별도의 EC2에서 독립된 애플리케이션으로 실행한다.

---

## 5. 왜 직접 import보다 CLI 실행을 권장하는가

현재 프로젝트에서 `airflow/`와 `collector/`는 서로 독립된 uv 프로젝트이고, 운영 환경에서는 서로 다른 EC2에 배포된다.

따라서 Airflow가 Collector의 내부 Python 모듈을 직접 import하면 다음 문제가 생긴다.

```text
Airflow Environment
    │
    ├─ collector dependency까지 필요
    ├─ package path 조정 필요
    ├─ dependency 충돌 가능
    └─ 두 프로젝트의 배포 경계가 흐려짐
```

반대로 CLI 실행 방식은 다음처럼 분리된다.

```text
Airflow Environment
    │
    │ command
    ▼
Collector Environment
    │
    ├─ collector/pyproject.toml
    ├─ collector/uv.lock
    └─ collector/.venv
```

따라서 Collector dependency가 바뀌어도 Airflow dependency에는 영향을 주지 않는다.

---

## 6. Collector 실행 인터페이스

Airflow와 안정적으로 연결하려면 Collector가 명확한 CLI 인터페이스를 가져야 한다.

권장 형태는 다음과 같다.

```bash
uv run python main.py \
  --source bike \
  --run-id 20260812T194000
```

필요하면 다음 옵션까지 확장할 수 있다.

```text
--source
    bike
    weather
    hotspot_population
    grid_population
    event
    rental_history

--run-id
    Airflow가 생성한 논리 실행 ID

--base-dttm
    해당 수집 작업의 기준 시각

--start-page
    부분 재처리가 필요한 경우 시작 page

--end-page
    부분 재처리가 필요한 경우 종료 page
```

단, 평상시 운영에서는 Airflow가 페이지 단위를 직접 관리하지 않는다. 페이지 단위 처리는 Collector의 책임으로 둔다.

```text
Airflow
└── source=hotspot_population 실행
    └── Collector
        ├── page 1
        ├── page 2
        ├── ...
        └── page N
```

---

## 7. run_id 전달 방식

`run_id`는 Airflow와 Collector를 연결하는 가장 중요한 식별자다.

우리 시스템에서 `run_id`는 **5분 논리 작업의 기준 시각**을 의미한다.

예:

```text
20260812T194000
```

Airflow Task retry, Collector API retry, 재처리가 발생하더라도 동일한 논리 작업이라면 같은 `run_id`를 사용한다.

```text
Airflow DAG Run
run_id = 20260812T194000

    └── Collector
        ├── page 1 attempt 1
        ├── page 2 attempt 1
        ├── page 3 attempt 1 → fail
        └── page 3 attempt 2 → success
```

S3에는 다음처럼 저장할 수 있다.

```text
bronze/{source}/{YYYY}/{MM}/{DD}/{run_id}/{page_no}_{attempt_no}.json
silver/{source}/{YYYY}/{MM}/{DD}/{run_id}/{page_no}.parquet
```

이렇게 하면 Airflow 로그와 S3 객체를 `run_id` 하나로 추적할 수 있다.

---

## 8. 성공/실패 전달 방식

Airflow와 Collector 사이에서 가장 단순하고 확실한 통신 규칙은 **process exit code**다.

### 성공

Collector가 모든 예상 Page에 대해 Bronze와 Silver 저장까지 완료하면:

```text
exit code = 0
```

Airflow는 Task를 `success`로 처리하고 다음 Task를 실행한다.

### 실패

최대 재시도 이후에도 API 또는 저장 작업이 완료되지 않으면:

```text
exit code != 0
```

Airflow는 해당 Task를 `failed`로 판단한다.

예:

```python
import sys


def main() -> None:
    try:
        run_collection()
    except Exception:
        logger.exception("collection failed")
        sys.exit(1)

    sys.exit(0)
```

Collector는 실패를 `print`만 하고 정상 종료하면 안 된다.

```text
잘못된 방식
오류 로그 출력 → exit 0

올바른 방식
오류 로그 출력 → exit 1
```

그래야 Airflow가 실패를 정확하게 감지할 수 있다.

---

## 9. 로그 연결

Collector는 실행 중 로그를 표준 출력(stdout/stderr)으로 남긴다.

```text
Collector logger
      │
      ▼
stdout / stderr
      │
      ▼
BashOperator
      │
      ▼
Airflow Task Log
```

권장 로그 필드는 다음과 같다.

```text
run_id
source
page_no
attempt_no
status
start_time
end_time
expected_count
received_count
valid_count
invalid_count
bronze_key
silver_key
error_type
error_message
```

예:

```text
run_id=20260812T194000
source=bike
page_no=3
attempt_no=2
status=success
received_count=1000
bronze_key=bronze/bike/2026/08/12/20260812T194000/3_2.json
silver_key=silver/bike/2026/08/12/20260812T194000/3.parquet
```

이 구조를 사용하면 Airflow UI에서 실패 Task를 클릭했을 때 Collector 내부의 어떤 Page에서 문제가 발생했는지 바로 확인할 수 있다.

---

## 10. 재시도 구조

우리 시스템에서는 재시도를 두 단계로 나눈다.

### 10.1 Collector 내부 Retry

API Page 단위의 일시적인 오류는 Collector 내부에서 처리한다.

```text
API Page Request
      │
      ├─ 성공 → Bronze → Transform → Silver
      │
      └─ 실패
           │
           ├─ exponential backoff + jitter
           ├─ retry
           ├─ 최대 3회 (최초 호출 포함)
           └─ 모두 실패 → Collector failure
```

재시도 단위는 개별 row가 아니라 해당 row가 포함된 **최대 1000건 API Page 전체**다.

### 10.2 Airflow Task Retry

Collector 전체 프로세스가 실패했을 경우 Airflow가 Task 자체를 다시 실행한다.

예:

```python
from datetime import timedelta

collect_bike = BashOperator(
    task_id="collect_bike",
    bash_command="...",
    retries=2,
    retry_delay=timedelta(minutes=1),
    execution_timeout=timedelta(minutes=4),
)
```

Airflow retry에서도 같은 논리 실행의 `run_id`를 유지한다.

Collector는 이미 정상 저장된 Page를 확인하여 불필요하게 다시 처리하지 않거나, 재처리되더라도 최종 Silver가 멱등적으로 동일한 위치에 기록되도록 설계한다.

---

## 11. AWS 전체 DAG 권장 구조

수집 주기와 컴퓨팅 역할이 다르기 때문에 모든 작업을 하나의 거대한 DAG로 묶기보다는 **운영 목적별 DAG로 분리**하는 것을 권장한다.

### 11.1 실시간/준실시간 수집 DAG

```text
realtime_collection_5m

Airflow EC2
    │
    ├─ SSM → Collector EC2: bike
    ├─ SSM → Collector EC2: hotspot_population
    └─ 필요한 최신 데이터 수집 확인
                 │
                 ▼
         trigger inference
                 │
                 ▼
       SSM → Inference EC2
                 │
                 ▼
              Gold RDS
```

### 11.2 날씨 수집 DAG

날씨의 실제 갱신 주기에 맞춰 별도 DAG로 둘 수 있다.

```text
weather_collection

Airflow EC2
    │
    └─ SSM → Collector EC2: weather
                    │
                    ▼
               S3 Silver
```

### 11.3 일 단위 수집/정리 DAG

```text
daily_collection_and_compaction

Airflow EC2
    │
    ├─ SSM → Collector EC2: event
    ├─ SSM → Collector EC2: grid_population
    └─ Bronze daily compaction
                    │
                    ▼
               S3 Bronze/Silver
```

### 11.4 Feature Mart DAG

```text
feature_mart

Airflow EC2
    │
    │ EMR Job 제출
    ▼
EMR
    │
    ├─ S3 Silver 장기 데이터 조회
    ├─ 대규모 Join / Aggregation
    └─ Feature Mart 생성
                    │
                    ▼
             S3 Feature Mart
```

### 11.5 Model Training DAG

```text
model_training

Airflow EC2
    │
    │ EMR Job 제출
    ▼
EMR
    │
    ├─ Feature Mart 조회
    ├─ 모델 학습
    ├─ 모델 평가
    └─ 모델 Artifact 저장
                    │
                    ▼
                S3 Models
                    │
                    ▼
             Inference EC2
```

Airflow는 EC2/EMR 사이에 실제 데이터를 전달하지 않는다. 실제 데이터는 S3에 저장하고 Airflow는 실행 순서와 작은 메타데이터만 관리한다.

---

## 12. 권장 폴더 구조

현재 `collector/`에는 실행 코드가 아직 없으므로 다음과 같이 시작할 수 있다.

```text
collector/
├── pyproject.toml
├── uv.lock
├── main.py
├── bike.py
├── weather.py
├── hotspot_population.py
├── grid_population.py
├── event.py
└── rental_history.py
```

공통 로직은 `libs/core`로 분리한다.

```text
libs/core/src/core/
├── s3.py
├── logging.py
├── retry.py
├── quality.py
└── time.py
```

그리고 Airflow 쪽은 다음 정도만 유지한다.

```text
airflow/
├── pyproject.toml
├── uv.lock
└── dags/
    ├── realtime_collection.py
    ├── daily_collection.py
    └── training.py
```

핵심은 DAG 파일에 다음 로직을 넣지 않는 것이다.

```text
API URL 조립
JSON parsing
S3 key 생성
데이터 validation
Parquet 변환
```

이 로직은 모두 Collector 또는 `libs/core`에 있어야 한다.

---

## 13. 로컬 환경의 역할

로컬 Docker Compose 환경은 **운영 아키텍처가 아니라 개발·통합 테스트용 축소 환경**이다.

개발자는 로컬에서 다음을 검증할 수 있다.

```text
Airflow Container
→ Collector CLI 실행
→ MinIO Bronze/Silver
```

하지만 실제 운영 흐름은 다음이다.

```text
Airflow EC2
→ SSM
→ Collector EC2
→ S3 Bronze/Silver
```

따라서 로컬에서 `BashOperator`로 Collector를 실행하는 것은 SSM 연동 전에 Collector CLI와 DAG 계약을 빠르게 검증하기 위한 테스트 방법일 뿐, 최종 운영 연결 방식이 아니다.

---

## 14. Airflow EC2 → Collector EC2 실행 상세

Airflow DAG는 AWS SDK 또는 Airflow AWS Provider를 통해 SSM Run Command를 요청한다.

논리 흐름은 다음과 같다.

```text
1. Airflow DAG Run 시작
2. 논리 run_id 결정
3. source 결정
4. SSM SendCommand 호출
5. Collector EC2에서 collector CLI 실행
6. SSM command_id 반환
7. Airflow가 command 상태 polling
8. Success / Failed / TimedOut 확인
9. Airflow Task 상태 결정
10. 성공한 경우 downstream 실행
```

예시 명령은 다음과 같다.

```bash
cd /app/DE_team2-GangnamguUmBokDong/collector
uv run python main.py \
  --source bike \
  --run-id 20260812T194000
```

Collector EC2에는 배포된 Git repository와 `collector` 전용 uv 환경이 존재한다고 가정한다.

---

## 15. AWS 권장 전체 실행 구조

```text
                         ┌──────────────────────┐
                         │      Airflow EC2     │
                         │                      │
                         │ Scheduler / DAG      │
                         │ Dependency / Retry   │
                         └──────┬───────┬───────┘
                                │       │
                         SSM    │       │ EMR Job API
                                │       │
                ┌───────────────┘       └──────────────┐
                ▼                                      ▼
       ┌──────────────────┐                   ┌──────────────────┐
       │   Collector EC2  │                   │       EMR        │
       │                  │                   │                  │
       │ API Collection   │                   │ Feature Mart     │
       │ Transform / QC   │                   │ Model Training   │
       └────────┬─────────┘                   └────────┬─────────┘
                │                                      │
                ▼                                      ▼
       ┌──────────────────┐                   ┌──────────────────┐
       │ S3 Bronze/Silver │◀─────────────────▶│ S3 Mart / Models │
       └────────┬─────────┘                   └────────┬─────────┘
                │                                      │
                └──────────────┐             ┌─────────┘
                               ▼             ▼
                         ┌──────────────────┐
                         │  Inference EC2   │
                         │                  │
                         │ Model Load       │
                         │ Prediction       │
                         └────────┬─────────┘
                                  │
                                  ▼
                            ┌─────────────┐
                            │  Gold RDS   │
                            └──────┬──────┘
                                   │
                                   ▼
                            ┌─────────────┐
                            │ Backend EC2 │
                            │   FastAPI   │
                            └──────┬──────┘
                                   │
                                   ▼
                                Client
```

이 구조에서 컴퓨팅 역할은 다음처럼 고정한다.

| 컴포넌트 | 역할 |
| --- | --- |
| Airflow EC2 | 전체 워크플로 오케스트레이션 |
| Collector EC2 | 외부 API 수집, Bronze/Silver 생성 |
| Inference EC2 | 주기적 모델 추론 및 Gold 생성 |
| EMR | Feature Mart 생성, 모델 학습, 대규모 Batch |
| S3 Bronze | 원본 API 응답 보존 |
| S3 Silver | 정제·검증된 장기 데이터 |
| S3 Mart/Models | 학습 Feature와 모델 Artifact |
| RDS Gold | 서비스용 최신 예측·재배치 결과 |
| Backend EC2 | FastAPI 기반 Gold 조회·제공 |

---

## 16. Airflow가 각 컴퓨팅 자원을 호출하는 방식

Airflow는 컴포넌트별로 실행 방식을 구분한다.

```text
Collector EC2
Airflow → SSM Run Command → Collector CLI

Inference EC2
Airflow → SSM Run Command → Predict CLI

EMR
Airflow → EMR Step / Job 제출 → 상태 Sensor/확인

Gold RDS
Airflow가 직접 데이터를 생성하지 않음
Inference EC2가 결과를 Write

Backend EC2
상시 서비스 프로세스이므로 일반적인 주기 DAG에서 실행하지 않음
```

Airflow가 모든 컴퓨팅의 코드를 직접 실행하는 것이 아니라 각 실행 환경에 **명령 또는 Job을 제출하고 완료 상태를 관찰**하는 형태로 통일한다.

---

## 15. AWS 권장 구조

```text
┌──────────────────────────────────────┐
│              Airflow EC2            │
│                                      │
│ Scheduler                            │
│ DAG                                  │
│ Retry / Dependency                   │
└──────────────────┬───────────────────┘
                   │
                   │ SSM Run Command
                   ▼
┌──────────────────────────────────────┐
│          Collection EC2             │
│                                      │
│ Git Repository                       │
│ collector/.venv                      │
│                                      │
│ uv run python main.py                │
└──────────────┬───────────┬───────────┘
               │           │
               │           │
               ▼           ▼
          External API    S3
                       ┌────────┐
                       │ Bronze │
                       └────────┘
                           │
                           ▼
                       ┌────────┐
                       │ Silver │
                       └────────┘
```

Collector 종료 후 Airflow는 SSM Command 상태를 확인한다.

```text
Pending
  ↓
InProgress
  ↓
Success / Failed / TimedOut
```

`Success`일 때만 downstream Task를 실행한다.

---

## 16. Airflow에서 SSM을 사용할 때의 논리 흐름

```text
1. DAG 시작
2. run_id 생성
3. SSM SendCommand 호출
4. Collection EC2에서 Collector 실행
5. command_id 반환
6. Airflow가 command status 확인
7. Collector exit 0 → SSM Success
8. Collector exit != 0 → SSM Failed
9. Airflow Task 성공/실패 판정
10. 성공 시 다음 Task 실행
```

의사코드로 표현하면 다음과 같다.

```python
command_id = ssm.send_command(
    instance_id=COLLECTOR_INSTANCE_ID,
    command=(
        "cd /app/DE_team2-GangnamguUmBokDong/collector && "
        "uv run python main.py "
        f"--source bike --run-id {run_id}"
    ),
)

status = wait_for_ssm_command(command_id)

if status != "Success":
    raise AirflowException(f"collector failed: {status}")
```

실제 구현 시 SSM 호출과 polling 로직은 별도 공통 함수 또는 Operator로 분리하는 것이 좋다.

---

## 17. Airflow와 Collector 사이에 전달하지 말아야 할 것

Airflow XCom으로 실제 수집 데이터를 전달하지 않는다.

잘못된 구조:

```text
Collector
   ↓
수천~수만 row JSON
   ↓
XCom
   ↓
다음 Task
```

권장 구조:

```text
Collector
   ↓
S3 Bronze / Silver

Airflow XCom
   ↓
run_id
S3 key
row count
status 같은 작은 metadata만 전달
```

데이터 전달은 S3를 사용하고, Airflow는 제어 정보만 전달한다.

---

## 18. 보안과 설정값 관리

Airflow DAG 코드에 API Key, AWS Access Key, SSH Key 등을 직접 적지 않는다.

로컬에서는 `.env`를 사용하고, 운영 환경에서는 IAM Role / Airflow Connection / AWS Secrets Manager 등의 방식으로 분리한다.

특히 AWS에서는 가능하면 다음처럼 구성한다.

```text
Airflow EC2 IAM Role
└─ ssm:SendCommand
└─ ssm:GetCommandInvocation

Collection EC2 IAM Role
└─ S3 Bronze/Silver Write
└─ 필요한 Secret Read
```

즉 Airflow가 S3에 직접 모든 데이터를 쓰는 권한을 갖기보다 각 컴포넌트가 필요한 최소 권한만 가진다.

---

## 19. 장애 시 동작

### Collector 내부 API 실패

```text
API 실패
  ↓
Collector page retry
  ↓
최대 3회 실패
  ↓
Collector exit 1
  ↓
Airflow Task failure
  ↓
Airflow Task retry
```

재시도 시 `attempt_no`가 증가한 Bronze 원본은 별도 객체로 남긴다.

### Airflow 장애

Airflow 자체가 잠시 중단되더라도 이미 S3에 저장된 Bronze/Silver는 유지된다. Airflow 복구 후 동일 `run_id`를 기준으로 재처리할 수 있어야 한다.

---

## 20. 구현 순서

AWS 운영 구조를 최종 목표로 두되, 실행 인터페이스부터 작은 단위로 검증한다.

### 1단계 — Collector CLI 확정

```text
uv run python main.py --source bike --run-id ...
```

Collector EC2에서 Airflow 없이 단독으로 실행 가능해야 한다.

### 2단계 — Airflow EC2 → Collector EC2 SSM 연결

```text
Airflow DAG
→ SSM SendCommand
→ Collector EC2
→ collector CLI
→ S3 Bronze/Silver
```

여기까지가 Airflow-Collector 연동의 첫 번째 완료 기준이다.

### 3단계 — 실패/재시도/로그 연결

```text
Collector page retry
Collector exit code
SSM command status
Airflow task retry
run_id
structured log
```

을 연결한다.

### 4단계 — Inference EC2 연결

```text
Collection 완료
→ Airflow
→ SSM
→ Inference EC2
→ S3 Silver 조회
→ Gold RDS Write
```

### 5단계 — EMR Feature Mart 연결

```text
Airflow
→ EMR Job
→ S3 Silver
→ Feature Mart
→ S3
```

### 6단계 — EMR Model Training 연결

```text
Airflow
→ EMR Training Job
→ Feature Mart
→ Model Artifact
→ S3 Models
```

### 7단계 — End-to-End 운영 검증

```text
External API
→ Collector EC2
→ S3 Bronze/Silver
→ Inference EC2
→ RDS Gold
→ Backend EC2
→ Client

S3 Silver
→ EMR Feature Mart
→ EMR Training
→ S3 Model
→ Inference EC2
```

전체 경로를 Airflow에서 관찰할 수 있도록 한다.

---

## 21. 최종 권장안

우리 프로젝트의 기준 아키텍처는 **AWS에서 컴포넌트를 역할별로 분리한 구조**다.

```text
Airflow EC2
    │
    ├─ SSM → Collector EC2
    │            │
    │            ├─ External API
    │            ├─ S3 Bronze
    │            └─ S3 Silver
    │
    ├─ SSM → Inference EC2
    │            │
    │            ├─ S3 Silver 조회
    │            ├─ S3 Model 조회
    │            └─ RDS Gold Write
    │
    └─ EMR Job → Feature Mart / Model Training
                 │
                 ├─ S3 Silver Read
                 └─ S3 Mart / Model Write

RDS Gold
    │
    ▼
Backend EC2 (FastAPI)
    │
    ▼
Client
```

최종적으로 기억해야 할 경계는 다음과 같다.

```text
Airflow EC2
= Schedule + Dependency + Task Retry + Monitoring

Collector EC2
= API + Page Retry + Bronze + Transform + Validation + Silver

Inference EC2
= 최신 Silver + Model → Prediction → Gold

EMR
= 장기 Silver → Feature Mart → Training

S3
= Bronze / Silver / Feature Mart / Model Artifact 저장 계층

RDS Gold
= 서비스용 결과 저장소

Backend EC2
= Gold 조회 및 API Serving
```

Airflow와 Collector 사이의 핵심 실행 계약은 다음이다.

```text
run_id
+ source
+ SSM command_id
+ process exit code
+ structured log
```

그리고 데이터는 Airflow를 통과하지 않는다.

```text
Control Plane
Airflow → SSM / EMR Job

Data Plane
External API → Collector → S3
S3 → Inference / EMR
Inference → RDS Gold
RDS Gold → Backend API
```

이 구분을 유지하면 각 컴퓨팅 리소스가 독립적으로 장애·확장될 수 있고, 향후 AWS 자원 할당이 줄어들어 일부 EC2 역할을 합치더라도 애플리케이션의 책임 경계와 실행 인터페이스는 그대로 유지할 수 있다.