# Airflow 구현 작업 단위

> **보관 문서:** 초기 구현을 이슈·브랜치 단위로 나눈 작업 기록이다. 완료 여부나 현재 운영 구조를 나타내지 않는다. 현재 구조는 [Airflow 운영 구조와 데이터 흐름](./explain.md)을 참고한다.

> 전체 설계와 역할 분리는 [Airflow 구현 계획](./implementation-plan.md)을 참고한다. 이 문서는 해당 계획을 실제 이슈·브랜치 단위로 나누고, 어떤 순서로 구현할지 정의한다.

## 브랜치 규칙

프로젝트 공통 브랜치 규칙을 따른다.

- `타입/설명` 형식, 소문자 + 하이픈(kebab-case)
- 타입: `feature` · `fix` · `refactor` · `docs` · `test` · `chore`
- 작업 브랜치는 항상 최신 `develop`에서 생성한다.
- 하나의 이슈는 하나의 브랜치에서 처리하고 PR을 통해 병합한다.
- Airflow와 Collector는 서로 다른 애플리케이션으로 유지한다.
- Airflow 코드에서 Collector 내부 모듈을 import하여 직접 실행하지 않는다.

---

## 구현 원칙

Airflow는 **언제 무엇을 실행할지** 관리한다.

Collector는 **데이터를 어떻게 수집하고 처리할지** 책임진다.

따라서 Airflow 구현 범위에는 다음이 포함된다.

- DAG 스케줄
- Collector Task 생성
- 병렬 실행
- Task dependency
- Airflow retry
- timeout
- Backfill 스케줄링
- Compaction 스케줄링
- Task 성공/실패 상태 관리

다음은 Airflow에서 구현하지 않는다.

- 외부 API 호출
- API 인증
- 페이지네이션
- API 내부 retry
- 응답 파싱
- Bronze 저장
- Silver 변환
- 데이터 품질 판정
- 실패 페이지 계산
- 부분 실패 비율 계산

위 기능은 Collector 책임이다.

---

## 작업 목록

| # | 브랜치 | 내용 | 주요 산출물 | 선행 |
| --- | --- | --- | --- | --- |
| 1 | `chore/airflow-skeleton` | Airflow 디렉토리 스켈레톤 및 구현 명세 작성 | `airflow/orchestration/`<br>`airflow/config/`<br>`airflow/callbacks/`<br>`airflow/tests/`<br>`docs/airflow/*` | — |
| 2 | `feature/airflow-collector-task` | Collector CLI를 실행하는 공통 Task builder 구현 | `airflow/orchestration/collector_task.py` | 1 |
| 3 | `feature/airflow-config` | source 목록, timezone, cron 등 Airflow 공통 설정 구현 | `airflow/config/sources.py`<br>`airflow/config/schedules.py` | 1 |
| 4 | `feature/airflow-realtime-collection` | 5분 Trigger 기반 실시간 수집 DAG, source별 병렬 실행 | `airflow/dags/realtime_collection.py` | 2·3 |
| 5 | `test/airflow-realtime-dag` | DAG import, schedule, 병렬 구조, retry, timeout, run_id 검증 | `airflow/tests/test_dag_imports.py`<br>`airflow/tests/test_realtime_collection.py` | 4 |
| 6 | `feature/airflow-task-callbacks` | Task 성공/실패 공통 callback 및 운영 로그 확장 지점 구현 | `airflow/callbacks/task_callbacks.py` | 1 |
| 7 | `feature/airflow-backfill` | 기술적 실패 복구 + 대여이력 최근 7일 재수집을 실행하는 일일 Backfill DAG | `airflow/dags/backfill.py` | Collector `--backfill` 계약 확정 |
| 8 | `test/airflow-backfill` | Backfill 스케줄, realtime DAG와 독립성, CLI 인자, retry 검증 | `airflow/tests/test_backfill.py` | 7 |
| 9 | `feature/airflow-bronze-compaction` | 일 단위 Data Interval 기반 Bronze Compaction DAG | `airflow/dags/bronze_compaction.py` | Compaction 실행 인터페이스 확정 |
| 10 | `test/airflow-bronze-compaction` | 일 단위 Data Interval 및 재처리 대상 기간 검증 | `airflow/tests/test_bronze_compaction.py` | 9 |
| 11 | `feature/airflow-integration` | 로컬 Docker에서 Airflow → BashOperator → Collector CLI end-to-end 검증 | DAG 및 로컬 통합 테스트 | 4·5, Collector CLI 완료 |
| 12 | `feature/airflow-remote-collector` | 4 EC2 전환 시 SSH 또는 SSM 기반 Collector 원격 실행 계층 구현 | `airflow/orchestration/collector_task.py` 확장 또는 별도 executor 모듈 | 11, Collector EC2 준비 |
| 13 | `feature/airflow-inference` | 추론용 DAG 및 Task 구현 | `airflow/dags/inference.py` | 4·5 |
| 14 | `test/airflow-inference` | 추론 DAG 및 Task 검증 | `airflow/tests/test_inference.py` | 13 |
| 15 | `feature/airflow-gold-load` | Gold 데이터 적재용 DAG 및 Task 구현 | `airflow/dags/gold_load.py` | 13 |
| 16 | `test/airflow-gold-load` | Gold 적재 DAG 및 Task 검증 | `airflow/tests/test_gold_load.py` | 15 |
| 17 | `feature/airflow-emr-job` | EMR Job 실행 DAG 및 Task 구현 | `airflow/dags/emr_job.py` | 13 |
| 18 | `test/airflow-emr-job` | EMR Job DAG 및 Task 검증 | `airflow/tests/test_emr_job.py` | 17 |
| 19 | `feature/airflow-integration-extended` | 추론·Gold Load·EMR Job 통합 테스트 | 통합 테스트 스크립트 | 14·16·18 |

현재 진행 상태:

- [ ] 1. Airflow 스켈레톤 및 명세
- [ ] 2. Collector Task builder
- [ ] 3. Airflow 공통 config
- [ ] 4. 실시간 수집 DAG
- [ ] 5. 실시간 DAG 테스트
- [ ] 6. Task callback
- [ ] 7. Backfill DAG
- [ ] 8. Backfill DAG 테스트
- [ ] 9. Bronze Compaction DAG
- [ ] 10. Bronze Compaction 테스트
- [ ] 11. 로컬 통합 테스트
- [ ] 12. 원격 Collector 실행
- [ ] 13. 추론 DAG 및 Task
- [ ] 14. 추론 DAG 테스트
- [ ] 15. Gold Load DAG 및 Task
- [ ] 16. Gold Load DAG 테스트
- [ ] 17. EMR Job DAG 및 Task
- [ ] 18. EMR Job DAG 테스트
- [ ] 19. 추론·Gold Load·EMR 통합 테스트

---

## #1 `chore/airflow-skeleton`

### 목적

Airflow 구현 전에 파일 구조와 책임 경계를 먼저 고정한다.

다른 팀원이 특정 파일을 맡더라도 구현 결과가 크게 달라지지 않도록 각 파일 상단에 다음을 명시한다.

- 모듈의 역할
- 입력/출력 계약
- Airflow와 Collector 책임 경계
- 구현 금지 사항
- 테스트 기준

### 생성 구조

```text
airflow/
├── dags/
│   ├── realtime_collection.py
│   ├── backfill.py
│   └── bronze_compaction.py
│
├── orchestration/
│   ├── __init__.py
│   └── collector_task.py
│
├── config/
│   ├── __init__.py
│   ├── sources.py
│   └── schedules.py
│
├── callbacks/
│   ├── __init__.py
│   └── task_callbacks.py
│
└── tests/
    ├── test_dag_imports.py
    ├── test_realtime_collection.py
    ├── test_backfill.py
    └── test_bronze_compaction.py
```

문서:

```text
docs/airflow/
├── implementation-plan.md
└── implementation-issues.md
```

### 완료 기준

- 필요한 디렉토리와 파일이 존재한다.
- 주요 `.py` 파일에 역할과 구현 계약이 docstring으로 작성되어 있다.
- 실제 API 호출 코드가 Airflow 영역에 존재하지 않는다.
- 테스트용 임시 DAG는 제거하고 최종 구조만 남긴다.

---

## #2 `feature/airflow-collector-task`

### 목적

DAG마다 반복되는 Collector 실행 코드를 공통화한다.

예상 인터페이스:

```python
build_collector_task(
    source="bike",
)
```

### 구현 위치

```text
airflow/orchestration/collector_task.py
```

### 현재 3 EC2 실행 방식

Airflow와 Collector가 동일 EC2에 위치한다.

```text
Airflow
↓
BashOperator
↓
Collector CLI
```

Collector 호출 예:

```bash
cd /workspace/collector && \
env -u VIRTUAL_ENV uv run python main.py \
    --source bike \
    --run-id 20260813T140000
```

### 공통 정책

초기값:

```text
retries = 2
retry_delay = 30초
execution_timeout = 4분
```

`retries=2`는 최초 실행을 포함해 최대 3번 Collector 프로세스를 실행한다.

### run_id

run_id는 DAG Trigger 시각의 `logical_date`를 Asia/Seoul 기준으로 변환해 사용한다.

형식:

```text
YYYYMMDDTHHMMSS
```

예:

```text
2026-08-13 14:05 KST
→ 20260813T140500
```

같은 DAG Run의 모든 source는 동일한 run_id를 사용한다.

Airflow retry에서도 run_id를 변경하지 않는다.

### 종료 코드 계약

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

Airflow는 `failed_pages`, `failure_ratio`를 직접 판단하지 않는다.

### 완료 기준

- source를 받아 BashOperator를 생성할 수 있다.
- 공통 retry와 timeout이 적용된다.
- run_id template이 CLI에 전달된다.
- Collector 내부 모듈을 import하지 않는다.

---

## #3 `feature/airflow-config`

### 목적

DAG별로 반복되는 source 목록과 스케줄 값을 한 곳에서 관리한다.

### `config/sources.py`

관리 대상:

- 실시간 Polling source 목록
- Backfill 대상 source 구분

예:

```python
REALTIME_SOURCES = (
    "bike",
    "population",
    "weather",
)
```

실제 source 이름은 Collector CLI가 지원하는 식별자와 반드시 일치해야 한다.

Airflow config에는 API URL, 페이지 크기, 인증키를 넣지 않는다.

### `config/schedules.py`

관리 대상:

```text
TIMEZONE
REALTIME_CRON
BACKFILL_CRON
COMPACTION_CRON
```

초기 실시간 정책:

```text
TIMEZONE = Asia/Seoul
REALTIME_CRON = */5 * * * *
```

### 완료 기준

- DAG에서 cron 문자열을 직접 반복하지 않는다.
- source 추가 시 realtime DAG 본문 수정이 필요 없다.
- Collector 세부 설정이 Airflow config에 섞이지 않는다.

---

## #4 `feature/airflow-realtime-collection`

### 목적

5분마다 현재 시점 API를 Polling하도록 Collector를 실행한다.

### 스케줄 방식

`CronTriggerTimetable`을 사용한다.

```text
14:00 → 14:00 Run
14:05 → 14:05 Run
14:10 → 14:10 Run
```

실시간 Collector는 `14:00~14:05` 같은 구간을 처리하는 Batch가 아니므로 Data Interval 기반 스케줄을 사용하지 않는다.

### Task 생성

`REALTIME_SOURCES`를 순회하며 source별 Task를 생성한다.

```text
              realtime_collection
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
 collect_bike   collect_population  collect_weather
```

source별 Task 사이에 dependency를 설정하지 않는다.

따라서 Airflow Executor와 EC2 자원이 허용하면 병렬 실행한다.

### DAG 정책

```text
catchup = False
max_active_runs = 1
```

`max_active_runs=1`은 서로 다른 5분 DAG Run의 중첩을 막기 위한 값이며, 동일 Run 내부의 source별 Task 병렬 실행을 막지 않는다.

### 완료 기준

- Airflow UI에서 DAG가 정상 로드된다.
- 5분 Trigger 시각에 Run이 생성된다.
- source별 Task가 독립적으로 생성된다.
- 모든 Task가 같은 run_id를 Collector에 전달한다.
- source 하나가 retry 중이어도 다른 source의 실행 자체를 막지 않는다.

---

## #5 `test/airflow-realtime-dag`

### 목적

실제 API가 아니라 Airflow 오케스트레이션 구조를 테스트한다.

### 검증 항목

- DAG import error가 없는가
- `CronTriggerTimetable`을 사용하는가
- cron이 5분 주기인가
- timezone이 `Asia/Seoul`인가
- source별 Task가 생성되는가
- source Task 사이에 upstream/downstream dependency가 없는가
- `max_active_runs=1`인가
- `retries=2`인가
- `retry_delay=30초`인가
- `execution_timeout=4분`인가
- 동일 DAG Run에서 run_id가 동일한가

### 테스트하지 않는 것

- 서울 API 정상 응답 여부
- API 인증
- 페이지네이션
- Bronze/Silver 저장

위 항목은 Collector 테스트다.

---

## #6 `feature/airflow-task-callbacks`

### 목적

Airflow Task의 성공/실패 시 공통으로 실행할 운영 로직의 확장 지점을 만든다.

### 구현 후보

```text
on_success_callback
on_failure_callback
```

초기에는 최소한 다음 context를 구조화하여 확인할 수 있도록 한다.

- dag_id
- task_id
- logical_date
- run_id
- try_number
- exception

향후 다음 기능을 붙일 수 있다.

- Slack 또는 기타 알림
- 운영 메트릭
- 중앙 로그

### 경계

Collector의 실패 페이지 목록이나 API 오류 세부 판단을 Airflow callback에서 다시 구현하지 않는다.

---

## #7 `feature/airflow-backfill`

### 목적

실시간 수집 과정에서 완전히 확보되지 않은 데이터를 하루 한 번 보완한다.

Backfill은 두 가지 목적을 가진다.

### 1. 기술적 실패 복구

Collector가 API 호출 재시도를 소진하고도 얻지 못한 조각을 다시 요청한다.

Collector 쪽에서 실패 조각을 `_retry_queue` 등 합의된 방식으로 기록하고, Airflow는 해당 Backfill 실행 인터페이스를 호출한다.

### 2. 대여이력 지연 도착 보완

대여이력은 대여 시작 시점에 최종 이력이 생성되지 않고 반납 후 기록이 생성된다.

현재 데이터 특성상 대여 후 약 3시간이 지나야 약 95%가 반납되어 기록이 조회 가능해진다.

따라서 API 호출 성공 여부와 데이터 완전성은 동일하지 않다.

매일 최근 7일 대여이력을 다시 조회해 이후 새롭게 생성된 이력을 보완한다.

### 실행 방식

하루 1회 새벽 실행한다.

실시간 DAG와는 dependency를 두지 않는다.

```text
realtime_collection ───── 독립 실행
backfill            ───── 독립 실행
```

같은 시간에 두 DAG가 실행될 수 있다.

### Collector 계약

Collector 쪽에서 합의된 `--backfill` CLI를 호출한다.

Airflow는 다음을 직접 하지 않는다.

- S3의 실패 페이지 판정
- API 페이지 재호출
- 최근 7일 데이터 중복 제거
- Silver merge

### 완료 기준

- 하루 한 번 스케줄된다.
- realtime DAG와 독립적으로 실행된다.
- Collector backfill CLI 계약을 정확히 따른다.
- Collector non-zero exit 시 Airflow retry가 동작한다.

---

## #8 `test/airflow-backfill`

### 검증 항목

- Backfill DAG가 import되는가
- 일 단위 schedule인가
- timezone이 KST인가
- realtime DAG에 dependency가 없는가
- Collector의 `--backfill` 계약을 호출하는가
- 실패 시 retry가 적용되는가

대여이력의 최근 7일 조회 로직 자체는 Collector에서 테스트한다.

---

## #9 `feature/airflow-bronze-compaction`

### 목적

하루 동안 생성된 Bronze 조각을 일 단위 장기 보관 형식으로 변환하는 실행 작업을 스케줄링한다.

Airflow는 직접 파일을 병합하지 않는다.

```text
Airflow
↓
Compaction Job 실행
↓
S3 Bronze 읽기
↓
일 단위 결과 생성
```

### Data Interval

Compaction은 Polling과 달리 특정 기간에 이미 생성된 데이터를 처리한다.

예:

```text
data_interval_start = 2026-08-13 00:00
data_interval_end   = 2026-08-14 00:00
```

이 Run은 `2026-08-13` 하루의 Bronze를 처리한다.

### 재처리

8월 13일 Run을 8월 20일에 다시 실행하더라도 처리 대상은 계속 8월 13일이어야 한다.

따라서 `today - 1 day` 방식으로 처리 날짜를 계산하지 않고 Data Interval을 실행 프로그램에 전달한다.

### 완료 기준

- 일 단위 DAG가 생성된다.
- 처리 기간이 Airflow Data Interval로 전달된다.
- 실행 날짜와 처리 대상 날짜가 분리된다.

---

## #10 `test/airflow-bronze-compaction`

### 검증 항목

- DAG import 성공
- 일 단위 schedule
- Data Interval 사용
- `data_interval_start`와 `data_interval_end` 전달
- 과거 DAG Run 재실행 시 동일 기간 유지

실제 JSON → NDJSON 변환 결과는 Compaction 프로그램에서 테스트한다.

---

## #11 `feature/airflow-integration`

### 목적

로컬 Docker 환경에서 실제 프로세스 경계까지 확인한다.

```text
Airflow Container
↓
BashOperator
↓
/workspace/collector
↓
uv run python main.py
```

### 검증 시나리오

#### 성공

```text
Collector exit 0
→ Airflow Task SUCCESS
```

#### 실패

```text
Collector exit != 0
→ Airflow Task retry
→ retry 소진 시 FAILED
```

#### run_id

5분 Trigger 시각과 Collector에 전달된 run_id가 동일해야 한다.

#### 병렬 실행

여러 source가 등록된 경우 source별 Task가 동시에 `running` 상태가 될 수 있어야 한다.

### 완료 기준

Airflow UI에서 다음을 직접 확인한다.

- scheduled Run 생성
- source별 병렬 Task
- retry
- 성공/실패 상태
- Collector 로그
- 동일 run_id

---

## #12 `feature/airflow-remote-collector`

### 목적

Collector 전용 EC2를 추가해 4 EC2 구조로 전환할 때 DAG를 크게 변경하지 않고 실행 계층만 교체한다.

### 3 EC2

```text
Airflow + Collector EC2
↓
BashOperator
```

### 4 EC2

```text
Airflow EC2
↓
SSH 또는 SSM
↓
Collector EC2
```

### SSH PoC

초기 원격 실행 검증은 SSHOperator를 사용할 수 있다.

필요 요소:

- Airflow Connection
- SSH user
- private key
- known_hosts
- Collector host
- security group

### SSM 전환

운영 환경에서는 SSM Run Command를 우선 검토한다.

장점:

- SSH Key 불필요
- inbound 22번 포트 불필요
- IAM 기반 권한 관리

### 핵심 원칙

`realtime_collection.py` 같은 DAG 본문에서 실행 위치를 판단하지 않는다.

물리적 실행 방법은 `orchestration` 계층에 숨긴다.

---

## #13 `feature/airflow-inference`

### 목적

추론용 DAG 및 Task를 구현한다.

### 구현 위치

```text
airflow/dags/inference.py
```

### 주요 내용

- 실시간 수집 결과를 기반으로 ML 추론 실행
- 추론 Task는 병렬로 실행 가능
- 실패 시 retry 정책 적용

### 완료 기준

- 추론 DAG가 Airflow UI에 등록된다.
- source별 Task가 생성되고 병렬 실행된다.
- run_id가 일관되게 전달된다.
- 실패 시 retry가 동작한다.

---

## #14 `test/airflow-inference`

### 목적

추론 DAG 및 Task의 구조와 동작을 검증한다.

### 검증 항목

- DAG import 성공
- Task 병렬 생성 확인
- retry 및 timeout 정책 확인
- run_id 일관성 검증

---

## #15 `feature/airflow-gold-load`

### 목적

Gold 데이터 적재용 DAG 및 Task를 구현한다.

### 구현 위치

```text
airflow/dags/gold_load.py
```

### 주요 내용

- Gold 데이터 적재 작업 스케줄링
- 데이터 적재 Task 병렬 실행 가능
- 실패 시 retry 정책 적용

### 완료 기준

- Gold Load DAG가 Airflow UI에 등록된다.
- Task가 병렬 생성되고 실행된다.
- run_id 일관성 유지
- 실패 시 retry가 동작한다.

---

## #16 `test/airflow-gold-load`

### 목적

Gold 적재 DAG 및 Task의 구조와 동작을 검증한다.

### 검증 항목

- DAG import 성공
- Task 병렬 생성 확인
- retry 및 timeout 정책 확인
- run_id 일관성 검증

---

## #17 `feature/airflow-emr-job`

### 목적

EMR Job 실행 DAG 및 Task를 구현한다.

### 구현 위치

```text
airflow/dags/emr_job.py
```

### 주요 내용

- EMR 클러스터 작업 실행 스케줄링
- Task 병렬 실행 및 관리
- 실패 시 retry 정책 적용

### 완료 기준

- EMR Job DAG가 Airflow UI에 등록된다.
- Task가 병렬 실행된다.
- run_id 일관성 유지
- 실패 시 retry가 동작한다.

---

## #18 `test/airflow-emr-job`

### 목적

EMR Job DAG 및 Task의 구조와 동작을 검증한다.

### 검증 항목

- DAG import 성공
- Task 병렬 생성 확인
- retry 및 timeout 정책 확인
- run_id 일관성 검증

---

## #19 `feature/airflow-integration-extended`

### 목적

추론·Gold Load·EMR Job 통합 테스트를 수행한다.

### 주요 내용

- 각 DAG 간 통합 동작 검증
- 병렬 실행 및 retry 정책 확인
- run_id 일관성 및 로그 확인

### 완료 기준

- 통합 테스트 스크립트 정상 동작
- Airflow UI에서 통합 DAG 정상 실행 확인
- 실패 시 재시도 및 상태 관리 확인

---

## 진행 순서

선행 관계를 기준으로 다음 순서로 진행한다.

| 라운드 | 동시 진행 가능 |
| --- | --- |
| 1 | #1 |
| 2 | #2 · #3 · #6 |
| 3 | #4 |
| 4 | #5 · #7 · #9 |
| 5 | #8 · #10 · #11 |
| 6 | #12 |
| 7 | #13 |
| 8 | #14 · #15 · #17 |
| 9 | #16 · #18 · #19 |

```text
#1 skeleton
 ├─ #2 collector task ─┐
 ├─ #3 config ─────────┼─ #4 realtime ─ #5 realtime test ─┐
 └─ #6 callbacks       │                                  │
                      │                                  ├─ #11 integration ─ #12 remote
Collector backfill ────┴────────────── #7 backfill ─ #8 ─┤
Compaction 계약 ────────────────────── #9 compaction ─ #10┘
                                               │
                                               ├─ #13 inference ─ #14 ─┐
                                               │                      │
                                               ├─ #15 gold load ─ #16 ─┤
                                               │                      │
                                               └─ #17 emr job ─ #18 ─ #19
```

# 최종 완료 기준

Airflow 구현 전체는 다음 조건을 만족하면 완료로 본다.

### 실시간 수집

```text
5분 Trigger
↓
source별 Collector 병렬 실행
↓
SUCCESS / PARTIAL_SUCCESS → Airflow SUCCESS
FAILED → Airflow retry
```

### Backfill

```text
하루 1회
↓
기술적 실패 조각 복구
+
최근 7일 대여이력 지연 도착 보완
```

### Compaction

```text
하루 Data Interval
↓
해당 날짜의 Bronze Compaction Job 실행
```

### 추론·서빙

```text
실시간 수집 결과 기반으로
추론 DAG 병렬 실행
↓
성공 시 Gold Load DAG 실행
↓
실패 시 Airflow retry
```

### Gold Load

```text
추론 결과 기반으로
Gold 데이터 적재 실행
↓
성공 시 EMR Job DAG 실행
↓
실패 시 Airflow retry
```

### EMR Job

```text
Gold 적재 완료 후
EMR 작업 실행
↓
성공 시 완료
↓
실패 시 Airflow retry
```

### 인프라

3 EC2에서는 BashOperator로 로컬 Collector를 실행하고,
Collector EC2가 추가되면 동일 DAG 구조를 유지하면서 SSH/SSM 실행 방식으로 전환할 수 있어야 한다.

### 책임 경계

최종적으로 다음 원칙이 코드 구조에서도 유지되어야 한다.

> **Airflow는 실행과 dependency를 오케스트레이션하고, Collector·Inference·Gold Load·EMR Job은 각자의 데이터 처리 책임을 수행한다.**
