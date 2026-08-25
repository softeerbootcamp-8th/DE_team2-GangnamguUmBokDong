# Airflow ↔ Collector 구성 및 스케줄링 설계

> **보관 문서:** EC2 분리 구성과 SSH/SSM을 검토하던 초기 설계 기록이다. 현재 운영 구조는 [Airflow 운영 구조와 데이터 흐름](./explain.md)을 따른다. 현재 구현 여부는 `airflow/dags/`, `airflow/orchestration/`, `ops/compose/`에서 확인한다.

## 1. 목적

이 문서는 우리 시스템에서 **Airflow가 Collector를 어떻게 스케줄링하고 실행하는지**, 그리고 AWS EC2를 **4대로 분리하는 경우와 3대로 통합하는 경우의 차이**를 정리한다.

다루는 내용은 다음 네 가지로 한정한다.

1. Airflow의 Collector 스케줄링 역할
2. EC2 4대 구성과 3대 구성 비교
3. EC2 3대 구성에서 Airflow와 Collector의 실행 방식
4. EC2 4대 구성에서 Airflow와 Collector의 원격 실행 방식 비교: SSH vs SSM

---

## 2. Airflow의 Collector 스케줄링 역할

Airflow는 데이터를 직접 수집하거나 정제하는 컴포넌트가 아니다.

Airflow의 역할은 **Collector가 언제 실행되어야 하는지 결정하고, 실행을 요청하고, 성공/실패에 따라 다음 작업을 제어하는 것**이다.

```text
Airflow
= Scheduler + Orchestrator

Collector
= 실제 수집 작업을 수행하는 Worker Application
```

### 2.1 역할 분리

| 구분 | Airflow | Collector |
| --- | --- | --- |
| 실행 시각 결정 | 담당 | 담당하지 않음 |
| 주기 관리 | 담당 | 담당하지 않음 |
| Task dependency | 담당 | 담당하지 않음 |
| Task-level retry | 담당 | 담당하지 않음 |
| timeout 관리 | 담당 | 담당하지 않음 |
| Collector 실행 요청 | 담당 | 요청을 받아 실행 |
| 외부 API 호출 | 담당하지 않음 | 담당 |
| pagination | 담당하지 않음 | 담당 |
| API page retry | 담당하지 않음 | 담당 |
| 데이터 저장/정제 | 담당하지 않음 | 담당 |
| 최종 성공/실패 반환 | 결과를 판정 | exit code로 반환 |

즉 Airflow DAG 안에 실제 API 수집 로직을 작성하지 않는다.

```text
Airflow DAG
    │
    │ Collector 실행
    ▼
Collector
    │
    ├─ API 호출
    ├─ 내부 재시도
    ├─ 데이터 처리
    └─ exit code 반환
          │
          ▼
       Airflow
          │
          ├─ success → 다음 Task
          └─ failure → Airflow retry / DAG failure
```

### 2.2 5분 주기 수집 예시

5분마다 실행하는 수집 작업이라면 Airflow가 논리적인 실행 시각을 관리한다.

```text
09:00 DAG Run
   ↓
Collector 실행
   ↓
성공/실패 확인

09:05 DAG Run
   ↓
Collector 실행
   ↓
성공/실패 확인

09:10 DAG Run
   ↓
Collector 실행
   ↓
성공/실패 확인
```

Airflow는 Collector에 최소한 다음과 같은 실행 정보를 전달한다.

```text
source
run_id
필요한 경우 base_dttm
```

예:

```bash
uv run python main.py \
  --source bike \
  --run-id 20260813T090000
```

Collector는 작업이 성공하면 `exit 0`, 최종 실패하면 non-zero exit code를 반환한다.

```text
Collector exit 0
→ Airflow Task success

Collector exit != 0
→ Airflow Task failure
→ 필요 시 Airflow Task retry
```

### 2.3 재시도 책임

재시도는 두 계층으로 나눈다.

```text
Airflow Task Retry
└─ Collector 전체 실행 재시도
   └─ Collector 내부 API Page Retry
```

- API 요청 자체의 일시적인 오류는 Collector 내부에서 처리한다.
- Collector 전체 실행이 최종적으로 실패한 경우 Airflow가 Task 단위로 재시도한다.

이렇게 역할을 나누면 DAG가 API 세부 구현에 종속되지 않는다.

---

## 3. EC2 4대 구성과 3대 구성 비교

우리 시스템에서 EC2를 역할별로 완전히 분리하면 다음과 같이 4대를 사용할 수 있다.

### 3.1 EC2 4대 구성

```text
EC2 #1
Airflow

EC2 #2
Collector

EC2 #3
Inference

EC2 #4
Backend
```

이 경우 Airflow와 Collector가 서로 다른 EC2에 있으므로 **원격 실행 방식이 필요하다.**

```text
Airflow EC2
    │
    │ SSM 또는 SSH
    ▼
Collector EC2
```

### 3.2 EC2 3대 구성

Airflow와 Collector를 하나의 EC2에 배치하면 다음과 같다.

```text
EC2 #1
Airflow + Collector

EC2 #2
Inference

EC2 #3
Backend
```

이 경우 Airflow와 Collector가 같은 EC2에 있으므로 원격 통신이 필요하지 않다.

```text
Airflow
    │
    │ local process
    ▼
Collector
```

### 3.3 전체 비교

| 비교 항목 | EC2 4대 | EC2 3대 |
| --- | --- | --- |
| 구성 | Airflow / Collector / Inference / Backend 분리 | Airflow+Collector / Inference / Backend |
| Airflow와 Collector 물리적 분리 | **분리** | 같은 EC2 |
| Airflow → Collector 실행 | **원격 실행 필요** | **로컬 실행** |
| SSM/SSH 필요 여부 | 필요 | 불필요 |
| 구조 단순성 | 보통 | **높음** |
| 자원 활용률 | 낮아질 수 있음 | **높아질 수 있음** |
| 장애 격리 | **높음** | 낮음 |
| 독립 Scaling | **높음** | Airflow와 Collector는 함께 자원 사용 |
| 독립 배포 | **유리** | 코드/환경을 분리하면 상당 부분 가능 |
| 운영 복잡도 | 높음 | **낮음** |
| 현재 프로젝트 규모 적합성 | 좋음 | **높음** |

---

## 4. EC2 4대 구성의 장단점

### 장점 1. 장애 격리

Airflow와 Collector가 다른 EC2에 있으므로 Collector의 자원 문제가 Airflow 서버에 직접 영향을 주지 않는다.

```text
Collector EC2
Memory 부족 / CPU 과부하 / 프로세스 장애

        ↓

Airflow EC2
계속 동작 가능
```

반대로 Airflow EC2에 문제가 발생하더라도 Collector EC2 자체는 별도 서버로 유지된다.

### 장점 2. 자원을 독립적으로 조절 가능

Collector 부하가 커지면 Collector EC2만 확장할 수 있다.

```text
Airflow EC2
2~4 vCPU / 4~8 GB

Collector EC2
4 vCPU / 8 GB
        ↓
8 vCPU / 16 GB
```

Airflow 자원을 불필요하게 같이 증가시킬 필요가 없다.

### 장점 3. 배포 경계가 명확함

```text
Airflow 변경
→ Airflow EC2 배포

Collector 변경
→ Collector EC2 배포
```

역할별 운영과 장애 분석이 명확하다.

### 단점 1. Airflow 전용 EC2의 자원 활용률이 낮을 수 있음

Airflow는 주로 스케줄링과 상태 관리를 담당하므로 현재 프로젝트 규모에서는 전용 EC2의 CPU가 상당 시간 idle일 수 있다.

### 단점 2. 원격 실행 계층이 추가됨

Airflow가 다른 EC2의 Collector를 실행해야 하므로 다음과 같은 연결 방식이 필요하다.

```text
Airflow EC2
→ SSM 또는 SSH
→ Collector EC2
```

이 때문에 IAM, SSM Agent 또는 SSH Key와 같은 추가 운영 요소가 생긴다.

---

## 5. EC2 3대 구성의 장단점

### 장점 1. Airflow ↔ Collector 실행이 단순함

같은 EC2이므로 원격 실행 계층이 필요 없다.

```text
Airflow
→ BashOperator / local subprocess
→ Collector CLI
```

따라서 다음 요소가 필요하지 않다.

```text
SSM SendCommand
SSM command_id polling
SSH connection
SSH private key
원격 접속용 Security Group 설정
```

### 장점 2. 자원 활용률을 높일 수 있음

Airflow는 지속적으로 무거운 연산을 하지 않고, Collector도 5분 주기로 일정 시간 동안 실행되는 batch workload다.

예:

```text
09:00 Collector 시작
09:00 ~ 09:01 수집/처리
09:01 ~ 09:05 Collector idle

Airflow Scheduler는 계속 실행
```

이 경우 Airflow와 Collector를 서로 다른 작은 EC2 두 대에 두는 것보다 하나의 EC2에서 CPU와 Memory를 공유하는 편이 효율적일 수 있다.

### 장점 3. 운영 복잡도가 감소함

```text
EC2 관리 대수
4 → 3

Airflow ↔ Collector 원격 통신
제거
```

즉 EC2 한 대만 줄어드는 것이 아니라 Airflow와 Collector 사이의 원격 실행 계층도 함께 사라진다.

### 단점 1. 장애 격리가 약해짐

Collector가 과도한 Memory를 사용하면 같은 EC2에서 실행 중인 Airflow Scheduler에도 영향을 줄 수 있다.

```text
Collector Memory 급증
    ↓
EC2 Memory Pressure
    ↓
Airflow Scheduler 영향 가능
```

EC2 자체에 장애가 발생하면 다음 두 컴포넌트가 동시에 중단된다.

```text
Airflow
+
Collector
```

### 단점 2. 자원을 독립적으로 조절하기 어려움

Collector 부하 때문에 EC2를 확장하면 Airflow가 사용하는 자원까지 함께 증가한다.

```text
Airflow + Collector EC2
4 vCPU / 8 GB
        ↓
8 vCPU / 16 GB
```

### 3대 구성을 사용해도 코드와 환경은 분리

같은 EC2에 배치한다고 해서 Airflow와 Collector를 하나의 Python project로 합치지 않는다.

```text
/app/project/

├── airflow/
│   ├── pyproject.toml
│   ├── uv.lock
│   └── .venv
│
└── collector/
    ├── pyproject.toml
    ├── uv.lock
    └── .venv
```

즉 다음 원칙을 유지한다.

```text
물리적 배치
Airflow + Collector = 같은 EC2

논리적 구조
Airflow ≠ Collector
```

이렇게 하면 나중에 EC2를 4대로 다시 분리해도 Collector 애플리케이션 자체는 그대로 유지할 수 있다.

---

## 6. EC2 3대일 때 Airflow와 Collector 실행 방식

3대 구성에서는 Airflow와 Collector가 같은 EC2에 있으므로 **BashOperator 또는 local subprocess 방식**을 사용한다.

### 구조

```text
┌──────────────────────────────────┐
│              EC2 #1              │
│                                  │
│  Airflow Scheduler               │
│         │                        │
│         │ BashOperator           │
│         ▼                        │
│  Collector CLI                   │
│                                  │
└──────────────────────────────────┘
```

Airflow는 Collector의 Python 함수를 직접 import하기보다 독립된 CLI를 호출한다.

```bash
cd /app/DE_team2-GangnamguUmBokDong/collector
uv run python main.py \
  --source bike \
  --run-id 20260813T090000
```

### 직접 import 대신 CLI를 사용하는 이유

같은 EC2에 있더라도 두 애플리케이션의 dependency를 분리하기 위해서다.

```text
Airflow Environment
        │
        │ shell command
        ▼
Collector Environment
```

Collector dependency가 변경되어도 Airflow 환경에 직접 영향을 주지 않는다.

### 성공/실패 전달

Collector process의 exit code를 Airflow가 직접 확인한다.

```text
Collector exit 0
→ BashOperator success
→ Airflow Task success

Collector exit != 0
→ BashOperator failure
→ Airflow Task failure/retry
```

### 3대 구성의 핵심 장점

원격 실행 계층 없이 다음 구조만으로 동작한다.

```text
Airflow Scheduler
→ Collector CLI
→ exit code
→ Airflow Task 상태
```

현재 프로젝트 규모에서 구현과 운영이 가장 단순한 방식이다.

---

## 7. EC2 4대일 때 Airflow와 Collector 실행 방식

4대 구성에서는 Airflow와 Collector가 서로 다른 EC2에 있으므로 원격 실행 방식이 필요하다.

주요 후보는 다음 두 가지다.

1. SSHOperator
2. AWS Systems Manager(SSM) Run Command

---

## 7.1 SSHOperator 방식

### 구조

```text
Airflow EC2
    │
    │ SSH
    │ TCP 22
    ▼
Collector EC2
    │
    ▼
Collector CLI
```

Airflow가 Collector EC2에 직접 SSH 로그인한 뒤 명령을 실행한다.

```text
Airflow
→ SSH connection
→ cd collector
→ uv run python main.py ...
→ remote process exit code
→ Airflow Task 상태
```

### 장점

- 구현이 단순하다.
- Airflow의 `SSHOperator`를 바로 사용할 수 있다.
- 원격 프로세스의 stdout/stderr를 확인하기 쉽다.
- 원격 명령의 exit code를 Airflow Task 성공/실패와 직접 연결하기 쉽다.
- 빠른 PoC에 적합하다.

### 단점

- Airflow가 Collector에 로그인하기 위한 SSH credential이 필요하다.
- SSH private key를 안전하게 저장하고 관리해야 한다.
- Collector EC2의 22번 포트를 Airflow EC2에서 접근 가능하도록 열어야 한다.
- Security Group, SSH User, known_hosts, host/IP 등을 관리해야 한다.
- SSH key 유출 및 key rotation을 고려해야 한다.

### SSH Key가 필요한 이유

SSH는 Airflow가 Collector EC2에 **직접 로그인하는 방식**이기 때문이다.

```text
Airflow EC2
   │
   │ private key로 인증
   ▼
Collector EC2 SSH Server
```

따라서 서버 접근 Credential 관리가 필요하다.

---

## 7.2 AWS SSM Run Command 방식

### 구조

```text
Airflow EC2
    │
    │ IAM Role
    │ ssm:SendCommand
    ▼
AWS Systems Manager
    │
    ▼
Collector EC2 SSM Agent
    │
    ▼
Collector CLI
```

Airflow가 Collector 서버에 직접 로그인하는 것이 아니라 AWS Systems Manager에 명령 실행을 요청한다.

```text
Airflow
→ SSM SendCommand
→ command_id
→ Collector EC2에서 CLI 실행
→ SSM Command Status
→ Airflow Task 상태
```

### 장점

- SSH private key가 필요 없다.
- Collector EC2의 inbound 22번 포트를 열 필요가 없다.
- IAM Role과 IAM Policy로 실행 권한을 관리할 수 있다.
- `command_id`를 통해 원격 명령 실행 상태를 추적할 수 있다.
- EC2 IP가 변경되어도 Instance ID 또는 tag 기반으로 대상을 지정할 수 있다.
- AWS 내부 운영 방식과 자연스럽게 결합된다.

### 단점

- SSH보다 초기 설정이 많다.
- Collector EC2가 SSM managed node로 동작해야 한다.
- SSM Agent와 필요한 IAM Role을 설정해야 한다.
- `SendCommand` 이후 작업이 완료될 때까지 command status를 확인하는 polling 로직이 필요하다.

### SSM에서 SSH Key가 필요 없는 이유

SSM은 서버 로그인 방식이 아니다.

```text
Airflow EC2
   │
   │ IAM Role
   ▼
AWS Systems Manager
   │
   ▼
Collector EC2 SSM Agent
```

AWS IAM이 Airflow EC2가 특정 Collector EC2에 명령을 실행할 권한이 있는지를 판단한다.

따라서 다음 정보를 Airflow 서버에 저장하지 않아도 된다.

```text
.pem private key
SSH password
SSH login credential
```

---

## 8. SSH와 SSM 상세 비교

### 8.1 구현 난이도

| 항목 | SSHOperator | SSM Run Command |
| --- | --- | --- |
| 초기 구현 난이도 | **낮음** | 중간 |
| 이유 | SSH Connection과 command 설정으로 바로 실행 가능 | IAM, SSM Agent, SendCommand, status polling 필요 |
| 별도 Worker/API 서버 | 불필요 | 불필요 |

PoC 관점에서는 SSH가 더 빠르다.

```text
SSH
Airflow → SSHOperator → command
```

SSM은 다음 준비 과정이 추가된다.

```text
IAM Role
→ SSM Agent
→ Managed Node
→ SendCommand
→ command_id
→ status polling
```

---

### 8.2 인증과 Credential

| 항목 | SSHOperator | SSM Run Command |
| --- | --- | --- |
| 인증 방식 | SSH Private Key / Password | **AWS IAM Role** |
| 장기 Key 관리 | **필요** | 불필요 |
| Key rotation | 고려 필요 | SSH Key rotation 불필요 |
| AWS IAM 통합 | 제한적 | **매우 자연스러움** |

SSM은 IAM 기반이므로 Airflow EC2에 장기 SSH private key를 저장하지 않는다는 장점이 있다.

---

### 8.3 네트워크와 보안

| 항목 | SSHOperator | SSM Run Command |
| --- | --- | --- |
| Collector inbound port | **22 필요** | **불필요** |
| Airflow → Collector 직접 TCP 연결 | 필요 | 불필요 |
| Security Group SSH 규칙 | 필요 | 불필요 |
| Private subnet 배치 | 가능 | **더 자연스러움** |

SSH:

```text
Airflow EC2
   │
   │ TCP 22
   ▼
Collector EC2
```

SSM:

```text
Airflow EC2 → AWS SSM
Collector EC2 SSM Agent → AWS SSM
```

SSM에서는 Airflow가 Collector의 SSH Port로 직접 접근하지 않는다.

---

### 8.4 작업 완료 상태 확인

| 항목 | SSHOperator | SSM Run Command |
| --- | --- | --- |
| 실행 시작 | SSH command 실행 | `SendCommand` |
| 실행 식별자 | SSH Task 자체 | `command_id` |
| 완료 확인 | 원격 process exit code | SSM Command Status |
| 상태 확인 난이도 | **매우 낮음** | 낮음~중간 |

SSH는 원격 프로세스가 끝나면 바로 exit code를 받을 수 있다.

```text
command
→ exit 0 / exit 1
→ Airflow success / failure
```

SSM은 다음 과정을 거친다.

```text
SendCommand
→ command_id
→ Pending
→ InProgress
→ Success / Failed / TimedOut
→ Airflow success / failure
```

따라서 상태 확인 자체는 둘 다 가능하지만 SSH가 더 직접적이고, SSM은 한 단계의 상태 조회가 추가된다.

---

### 8.5 운영 복잡도

| 운영 요소 | SSHOperator | SSM Run Command |
| --- | --- | --- |
| SSH private key | 관리 필요 | 불필요 |
| SSH User | 관리 필요 | 불필요 |
| known_hosts | 관리 필요 | 불필요 |
| Port 22 | 관리 필요 | 불필요 |
| IAM Policy | 일부 필요 가능 | **필수** |
| SSM Agent | 불필요 | **필수** |
| command status polling | 불필요 | 필요 |

즉 다음과 같은 차이가 있다.

```text
SSH
초기 구현은 단순
→ 운영하면서 Key / User / Port / Host 관리 필요

SSM
초기 설정은 조금 복잡
→ 이후 IAM과 AWS API 중심으로 운영
```

---

### 8.6 최종 비교

| 평가 항목 | SSHOperator | SSM Run Command |
| --- | --- | --- |
| 초기 구현 속도 | **매우 좋음** | 좋음 |
| 구현 난이도 | **낮음** | 중간 |
| SSH Key 관리 | 필요 | **불필요** |
| Inbound 22번 Port | 필요 | **불필요** |
| IAM 기반 권한 관리 | 보통 | **매우 좋음** |
| 실행 완료 추적 | **매우 쉬움** | 좋음 |
| 장기 운영 보안 | 보통 | **좋음** |
| AWS 환경 적합성 | 좋음 | **매우 좋음** |
| PoC 적합성 | **매우 높음** | 높음 |
| 현재 프로젝트 운영 적합성 | 높음 | **매우 높음** |

4 EC2 구조를 선택한다면 최종 우선순위는 다음과 같다.

```text
1순위: AWS SSM Run Command
2순위: SSHOperator
```

SSH는 구현이 더 빠르지만, AWS 운영 환경에서는 **SSH Key와 inbound 22번 포트를 별도로 관리하지 않고 IAM 기반으로 권한과 실행 상태를 관리할 수 있다는 점에서 SSM을 기본 방식으로 선택한다.**

---

## 9. 최종 판단

### EC2 자원이 충분한 경우

```text
EC2 #1 Airflow
EC2 #2 Collector
EC2 #3 Inference
EC2 #4 Backend
```

Airflow와 Collector를 물리적으로 분리한다.

```text
Airflow EC2
    │
    │ SSM Run Command
    ▼
Collector EC2
```

장애 격리와 독립 Scaling이 가장 중요할 때 적합하다.

### 현재 규모에서 자원 효율과 단순성을 우선하는 경우

```text
EC2 #1 Airflow + Collector
EC2 #2 Inference
EC2 #3 Backend
```

Airflow와 Collector를 같은 EC2에 배치한다.

```text
Airflow
    │
    │ BashOperator / local subprocess
    ▼
Collector CLI
```

현재 프로젝트 규모에서는 이 구조가 자원 효율과 구현 단순성 측면에서 더 현실적인 선택일 수 있다.

### 공통 원칙

3대 또는 4대 중 어떤 구성을 선택하더라도 Airflow와 Collector의 **논리적 책임과 코드 환경은 분리**한다.

```text
Airflow
= Schedule / Dependency / Retry / Monitoring

Collector
= 실제 데이터 수집 작업
```

따라서 물리적 배치는 자원 상황에 따라 변경할 수 있다.

```text
현재 3 EC2
Airflow + Collector
        │
        │ 필요 시 분리
        ▼

향후 4 EC2
Airflow EC2
    │
    │ SSM
    ▼
Collector EC2
```

즉 **애플리케이션은 논리적으로 분리하고, 물리적 EC2 배치만 현재 자원 규모와 운영 요구사항에 맞게 선택한다.**
