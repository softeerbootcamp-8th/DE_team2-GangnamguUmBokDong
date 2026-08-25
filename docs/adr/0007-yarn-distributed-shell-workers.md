# 7. LightGBM 분산 학습 워커를 YARN Distributed Shell로 기동

## Status

Accepted

## Context

[ADR-0005](0005-lightgbm-distributed-training.md)가 LightGBM 자체 분산 학습(Socket,
`tree_learner="data"`)을 채택하고 코드(`training/config.py`의 `LGB_*` 환경변수,
`train_common.py`의 분산 파라미터 주입)까지 준비해뒀지만, "워커 인프라(머신
IP/포트, 클러스터)가 아직 없다"는 전제로 `train_common.py`가
`LGB_NUM_MACHINES>1`이면 곧바로 `NotImplementedError`를 던지게 막아뒀다
(station_no 샤딩이 `lazy_train_dataset.py`의 날짜별 지연 로딩과 아직 연동되지
않았기 때문).

학습용 EC2를 더 이상 못 쓰게 되고 EMR도 m4.large(2vCPU/8GB)만 허용되는
제약이 생기면서, 이제 그 "워커 인프라"를 실제로 만들어야 하는 시점이 됐다.
실측 결과 12개월치 데이터를 단일 머신에서 학습하려면 31GB+ RAM이 필요해서
(`docs/ml/training/DESIGN.md`, `LGB_DEFER_VALID_DATASET` 도입 근거) m4.large 한
대로는 턱없이 부족하고, ADR-0005가 이미 정해둔 데이터-병렬 분산(각 머신이
station_no로 나눈 자기 몫만 들고 학습)으로 메모리와 시간을 동시에 머신 대수만큼
나눠야 한다.

**남은 문제는 "N대의 EMR core 노드에 각각 독립적인 LightGBM 프로세스를 어떻게
띄우고, 서로의 주소(host:port)를 어떻게 알게 하느냐"였다.** 원래 자연스러운
방법은 SSM(AWS Systems Manager)으로 각 노드에 원격 명령을 보내는 것인데, 이
AWS 계정은 SSM(`SendCommand`/`StartSession`/`DescribeInstanceInformation`)을
조직 SCP(Service Control Policy)로 전면 차단해뒀다(`terraform/compute_train.tf`,
`terraform/emr.tf`에 실측 기록됨). SSH도 검토했지만, 지금 있는 SSH 접근(상시
EC2 한 대에 bastion을 거쳐 사람이 수동으로 붙는 `make ssh-train`)을 "Airflow가
자동으로, 매번 새로 생기는 EMR core 노드 N대 전부에" 붙는 용도로 바꾸려면
보안그룹 개방, SSH 키 자동 배포, 노드별 병렬 접속 오케스트레이션을 전부 새로
지어야 해서 SSM만큼의 신규 인프라 부담이 그대로 남는다.

반면 **EMR Step 제출(`AddJobFlowSteps`/`RunJobFlow`의 `Steps`)은 지금도 실제로
동작하는 유일한 경로**다 — 피처마트 생성용 Spark 잡을 지금도 이 방식으로
돌리고 있다. SSM/SSH 같은 네트워크 접근이 아니라 EMR 컨트롤플레인 API 호출이라
SCP 차단 대상도 아니고, IP를 몰라도 되고, 새 보안그룹/키 배포도 필요 없다.

### YARN과 Distributed Shell이 뭔지

EMR 클러스터는 기본적으로 Hadoop YARN(Yet Another Resource Negotiator)이라는
자원관리자를 쓴다 — 클러스터 안 모든 노드의 CPU/메모리를 파악하고 있다가,
누가 "이만큼의 자원으로 이 작업을 실행해줘"라고 요청하면 여유 있는 노드에
배치해주는 역할이다. Spark 잡을 돌리면 executor들이 여러 노드에 흩어져서 뜨는
것도 이 YARN이 스케줄링해주는 것이다.

**Distributed Shell**은 Hadoop 배포판에 기본 포함된 표준 예제 YARN 애플리케이션으로,
하는 일이 정확히 하나다: *"이 셸 명령어를 N개의 컨테이너(격리된 실행 단위)로
쪼개서 클러스터 전체에 병렬로 뿌려줘."*

```
yarn jar hadoop-yarn-applications-distributedshell.jar \
  -shell_command "학습_스크립트.sh" \
  -num_containers 8 \
  -container_memory 6144 -container_vcores 2
```

이걸 지금 Spark 잡을 제출하는 것과 완전히 같은 방식(EMR Step, `command-runner.jar`)으로
제출하면, YARN이 알아서 8개 컨테이너를 core 노드들에 나눠 배치해서 8개의
독립된 LightGBM 프로세스를 띄워준다.

**다만 YARN은 "컨테이너들이 서로의 주소를 알게 해주는 것"까지는 안 해준다** —
LightGBM 소켓 분산은 각 머신이 자기 rank(몇 번째 머신인지)와 전체 머신 목록
(`LGB_MACHINES="host:port,host:port,..."`)을 미리 알아야 하는데, YARN
distributed-shell은 컨테이너를 띄워줄 뿐 이런 상호 등록(rendezvous)은
지원하지 않는다. 그래서 각 컨테이너가 시작하자마자 자기 주소를 S3의 정해진
위치에 등록하고, 정해진 개수가 다 모일 때까지 기다리는 barrier 로직을 직접
만들어야 한다(§ 아래 Decision 참고).

### 검토한 대안: SynapseML(LightGBM-on-Spark)

Spark ML과 통합된 LightGBM 래퍼로, Spark의 executor 배치를 그대로 재사용해
"워커가 서로를 찾는 문제"를 라이브러리가 대신 해결해준다 — barrier 로직을
직접 짤 필요가 없고, 기존 spark-submit 경로를 그대로 재사용하므로 새 실행
경로(YARN distributed-shell)도 안 만들어도 된다는 장점이 크다.

기각한 이유는 ADR-0005 때와 동일하게 남아 있다 — 이 프로젝트의 핵심 로직인
exposure offset(`init_score=log(exposure)`), quantile(P10/50/90) 3종,
conformal correction, station_no categorical 인코딩을 `train_common.py`의
순수 LightGBM Python API(`lgb.Dataset`/`lgb.train`) 대신 SynapseML의 Spark ML
Estimator API로 전부 다시 구현해야 한다. "워커 오케스트레이션을 덜 짜는 대신
정확성-critical한 학습 로직을 통째로 재작성"하는 트레이드오프라, 이미 검증된
학습 로직을 건드리지 않는 쪽(YARN distributed-shell)이 리스크가 작다고
판단했다.

| | YARN Distributed Shell | SynapseML |
|---|---|---|
| 필요 인스턴스 수 | 비슷함(데이터 양·머신당 메모리로 결정, 라이브러리와 무관) | 비슷함 |
| 워커 발견/동기화 | 새로 구현 필요(barrier) | 라이브러리 내장 |
| 실행 경로 | 새 EMR Step 타입(distributed-shell) | 기존 spark-submit 재사용 |
| 학습 로직(offset/quantile/conformal) | 그대로(이미 구현·검증됨) | 대부분 재작성 필요 |
| conformal correction 정확도 | 근사치(ADR-0005의 기존 한계 유지) | 전체 검증셋 기준 개선 가능 |

## Decision

LightGBM 분산 학습 워커를 **YARN Distributed Shell**로 기동한다. 구체적으로:

- 신규 `ml/training/scripts/yarn_worker_bootstrap.py`가 distributed-shell
  컨테이너의 진입점이 된다: YARN이 컨테이너마다 주는 `CONTAINER_ID`에서 순번을
  얻고(실패 시 UUID로 대체), 자기 private IP + `LGB_LOCAL_LISTEN_PORT`를
  `s3://.../training-runs/{run_id}/workers/{순번}.json`에 기록한 뒤, 같은
  prefix에 `LGB_NUM_MACHINES`개 파일이 다 모일 때까지 폴링한다(`LGB_TIME_OUT`
  안에 못 모으면 실패). 다 모이면 정렬된 순서로 자기 `LGB_MACHINE_RANK`와
  전체 `LGB_MACHINES` 문자열을 계산해 환경변수로 주입한 뒤
  `train_rental_model`/`train_return_model`을 실행한다.
- `train_common.py`의 `NotImplementedError` 가드를 제거하고, ADR-0005가 이미
  설계해둔 `_shard_for_this_machine()`(station_no를 `zlib.crc32`로 머신 수만큼
  분배)을 `lazy_train_dataset.py`의 날짜별 지연 로더와 실제로 연동한다 —
  단, station_no `CategoricalDtype`을 고정하는 `station_categories_for_dates()`
  호출은 절대 샤딩하지 않고 항상 전체 station 목록을 스캔한다(샤딩하면
  머신마다 카테고리 코드가 어긋나 inference가 조용히 깨진다).
- EMR core 노드 수는 단계별로 리사이즈한다 — 피처마트(Spark)는 3노드로
  충분하고 분산 학습만 8노드가 필요하므로, 클러스터를 껐다 켜지 않고
  `ModifyInstanceGroups`로 그 자리에서 늘린다(줄이는 건 실행 중인 작업을 죽일
  위험이 있어 이번 범위에서는 안 함 — 한 사이클 안에서 한 번 8로 늘리면 그
  사이클이 끝날 때까지 유지).

## Consequences

- 긍정적: SSM/SSH 없이, 이미 실전에서 검증된 EMR Step 경로만으로 분산 학습
  워커를 띄울 수 있다. `train_common.py`의 exposure offset/quantile/conformal
  로직을 전혀 건드리지 않아 새 버그 표면이 좁다. ADR-0005가 이미 설계해둔
  샤딩/환경변수 주입 설계를 그대로 완성하는 것이라 재설계 비용이 없다.
- 부정적: barrier(S3 자기등록 + 폴링) 로직이 저장소에 없던 새 코드라 별도
  검증이 필요하다. YARN distributed-shell은 SynapseML만큼 성숙한 실서비스
  검증 이력이 없어, 컨테이너 배치 실패·재시도 시나리오를 직접 다뤄야 한다.
  EMR 리사이즈는 늘리는 것만 지원하므로(줄이지 않음) 피처마트 단계에서 유휴
  노드 비용이 그 사이클 나머지 기간 동안 계속 발생한다.
- 중립적/후속 고려사항: conformal correction이 대표 머신 샤드만으로 근사되는
  ADR-0005의 기존 한계는 이번에도 그대로 남는다. `run_job_flow()`에 EMR 관리형
  정책이 요구하는 태그(`for-use-with-amazon-emr-managed-policies=true`)가
  누락돼 있던 것도 이번에 같이 고친다. 상세 구현은
  [training/DESIGN.md](../ml/training/DESIGN.md) 1-1번 항목 참고.
- **EMR 정리 보장(2026-08 추가 검증)**: 상시 클러스터를 쓰므로(자동 종료 없음,
  `KeepJobFlowAliveWhenNoSteps=True`) "어떤 경우에도 EMR이 삭제돼야 한다"는
  요구사항을 만족하려면 두 겹의 방어가 필요했다. (1) `monthly_retrain.py`의
  `terminate_cluster`는 처음엔 `trigger_rule=ALL_DONE`만 썼는데, Airflow
  3.3.1을 직접 확인해보니 운영자가 DAG Run 전체를 수동으로 "Mark Failed"
  처리하면 `_set_dag_run_terminal_state()`가 아직 실행 안 된 일반 태스크를
  trigger_rule 평가 없이 그냥 SKIPPED로 강제 전환해버려 이 안전망이 무력화됐다
  — `is_teardown=True`인 태스크만 이 강제 skip에서 예외라, setup/teardown
  API(`terminate_cluster.as_teardown(setups=create_cluster_and_evaluate)`)로
  바꿔야 했다. (2) 그래도 Airflow 스케줄러 자체가 죽는 등 더 근본적인 장애는
  어떤 DAG 태스크도 못 구하므로, 그 실행 그래프와 완전히 독립적으로 실제 AWS
  상태를 직접 확인해 정리하는 `emr_orphan_reaper.py`(15분마다, 8시간 초과
  클러스터 강제 종료)를 별도 안전망으로 추가했다.
