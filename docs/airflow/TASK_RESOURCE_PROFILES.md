# Airflow 태스크 자원 계측

Airflow의 모듈 CLI 태스크는 실행 시간뿐 아니라 **자기 process tree의 메모리**, **scheduler 컨테이너 전체 메모리**, **호스트 메모리 압력**과 **workspace 디스크 변화**를 JSON manifest로 남긴다. 이 자료는 LocalExecutor 동시성 및 EC2 크기를 조정하는 근거로 사용한다.

이 계측기는 `/proc`와 cgroup v2를 읽는 Linux 컨테이너용이다. macOS 호스트에서 스크립트를 직접 실행하는 방식은 지원하지 않는다.

## 실행 계약

개발과 운영 Compose는 같은 기본값을 사용한다.

```dotenv
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__PARALLELISM=3
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=2
AIRFLOW_RESOURCE_PROBE_SAMPLE_SECONDS=1
```

운영 기본값이 작은 이유는 하나의 scheduler 컨테이너에서 pandas, PyArrow와 LightGBM subprocess가 동시에 실행되기 때문이다. 측정값을 비교할 때는 executor, 병렬 태스크 수와 표본 주기가 같은지 먼저 확인한다.

## 계측되는 태스크

`airflow/orchestration/task_builder.py`의 `build_module_task()`로 생성된 `BashOperator`가 자동으로 `ops/resource_probe.py`에 감싸진다.

| 모듈 | 대표 작업 |
| --- | --- |
| Collector | 외부 API 수집, replay, compaction |
| Normalizer | 실시간 인구 보정, station master enrichment |
| Nowcaster | 일별 생활인구 nowcast |
| ML inference | realtime 대여·반납 예측 |
| Loader | serving plan, Gold finalize, event publication |
| Rebalance | 긴급도와 재배치 경로 계산 |

다음 작업은 이 계측 범위에 포함되지 않는다.

- 월별 재학습 DAG의 `PythonOperator`: AWS API 제어와 원격 실행을 담당한다.
- 원격 학습 EC2에서 실행되는 평가·학습 프로세스: 원격 머신의 학습 manifest를 확인한다.
- Airflow webserver와 dag processor 자체: task별 wrapper 밖의 서비스 프로세스다.

## Manifest 위치와 식별자

각 task attempt는 별도 파일에 기록된다.

```text
airflow/resource-profiles/
└── <dag_id>/<run_id>/<task_id>/map-<map_index>/try-<try_number>.json
```

metadata에는 `dag_id`, `run_id`, `task_id`, `try_number`, `map_index`, `executor`, `parallelism`, `max_active_tasks_per_dag`가 들어간다. retry나 mapped task의 측정값을 서로 덮어쓰지 않는다.

저장소 전체가 컨테이너의 `/workspace`에 bind mount되므로 manifest는 호스트에도 남는다. 이 디렉터리는 Git과 Docker build context에서는 제외된다.

## 기록 방식

`resource_probe.py`는 대상 명령을 별도 process group으로 시작하고 기본 1초마다 다음 값을 갱신한다.

- root process와 모든 재귀 자식의 RSS, PSS, swap
- scheduler cgroup의 memory·swap과 OOM event
- Linux 전체 memory·swap 사용량
- `/proc/pressure/memory`의 PSI
- `/workspace` filesystem의 가용 공간
- 시작·종료 시각, wall time, exit code와 종료 signal

manifest는 임시 파일을 쓴 뒤 원자적으로 교체된다. 대상 명령이 실패해도 마지막 표본과 종료 상태를 남기며, wrapper가 SIGINT 또는 SIGTERM을 받으면 같은 신호를 자식 process group 전체에 전달한다. 대상 명령의 종료 코드는 그대로 Airflow에 반환된다.

## 먼저 확인할 필드

현재 schema는 `resource-profile-v2`다.

| 필드 | 의미 | 판단 용도 |
| --- | --- | --- |
| `status` | `starting`, `running`, `succeeded`, `failed`, `signaled`, `resource_limit` | 실행 완료 여부 |
| `exit_code` | 대상 명령의 최종 종료 코드 | Airflow task 실패 원인 확인 |
| `wall_time_seconds` | 명령 실행 벽시계 시간 | 처리시간 비교 |
| `sample_count` | 수집한 표본 수 | peak 값 신뢰성 확인 |
| `peak_process_tree_pss_plus_swap_pss_bytes` | 공유 메모리를 비례 배분한 task process tree footprint | 태스크 자체 용량의 우선 비교값 |
| `peak_process_tree_rss_plus_swap_bytes` | 공유 페이지 중복을 포함한 process tree 상한 | 보수적인 task 메모리 상한 |
| `peak_cgroup_memory_plus_swap_bytes` | scheduler 컨테이너 전체 사용량 | 현재 동시성에서 컨테이너 limit 검토 |
| `peak_system_memory_plus_swap_used_bytes` | 호스트 전체 사용량 | EC2 전체 용량 검토 |
| `memory_pressure_peak_avg10` | 측정 중 최대 memory PSI 10초 평균 | 메모리 경합 여부 |
| `memory_pressure_total_stall_delta_us` | 실행 중 누적 memory stall 증가량 | swap·회수로 인한 지연 판단 |
| `filesystems./workspace.peak_consumed_bytes` | 실행 중 workspace 가용 공간의 최대 감소량 | 임시파일·디스크 여유 검토 |

PSS를 읽을 권한이 없거나 커널이 지원하지 않으면 `pss_process_count`가 0이고, `peak_process_tree_pss_plus_swap_pss_bytes`에는 RSS+swap fallback이 기록된다. 이 경우 다른 실행의 PSS 값과 직접 비교하지 않는다.

## 해석 순서

1. `status`, `exit_code`, `termination_signal`로 정상 종료인지 확인한다.
2. metadata의 동시성 계약과 `sample_count`를 확인한다.
3. task 자체 크기는 process-tree PSS 계열로 비교한다.
4. 동일 시각의 다른 task와 page cache를 포함한 운영 peak는 cgroup 값으로 판단한다.
5. 호스트에 다른 컨테이너나 프로세스가 있었다면 system peak는 Airflow만의 사용량으로 해석하지 않는다.
6. PSI stall과 swap 증가가 함께 나타나면 RAM이 남아 보이더라도 메모리 압박으로 처리시간이 늘었는지 확인한다.

표본 방식이므로 1초보다 짧은 순간 peak는 놓칠 수 있다. 더 촘촘한 측정이 필요하면 `AIRFLOW_RESOURCE_PROBE_SAMPLE_SECONDS`를 낮출 수 있지만, 표본 주기가 다른 결과끼리는 peak와 실행시간을 그대로 비교하지 않는다.

## 코드와 테스트 근거

- wrapper 구현: `ops/resource_probe.py`
- Airflow 자동 적용과 manifest 경로: `airflow/orchestration/task_builder.py`
- 개발·운영 동시성 설정: `ops/compose/docker-compose.yml`, `ops/compose/docker-compose.prod.yml`
- 종료 코드·signal·manifest 검증: `ops/tests/test_resource_probe.py`
- Compose 계약 검증: `airflow/tests/test_compose_runtime.py`
- task wrapper 계약 검증: `airflow/tests/test_task_builders.py`
