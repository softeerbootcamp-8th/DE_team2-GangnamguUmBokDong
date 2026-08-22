# Airflow 태스크 자원 계측

개발·배포 Airflow는 같은 LocalExecutor 동시성 계약과 같은 계측 주기를 사용한다.
기본값은 전체 병렬 태스크 3개, DAG당 활성 태스크 2개, 자원 표본 주기 1초다.

```dotenv
AIRFLOW__CORE__PARALLELISM=3
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=2
AIRFLOW_RESOURCE_PROBE_SAMPLE_SECONDS=1
```

값을 바꾼 실행끼리는 전체 peak RAM과 처리시간을 직접 비교하지 않는다. 결과 manifest의
metadata에 executor와 두 동시성 값이 들어가므로 측정 계약을 함께 확인할 수 있다.

## 기록 범위

`orchestration.task_builder.build_module_task`로 만든 collector, normalizer,
nowcaster, inference, loader, rebalance BashOperator를 `ops/resource_probe.py`가
감싼다. 태스크가 만든 재귀 자식 프로세스까지 RSS, PSS, swap PSS를 표본화하고 같은
시점의 scheduler cgroup RAM·swap, WSL/호스트 RAM·swap, memory PSI와 workspace
filesystem 여유를 기록한다.

PythonOperator로 AWS API만 제어하는 monthly retrain task는 이 process-tree 계측
대상이 아니다. 원격 EMR·EC2 학습 자원은 해당 머신의 학습 manifest를 사용한다.

manifest는 다음 경로에 태스크 시도별로 저장된다.

```text
airflow/resource-profiles/<dag_id>/<run_id>/<task_id>/map-<map_index>/try-<try_number>.json
```

이 디렉터리는 Git과 Docker build context에서 제외하지만 `/workspace` bind mount에
남으므로 컨테이너 재생성 뒤에도 호스트에서 확인할 수 있다. 태스크 성공·실패 exit
code를 그대로 반환하고, timeout이나 수동 종료의 SIGTERM도 전체 자식 process group에
전달한 뒤 final manifest를 기록한다.

## 해석

- `peak_process_tree_pss_plus_swap_pss_bytes`: 해당 태스크와 재귀 자식의 우선 비교값
- `peak_process_tree_rss_plus_swap_bytes`: 공유 페이지 중복을 포함한 보수적 상한
- `peak_cgroup_memory_plus_swap_bytes`: 같은 scheduler 컨테이너에서 동시에 실행된
  다른 태스크와 page cache까지 포함한 전체값
- `peak_system_memory_plus_swap_used_bytes`: Docker와 다른 WSL 프로세스까지 포함한 값

따라서 태스크 자체 용량은 process-tree PSS 계열로, 현재 동시성 계약에서 필요한 전체
머신 용량은 cgroup·system 동시 peak로 판단한다.
