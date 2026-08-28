# 자동 재학습 파이프라인 (Automated Retraining) 설계 및 운영 보고서

> **문서 상태**: 운영 반영 완료 (`main` 브랜치 기준 최신 상태)  
> **대상 코드**: `airflow/dags/monthly_retrain.py`, `ml/training/monitor_performance.py`, `ml/training/monthly_retrain_check.py`, `ml/feature_engine/spark/build_multi_horizon_features.py`  
> **관련 결정**: `docs/ml/history.md` (Decision 31, 32, 33, 34)

---

## 1. 개요 및 설계 목적

따릉이 대여/반납 수요는 계절 변화, 도시 인프라 확장, 날씨 패턴 변화 등에 따라 지속적으로 드리프트(Data Drift / Concept Drift)가 발생합니다.  
본 자동 재학습 파이프라인(`monthly_retrain`)은 **인간의 수동 개입 없이 매월 자동으로 모델 성능을 평가하고, 성능 저하가 감지되었을 때만 EMR 분산 클러스터를 활용해 모델을 재학습 및 무중단 승격**시키는 End-to-End MLOps 오케스트레이션을 제공합니다.

```text
[Airflow DAG: monthly_retrain (매월 1일 03:00 UTC)]
       │
       ▼
 1. EMR 클러스터 1회 프로비저닝 (8노드 고정)
       │
       ▼
 2. Feature Mart 최신화 (Spark multi-horizon 독립 워터마크)
       │
       ▼
 3. [대여(rental) 사이클]
    ├─ 최근 1개월 평가 (일자별 부분합 S3 캐싱)
    ├─ 성능 저하 판정 (Deviance >= 10% or Coverage Drift >= 15%p)
    └─ (필요 시) YARN 분산 학습 -> 챔피언 포인터 무중단 승격
       │
       ▼
 4. [반납(return) 사이클]
    ├─ 최근 1개월 평가 (일자별 부분합 S3 캐싱)
    ├─ 성능 저하 판정
    └─ (필요 시) YARN 분산 학습 -> 챔피언 포인터 무중단 승격
       │
       ▼
 5. EMR 클러스터 자동 종료 (Zero Idle Cost)
```

---

## 2. 전체 파이프라인 아키텍처 및 생애주기

### 2-1. 단일 EMR 클러스터 생애주기 통합 (Decision 33)
- **기존 문제**: 대여/반납 DAG가 분리되어 있거나 단계별 동적 리사이즈(3노드 -> 8노드) 시, 클러스터 2중 기동 오버헤드(15분) 및 노드 프로비저닝 지연 중 스텝 유실 위험 발생.
- **개선 설계**:
  - `monthly_retrain` 단일 DAG에서 하나의 EMR 클러스터(`m4.large` Master 1대 + Core 8대 고정)를 기동.
  - **대여 사이클 완료 후 반납 사이클을 순차 실행**하여 자원 충돌을 방지하고 클러스터 기동 오버헤드를 1회로 축소.
  - 모든 작업 완료 후 `finally` 블록에서 클러스터를 무조건 안전하게 종료(`terminate_emr_cluster`).

### 2-2. Airflow DAG 태스크 흐름
```text
start_monthly_retrain
  └─ launch_emr_cluster (8 core nodes)
      └─ spark_build_features (1단계 Tick Base Mart 최신화)
          └─ spark_build_multi_horizon_features (2단계 Multi-horizon Mart 최신화)
              └─ monthly_retrain_check_rental (대여 평가/재학습/승격)
                  └─ monthly_retrain_check_return (반납 평가/재학습/승격)
                      └─ terminate_emr_cluster
```

---

## 3. 성능 모니터링 및 재학습 트리거 기준 (Degradation Criteria)

### 3-1. 평가 데이터 윈도우
- **기간**: 기준일(`as_of`) 직전 1개월(`lookback_months = 1`)의 전체 실측 데이터.
- **기준 Horizon**: `horizon = 1`로 고정 평가하여 챔피언 등록 당시 baseline과의 메트릭 연속성을 유지.

### 3-2. 재학습 트리거 규칙 (`decide_retrain`)
챔피언 모델의 등록 당시 테스트셋 메트릭(Baseline)과 최근 1개월 실측 메트릭을 비교하여 아래 조건 중 **하나라도 만족하면 재학습을 트리거**합니다:

| 지표 | 모니터링 기준 | 임계값 (`common_config.py`) | 트리거 조건 |
| :--- | :--- | :--- | :--- |
| **Poisson Deviance** | 모델의 포아송 오차 상대 증가율 | `10%` (`0.10`) | $\frac{\text{Current} - \text{Baseline}}{\text{Baseline}} \ge 0.10$ |
| **P10~P90 Coverage** | Conformal 신뢰구간 커버리지 드리프트 | `15%p` (`0.15`) | $\|\text{Current Coverage} - \text{Baseline Coverage}\| \ge 0.15$ |

> **참고**: Deviance가 이전보다 좋아진 경우(음수 변화)는 정상적인 성능 향상으로 간주하여 재학습을 트리거하지 않습니다.

### 3-3. MLflow Tracking 연동
평가 결과(`deviance_relative_change`, `coverage_drift`, `needs_retrain`, `reasons`)는 MLflow의 `monthly_retrain_monitor` 실험에 매월 실행마다 자동으로 로깅되어 장기 성능 추이를 추적합니다.

---

## 4. 일자별 부분합 캐싱 아키텍처 (Evaluation Shard Caching)

### 4-1. 설계 원리 (Decision 34)
평가 지표인 Poisson Deviance, RMSE, Coverage는 모두 **샘플별 잔차/적중 여부의 합과 총 샘플 수($N$)의 비율로 표현되는 가법적(Additive) 선형성**을 가집니다:

$$\text{Deviance} = \frac{2 \sum \text{deviance\_term}_i}{\sum N}, \quad \text{Coverage} = \frac{\sum \text{coverage\_hit}_i}{\sum N}, \quad \text{RMSE} = \sqrt{\frac{\sum (y_i - \hat{y}_i)^2}{\sum N}}$$

따라서 각 일자별로 부분합 `(n_rows, sum_deviance_term, sum_sq_err, sum_coverage_hits)` 4개 값만 S3에 캐싱해두면, 전체 기간을 결합할 때 단순 합산만으로 수학적으로 100% 동일한 결과를 즉시 산출할 수 있습니다.

### 4-2. 캐시 저장 경로 및 키 구조
```text
s3://<bucket>/models/eval_cache/<model_name>/<archive_prefix>/h<horizon>/<YYYY-MM-DD>.json
```
- 모델 아카이브 경로(`archive_prefix`)와 예측 시계(`horizon`)가 캐시 키에 포함되어, 모델이 승격되면 자동으로 새 캐시 네임스페이스가 생성됩니다.

### 4-3. 불연속 결측 구간 분할 및 배치 캐싱 흐름
```text
[평가 대상 30일 (08-01 ~ 08-30)]
  ├─ S3 캐시 조회 -> 08-02 캐시 존재, 08-01 및 08-03~08-30 결측
  ├─ _group_contiguous_dates() 결측 구간 분할 -> [('08-01', '08-01'), ('08-03', '08-30')]
  ├─ evaluate_recent_performance_shards_by_day()
  │    └─ 각 연속 구간별로 S3 Parquet 1회 배치 I/O 및 배치 추론 실행
  │    └─ 날짜별 groupby로 분할하여 08-01 및 08-03~08-30 각각의 S3 일자별 캐시 저장
  └─ combine_evaluation_shards()로 전체 Shard(캐시 + 신규) 합산하여 최종 평가 완료
```
- **효과**: 30일 전체 miss 시에도 1회의 배치 연산으로 30개 일자별 캐시가 전부 생성되며, 다음 실행 시에는 새로 추가된 1일만 증분 계산하므로 **평가 시간이 90% 이상 단축**되고 이중 집계가 원천 배제됩니다.

---

## 5. EMR 클러스터 및 YARN 분산 오케스트레이션

### 5-1. Master 노드 OOM 방지를 위한 컨테이너 격리 (Decision 32)
- **문제**: EMR Master 노드는 NameNode, ResourceManager 등 필수 데몬으로 인해 가용 메모리가 부족하여 오케스트레이터 프로세스가 메모리 1.5GB만 점유해도 OS OOM-Killer(ExitCode 137)에 의해 강제 종료됨.
- **해결**: 오케스트레이터 프로세스(`monthly_retrain_check.py`) 자체를 Core 노드의 YARN 컨테이너(`-num_containers 1`)로 격리 실행.

### 5-2. Spark 자원 독점 제거 및 순수 YARN DistributedShell 직결
- **기존 문제**: Spark-submit 래퍼로 오케스트레이터를 감쌀 경우 연산을 하지 않아도 Dynamic Allocation이 클러스터 내 수십 개 Executor를 독점하여 내부 분산학습 워커와 심각한 리소스 경합 발생.
- **해결**: Spark-submit을 배제하고 **순수 YARN DistributedShell CLI를 직접 호출**.

```bash
# 오케스트레이터 래퍼 실행 (Core 노드 컨테이너 격리)
yarn org.apache.hadoop.yarn.applications.distributedshell.Client \
  -jar /usr/lib/hadoop-yarn/hadoop-yarn-applications-distributedshell.jar \
  -shell_command "python3 -m training.monthly_retrain_check --model rental" \
  -num_containers 1 \
  -container_memory 7000 \
  -container_vcores 2 \
  -master_memory 1024 \
  -timeout 345600000
```

### 5-3. 워커 노드 자원 분배 및 Barrier 동기화
- **분산 학습 워커 자원**: EMR Core 노드당 정확히 `7,000MB Memory, 2 vCore`를 명시적으로 할당.
- **노드 예약 마진 (`_WRAPPER_NODE_RESERVATION = 3`)**: Outer AM, Outer Worker, Inner AM이 서로 다른 노드에 배치되는 최악의 경우를 고려하여 `core_instance_count - 3`개(5개 노드)의 워커를 안전하게 요청(Barrier 타임아웃 방지).

---

## 6. Feature Mart 워터마크 격리 및 재사용

- **모델별 전용 워터마크 (Decision 34)**:
  - `_multi_horizon_watermark_rental.json`
  - `_multi_horizon_watermark_return.json`
- **신선도 판정 규칙**:
  - 단일 모델 실행 시 공통 워터마크 fallback을 제거하여, 한 모델의 실행이 다른 모델의 피처 생성을 잘못 건너뛰게(Skip) 만드는 현상을 차단.
  - 공통 워터마크(`_multi_horizon_watermark.json`)는 **`rental`과 `return` 두 모델의 전용 워터마크가 둘 다 동일 소스 기준 최신일 때만 갱신**하여 의미를 보존.

---

## 7. 무중단 챔피언 모델 승격 (Zero-Downtime Atomic Promotion)

재학습이 완료되면 신규 아티팩트를 즉시 서빙 환경에 반영하기 위해 **S3 원자적 포인터(Atomic Pointer)** 방식을 사용합니다:

```text
[재학습 완료 아티팩트]
s3://<bucket>/models/archive/dt=2026-08-28_03-00-00/default/
  ├─ model_poisson.txt, model_q10.txt, model_q50.txt, model_q90.txt
  ├─ profile.json, metrics.json, conformal_correction.json
  └─ categorical_categories_station_no.json

        │ (승격 명령: write_champion_pointer)
        ▼
[챔피언 포인터 S3 단일 쓰기]
s3://<bucket>/models/champion/rental.json -> {"archive_prefix": "models/archive/dt=2026-08-28_03-00-00/default"}
```

- **무중단 서빙**: 실시간 추론 엔진(`ml/inference`)은 `rental.json` 포인터를 읽어 해당 아티팩트 디렉토리의 모델을 메모리에 로드합니다.
- **롤백 용이성**: 성능 이슈 발생 시 이전 아카이브 경로를 가리키는 JSON 한 줄만 S3에 덮어쓰면 수 초 내에 즉시 롤백됩니다.

---

## 8. 핵심 성과 및 최적화 요약

| 최적화 영역 | 기존 방식의 문제점 | 개선된 아키텍처 및 성과 |
| :--- | :--- | :--- |
| **클러스터 생애주기** | 대여/반납 DAG 분리로 EMR 2중 기동 (15분 지연) | 단일 EMR 클러스터에서 대여/반납 순차 완주 (기동 시간 50% 단축) |
| **평가 캐싱** | 매월 30일 전체 데이터를 중복 평가 | 일자별 부분합 S3 캐싱으로 월별 평가 시간 90% 단축 |
| **마스터 노드 안정성** | Master 노드 메모리 부족으로 OOM (ExitCode 137) | Core 노드 YARN 컨테이너로 오케스트레이터 격리 실행 (OOM 0건) |
| **자원 경합 제거** | Spark Dynamic Allocation의 Executor 독점 경합 | 순수 YARN DistributedShell 직결로 공평한 컨테이너 할당 |
| **피처 신선도 보장** | 공통 워터마크 오염으로 단일 모델 생성 스킵 버그 | 모델별 전용 워터마크 분리 및 조건부 공통 워터마크 갱신 |
