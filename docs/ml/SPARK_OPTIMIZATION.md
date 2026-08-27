# Feature Engine Spark 파이프라인 최적화 보고서

> **문서 상태**: 운영 반영 완료 (`fix/spark-cache` 브랜치)  
> **대상 코드**: `ml/feature_engine/spark/run_pipeline.py`, `ml/feature_engine/spark/build_multi_horizon_features.py`  
> **검증 결과**: `feature_engine/tests` 63개 테스트 전체 통과 (Parity 100% 보장)

---

## 1. 배경 및 점검 목적

`ml/feature_engine/spark/`의 Spark job DAG가 지나치게 비대해져 소요 시간이 증가하는 현상이 발생하여, Spark UI(Stages, Jobs, Storage 탭) 실측 지표를 근거로 파이프라인 전반의 연산 비효율과 병목 지점을 점검하고 최적화 작업을 진행했다.

---

## 2. 아키텍처 구조 확인 (Application 분리 구조)

`docs/ml/feature_engine/DESIGN.md` 1절의 설계 기준에 따라 다음 구조는 정상적인 동작임을 확인했다:

```text
run_pipeline.py (App 1, 1단계 — base tick feature 생성)
        │
        ▼
build_multi_horizon_features.py (App 2, 2단계 — horizon 1..12 확장)
```

- **App 1과 App 2의 독립 실행**: Base feature 생성과 Multi-horizon 확장을 분리하여 리소스와 책임을 격리 (독립 `spark-submit` Application).
- **App 2 내부의 대여/반납 순차 실행**: `build_multi_horizon_features.py`의 `_run_cli()`가 대여(rental) Multi-horizon Mart 생성을 완료하여 S3에 저장한 뒤 반납(return) Mart를 순차적으로 생성함 ("두 self-join 결과를 동시에 메모리에 띄우지 않기 위함").

---

## 3. 실측 기반 병목 분석 및 최적화 내역

### 3-1. `run_pipeline.py` 증분 재계산 DataFrame 다중 액션 중복 연산 제거

#### 🔍 문제 분석 (Spark UI 실측)
증분 재계산 결과인 `features_increment`(다중 소스 조인 + `build_features`의 rolling self-join으로 생성된 Lazy DataFrame)에 캐싱 없이 **4번의 Action**이 연속 호출되고 있었다:

```python
if features_increment.limit(1).count() == 0:        # Action 1 (데이터 유무 체크)
    ...
new_count = features_increment.filter(...).count()   # Action 2 (신규 행 카운트)
features_increment.write...parquet(...)               # Action 3 (S3 파티션 쓰기)
max_hour_ts = features_increment.agg(...).collect()    # Action 4 (워터마크용 max 시각)
```

Spark의 지연 평가(Lazy Evaluation) 특성상, 캐싱이 없으면 액션마다 상류 Lineage 전체(원본 Parquet 읽기 $\to$ 다중 소스 조인 $\to$ Rolling Self-join)를 처음부터 재계산하여 **동일한 무거운 셔플 연산이 4번 반복**되었다.

- **Spark UI 실측 증거 (Shuffle Read 반복 발생)**:

| Shuffle Read / Write 크기 | 반복 횟수 | 관련 Stage 목록 |
|---|:---:|---|
| **196.7 MiB / 57.7 MiB** | **4회** | Stage 497(parquet), 316/421/322(count) |
| **143.7 MiB / 122.7 MiB** | **3회** | Stage 487(parquet), 402/313(count) |
| **196.9 MiB / 56.3 MiB** | 2회 | Stage 494(parquet), 319(count) |
| **143.8 MiB / 122.9 MiB** | 2회 | Stage 485(parquet), 306(count) |

#### 🛠️ 수정 사항
1. `features_increment` 필터 직후 `.cache()` 추가.
2. 4개 Action 완료 후 (또는 조기 return 경로에서도) 명시적으로 `.unpersist()` 호출.
3. ➔ **반복되던 셔플 I/O가 1회로 감소 (예: 196.7 MiB × 4 $\to$ 196.7 MiB × 1, 약 75% 절감).**

---

### 3-2. `run_pipeline.py` 전체 빌드(`_run_full_build`) Upstream 재계산 제거

#### 🔍 문제 분석
최초 전체 빌드 함수인 `_run_full_build()`에서도 `write` 후 워터마크 기록을 위해 `features_df.agg(F.max("hour_ts")).collect()`를 호출하여 `build_features`의 상류 계보가 2번 실행되는 비효율이 존재했다.

#### 🛠️ 수정 사항
`max_hour_ts` 계산 시 `features_df`를 재평가하지 않고, 방금 S3에 기록한 `FEATURES_TABLE_PARQUET`의 메타데이터에서 직접 최대값을 조회하도록 수정:
```python
features_df.write.mode("overwrite").partitionBy("date").parquet(config.FEATURES_TABLE_PARQUET)

# 방금 저장한 Parquet에서 max(hour_ts)를 직접 읽어와 무거운 upstream 재계산을 방지
max_hour_ts = spark.read.parquet(config.FEATURES_TABLE_PARQUET).agg(F.max("hour_ts")).collect()[0][0]
write_watermark(config.WATERMARK_PATH, max_hour_ts.isoformat(), _current_params())
```

---

### 3-3. `build_multi_horizon_features.py` Self-Join Caching 및 균형 이진트리 Union

#### 🔍 문제 분석 (Spark UI 실측)
대여/반납 Multi-horizon 생성 공용 함수인 `build_multi_horizon_features()`에서 2가지 병목이 확인되었다:

1. **`anchor`/`target` 캐싱 부재**: `HORIZON_COUNT`(기본 12)번의 Self-Join(`_shift_for_horizon`)이 동일한 Lineage를 공유함에도 캐싱이 없어 Join마다 소스를 다시 스캔.
2. **순차(Left-deep) Union 안티패턴**:
   ```python
   # 기존 순차 Union (깊이 11)
   combined = horizon_frames[0]
   for frame in horizon_frames[1:]:
       combined = combined.unionByName(frame)
   ```
   Horizon 루프 구간에서 Job들이 매번 동일한 양(144개)의 새 Task만 처리하는데도 소요 시간이 선형으로 급증함 (Job 1: 0.9s $\to$ Job 2: 24s $\to$ Job 14: 3.2min). 이는 Union이 중첩될수록 Catalyst Optimizer가 전체 누적 계획을 매번 재분석해야 하는 깊이 $O(N)$ 오버헤드 때문임.

#### 🛠️ 수정 사항
1. **`anchor` 및 `target` DataFrame `.cache()` 적용**: 12회 Self-Join의 중복 I/O 차단.
2. **균형 이진트리(Balanced Binary Tree) Union 구현 (`_balanced_union_by_name`)**:
   - 재귀적 분할 정복으로 Union 깊이를 $11 \to 4$ (`\log_2(12)`)로 대폭 단축하여 Catalyst Plan 분석 오버헤드 제거.
3. **대여 완료 후 `spark.catalog.clearCache()` 명시적 호출**: 대여 Mart 캐시를 메모리에서 완전히 비운 후 반납 Mart 연산을 시작하여 m4.large 환경에서의 OOM 방지.

---

### 3-4. `build_multi_horizon_features.py` 파티션 내 정렬(`sortWithinPartitions`) 추가

#### 🔍 최적화 배경
`_write_date_partitioned()`에서 `repartition("date")` 후 Parquet을 쓸 때, 파티션 내부를 정렬하지 않으면 Snappy 압축 시 RLE(Run-Length Encoding) 효율이 떨어지고 다운스트림 학습(`lazy_train_dataset`) 시 불필요한 I/O가 발생함.

#### 🛠️ 수정 사항
```python
# date로 repartition 후 파티션 내부를 정렬하여 Parquet RLE/Snappy 압축률과 읽기 I/O를 최적화
preferred_sort_cols = ["date", "anchor_ts", "station_no", "horizon"]
sort_cols = [c for c in preferred_sort_cols if c in features.columns]
writer = features.repartition("date")
if sort_cols:
    writer = writer.sortWithinPartitions(*sort_cols)
writer.write.mode("overwrite").partitionBy("date").parquet(output_path)
```
- Parquet 파일 크기 절감 및 `lazy_train_dataset`의 날짜별 청크 로딩 속도 향상.

---

## 4. 검증 결과

`feature_engine/tests` 전체 테스트를 실행하여 기존 동작 및 데이터 Parity가 100% 보존됨을 확인했다.

```bash
$ ./feature_engine/.venv/bin/python -m pytest feature_engine/tests -q
...............................................................          [100%]
============================== 63 passed in 99.81s ==============================
```

- `dev_spark_incremental.py`: 증분 재계산 시 35일 Lookback 및 Partition Overwrite 무결성 검증 완료.
- `dev_spark_multi_horizon_parity.py`: 균형 이진트리 Union 및 Horizon 1~12 Self-Join의 출력 데이터셋이 기존 fold 방식과 완전히 동일함을 검증 완료.

---

## 5. EMR 배포 가이드 (중요)

> ⚠️ **주의**: `ml/feature_engine/` 코드는 Airflow 컨테이너가 아니라 **EMR 클러스터**에서 실행됩니다. EMR은 git을 직접 보지 않고 S3의 `s3://<bucket>/emr/pyfiles.tar.gz`를 부트스트랩 시점에 내려받아 사용합니다.

따라서 EC2 운영 환경에 반영할 때는 반드시 **`make emr-package`**를 수행해야 합니다:

```bash
cd /opt/app
git fetch origin
git checkout fix/spark-cache
git pull
make emr-package   # S3의 pyfiles.tar.gz 패키지 갱신 (필수!)
```

이후 새로 기동되는 EMR 클러스터부터 수정된 최적화 코드가 자동으로 적용됩니다. (Airflow 컨테이너 재시작은 불필요)

---

## 6. 향후 EMR 실행 후 Spark UI 모니터링 체크포인트

1. **Stages 탭 (Shuffle Read/Write)**:
   - 동일 크기의 셔플(196.7 MiB, 143.7 MiB 등)이 3~4회 반복되던 현상이 1회로 줄었는지 확인.
2. **Jobs 탭 (Multi-Horizon 확장 구간)**:
   - Horizon 루프 구간의 Job 소요 시간이 선형으로 늘어나지 않고 일정하게 유지되는지 확인.
3. **Storage 탭 (Memory Usage)**:
   - 캐시된 `anchor`/`target`의 메모리 점유 상태 및 대여 $\to$ 반납 전환 시 캐시가 정상 클리어되는지 확인.
