# ADR-0007: 모델 학습은 단일 EC2에서 lazy LightGBM으로 실행한다

- 상태: 채택
- 결정일: 2026-08-24
- 작성자: Data Engineering 2팀
- 대체 대상: ADR-0005
- 대체한 ADR: 없음

## 배경

Feature mart는 기간이 늘수록 입력량이 커져 일회성 EMR Classic 클러스터의 Spark로 생성하지만, 월별 모델 학습은 정해진 rolling window만 사용한다. 학습 데이터를 한 번에 pandas DataFrame으로 올리면 메모리 부족 위험이 있어 날짜 partition 단위로 읽고 해제하는 경계가 필요하다.

기존 Socket 분산 학습 준비 코드는 station 단위 shard를 전제로 했지만, 현재 학습 입력은 날짜별 `lgb.Sequence`와 memory-mapped label·exposure를 사용한다. 두 방식을 연결하는 shard, 워커 orchestration과 검증이 구현되지 않았다.

## 결정

1. 모델 학습은 Terraform의 단일 `aws_instance.train`에서 실행한다.
2. `lazy_train_dataset.py`가 S3 feature mart를 날짜 partition 단위로 읽어 LightGBM `Sequence`와 memmap을 구성한다.
3. 대여·반납 모델은 같은 단일 머신에서 Poisson 모델과 P10·P50·P90 quantile 모델을 순차 학습한다.
4. 대여 Poisson은 `init_score=log(exposure)`를 사용하고, quantile 모델은 exposure offset 없는 Dataset을 별도로 구성한다.
5. 장시간 boosting은 phase·round checkpoint를 S3에 남기며, 입력·profile·parameter·코드 fingerprint가 일치할 때만 재개한다.
6. `LGB_NUM_MACHINES > 1`은 `NotImplementedError`로 차단한다. 실제 shard, 네트워크와 다중 머신 E2E 검증이 추가되기 전까지 분산 학습을 지원한다고 간주하지 않는다.

## 근거

- Rolling window는 장기 이력 증가와 학습 데이터 크기를 분리한다.
- 날짜 단위 lazy loading과 memmap은 전체 feature mart를 한 번에 메모리에 올리지 않게 한다.
- 단일 학습 EC2는 워커 동기화, Socket port와 분산 실패 복구 운영을 추가하지 않아도 된다.
- Poisson·quantile과 conformal 평가를 한 프로세스에서 수행하면 기존 모델·서빙 계약을 유지할 수 있다.
- Checkpoint는 메모리 절감과 별개로 긴 boosting을 처음부터 반복하는 비용을 줄인다.

## 결과

현재 인프라와 학습 경로가 일치하며 단일 머신 실행은 테스트할 수 있다. 학습 규모가 EC2 memory나 허용 시간을 넘어가면 instance type, 학습 window와 sampling profile을 먼저 조정한다.

다중 머신이 필요해지면 lazy dataset shard, 전체 validation·conformal 집계, 워커 네트워크, 동시 실행과 장애 복구를 구현하고 별도 ADR로 결정한다. 환경변수와 LightGBM parameter 전달 코드만 존재하는 상태는 분산 학습 지원으로 보지 않는다.

## 구현 및 검증 근거

- 단일 학습 EC2: `terraform/compute_train.tf`
- 학습 진입점: `ml/training/train_common.py`
- 날짜별 lazy dataset: `ml/training/lazy_train_dataset.py`
- checkpoint: `ml/training/checkpointing.py`
- 학습 orchestration: `airflow/dags/monthly_retrain.py`
- 학습 테스트: `ml/training/tests/`
- [학습 설계](../ml/training/DESIGN.md)
