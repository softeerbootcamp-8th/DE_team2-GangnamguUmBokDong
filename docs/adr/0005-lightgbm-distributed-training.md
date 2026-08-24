# ADR-0005: LightGBM Socket 분산 학습을 준비한다

- 상태: 대체됨
- 결정일: 2026-08-14
- 작성자: Data Engineering 2팀
- 대체 대상: 없음
- 대체한 ADR: ADR-0007

## 배경

학습 데이터가 커질 경우 단일 머신의 메모리와 실행 시간이 한계가 될 수 있어 LightGBM 자체 Socket 분산 학습, SynapseML과 Dask-LightGBM을 검토했다. 기존 Poisson exposure offset과 quantile 학습 코드를 유지하기 쉬운 LightGBM Socket 방식을 우선 준비하기로 했다.

## 결정

환경변수로 LightGBM topology를 설정하고 Poisson·quantile 학습 parameter에 Socket 분산 옵션을 전달한다. 기본값은 `tree_learner=serial`, `num_machines=1`로 두어 워커 인프라가 없을 때 기존 단일 머신 학습을 유지한다.

## 근거

LightGBM 자체 분산 기능은 기존 `lgb.Dataset`, exposure offset과 채점 계약을 유지할 수 있어 SynapseML보다 코드 변경 범위가 작았다. 워커 주소와 port는 모델 hyperparameter가 아니라 배포 환경 설정이므로 profile 대신 환경변수로 분리했다.

## 결과

`config.py`와 `train_common.py`에 분산 parameter 전달 코드는 추가됐지만, 이후 학습 입력이 날짜 partition 기반 lazy loading으로 전환되면서 기존 station shard 방식이 제거됐다. 현재 `train_target()`은 `LGB_NUM_MACHINES > 1`을 명시적으로 거부하고, 다중 워커 인프라·보안그룹·동시 기동 절차와 E2E 검증도 존재하지 않는다.

따라서 이 ADR은 운영 가능한 기능을 나타내지 않는다. [ADR-0007](0007-single-machine-lightgbm-training.md)이 현재 결정인 단일 머신 lazy LightGBM 학습으로 대체한다. 남아 있는 `LGB_*` 설정과 `_distributed_params()`는 향후 재검토를 위한 준비 코드일 뿐 지원 계약이 아니다.

## 관련 자료

- `ml/training/config.py`
- `ml/training/train_common.py`
- `ml/training/lazy_train_dataset.py`
- [ADR-0007](0007-single-machine-lightgbm-training.md)
- [학습 설계](../ml/training/DESIGN.md)
- [학습 변경 이력](../ml/history.md)
