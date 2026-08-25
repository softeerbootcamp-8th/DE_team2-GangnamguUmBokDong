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

따라서 이 ADR은 운영 가능한 기능을 나타내지 않는다. [ADR-0007](0007-single-machine-lightgbm-training.md)이 한때 단일 머신 lazy LightGBM 학습으로 대체했었고, 그 결정은 다시 [ADR-0008](0008-yarn-distributed-shell-workers.md)로 대체됐다(현재 결정). 남아 있는 `LGB_*` 설정과 `_distributed_params()`는 향후 재검토를 위한 준비 코드일 뿐 지원 계약이 아니다.

## Consequences

- 긍정적: 실제 워커 인프라가 서면 `LGB_MACHINES`에 `host:port` 목록만 채우고
  코드 변경 없이 분산 학습으로 전환할 수 있다. exposure offset·conformal
  보정 등 기존 정확성-critical 로직을 그대로 재사용해 새 버그 표면이 작다.
  기본값이 기존과 동일해 지금 당장 아무 것도 깨지지 않는다(45개 회귀 테스트
  + 실제 `train_target()` 호출로 검증 완료).
- 부정적: split-conformal correction(P10/P90 보정값)이 대표 머신의 검증
  station 샤드만으로 계산되는 근사치가 된다 — 여러 머신의 conformity score를
  모아 전체 검증셋 기준으로 정확히 맞추려면 LightGBM 소켓 프로토콜 밖의 별도
  집계 단계가 필요한데, 지금은 범위 밖으로 남겨뒀다. 설정 표면(환경변수 6개)이
  늘어나 배포 문서화 부담이 생긴다.
- 중립적/후속 고려사항: 워커 인프라(EMR + YARN Distributed Shell)와
  `_shard_for_this_machine()`의 실제 구현 위치(`lazy_train_dataset.py`)는
  [ADR-0008](0008-yarn-distributed-shell-workers.md)에서 확정했다(중간에
  [ADR-0007](0007-single-machine-lightgbm-training.md)이 단일 머신 학습을
  잠시 채택했다가, 다시 ADR-0008로 대체됐다) — 이 문서 작성 시점에는
  `_shard_for_this_machine()`을 `train_common.py` 소유로 적었지만 실제 구현은
  날짜별 지연 로더 쪽에 있다. conformal correction 근사 문제를 언제·어떻게
  해소할지(전체 검증셋 집계 단계 추가 등)는 여전히 미결정이다. 상세 구현은
  [training/DESIGN.md](../ml/training/DESIGN.md) 1-1번 항목, 결정 경위는
  [history.md](../history.md) 17번 항목 참고.

## 관련 자료

- `ml/training/config.py`
- `ml/training/train_common.py`
- `ml/training/lazy_train_dataset.py`
- [ADR-0007](0007-single-machine-lightgbm-training.md)
- [ADR-0008](0008-yarn-distributed-shell-workers.md)
- [학습 설계](../ml/training/DESIGN.md)
- [학습 변경 이력](../ml/history.md)
