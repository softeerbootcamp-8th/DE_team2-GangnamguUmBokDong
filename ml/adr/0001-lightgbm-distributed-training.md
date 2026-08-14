# 1. LightGBM 자체 분산 학습(Socket) 도입

## Status

Accepted

## Context

`training/`은 지금까지 항상 단일 머신 로컬 LightGBM으로 학습해왔다. `make_dataset`은
EMR/Spark로 분산 처리하지만, 학습은 "최근 N개월만 잘라서" 쓰기 때문에 학습
데이터량이 히스토리 길이와 무관하게 고정되고, 그래서 분산 학습 없이도 확장성
문제가 없었다.

이 원칙을 처음 세울 때(과거 EMR/Spark 이전 결정 논의) LightGBM 자체 분산
학습(Socket/MPI)과 SynapseML(LightGBM-on-Spark)도 검토했지만 채택하지
않았다 — EMR 클러스터를 쓴다고 학습이 자동으로 분산되는 게 아니라 별도
인프라/구현 부담이 컸기 때문이다. 다만 그 시점에도 "여러 해로 데이터가
쌓이면(3년치 기준 학습 구간만 최적화해도 ~88GB) 분산 학습이 실제로
정당화되는 규모가 온다"는 전망은 남겨뒀었다.

이제 그 시점이 되어, 실제 워커 인프라(머신 IP/포트, 클러스터)가 준비되기
전에 코드를 먼저 준비해두기로 했다. 검토한 방식은 세 가지였다:

- **LightGBM 자체 분산(Socket/MPI)**: LightGBM 코어 기능이라 안정적이고, 이
  프로젝트의 핵심 로직인 exposure offset(`init_score=log(exposure)`,
  Poisson censoring 보정)을 그대로 재사용할 수 있다. 단점은 EMR과 무관한
  별도 워커 클러스터(포트가 열린 노드들)를 직접 띄우고 관리해야 한다는 것.
- **SynapseML(LightGBM-on-Spark)**: 이미 있는 EMR 클러스터를 그대로 쓸 수
  있다. 단점은 `VectorAssembler`로 피처를 재조립해야 하고, exposure offset
  처리가 표준 API에 없어 직접 우회 구현이 필요해 이 프로젝트에서 가장
  까다로운 부분과 정면충돌한다.
- **Dask-LightGBM**: API가 pandas와 유사해 마이그레이션 부담이 적지만,
  EMR(Spark)과 별개로 Dask 클러스터를 새로 구성해야 해서 인프라가 하나 더
  늘어난다.

## Decision

LightGBM 자체 분산 학습(Socket, `tree_learner="data"`/`"voting"`)을
채택한다. exposure offset 등 이미 검증된 채점 로직(`common/scoring.py`,
`training/train_common.py`)을 그대로 재사용할 수 있어 다른 두 대안보다
리스크가 작기 때문이다.

인프라(워커 IP/포트)가 아직 없으므로 코드만 먼저 준비하고, 기본값은 지금까지와
동일한 단일 머신 학습을 유지한다:

- `training/config.py`에 인프라 토폴로지 값(`LGB_TREE_LEARNER`,
  `LGB_NUM_MACHINES`, `LGB_MACHINE_RANK`, `LGB_MACHINES`,
  `LGB_LOCAL_LISTEN_PORT`, `LGB_TIME_OUT`)을 환경변수로만 추가한다 — 실험
  하이퍼파라미터가 아니라 배포 환경마다 달라지는 값이라 프로필 파일
  (`profiles/*.json`)에는 넣지 않는다. 기본값(`tree_learner="serial"`,
  `num_machines=1`)은 기존 동작과 완전히 동일하다.
- `train_common.py`는 `station_id`를 `zlib.crc32` 해시로 머신 수만큼 나눠
  train/valid를 샤딩한다(`_shard_for_this_machine()`) — LightGBM 소켓 분산은
  전체 데이터를 자동으로 나눠주지 않으므로 각 머신이 자기 몫만 들고
  `lgb.train()`을 호출해야 한다. `hash()` 내장 함수 대신 `zlib.crc32`를 쓰는
  이유는 `PYTHONHASHSEED`가 프로세스마다 달라 머신 간 배정이 어긋날 수
  있기 때문이다.
- Poisson + quantile(P10/50/90) 4개 `lgb.train()` 호출 전부에 분산 파라미터를
  실어 보내고, 모든 머신이 정확히 같은 횟수만큼 동기 호출하도록 유지한다
  (소켓 핸드셰이크가 전 머신 동기 호출을 전제로 함 — 한 머신만 조기 종료하면
  다른 머신이 다음 호출에서 무한 대기한다). 파일 저장/최종 지표 계산만
  `LGB_MACHINE_RANK == 0`(대표 머신)으로 제한한다 — boosting이 끝나면
  gradient가 매 라운드 네트워크로 동기화되어 모든 머신의 booster가 동일하기
  때문이다.

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
- 중립적/후속 고려사항: 분산 코드 경로 자체는 실제 다중 머신 환경 없이는
  End-to-End 검증이 불가능하다(2026-08-13 기준 인프라 미비) — 인프라가 서면
  실제 여러 머신에서 동시에 스크립트를 띄워 검증해야 한다. conformal
  correction 근사 문제를 언제·어떻게 해소할지(전체 검증셋 집계 단계 추가 등)는
  아직 미결정이다. 상세 구현은 [training/DESIGN.md](../training/DESIGN.md)
  1-1번 항목, 결정 경위는 [history.md](../history.md) 17번 항목 참고.
