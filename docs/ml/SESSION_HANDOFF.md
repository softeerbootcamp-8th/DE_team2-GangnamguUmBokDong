# ML 작업 인계 가이드

> 현재 상태: **과거 세션 메모를 대체한 저장소 기준 안내서**
>
> 이 문서는 작업 완료 여부를 보증하지 않는다. 새 작업은 반드시 코드·테스트·실행
> 산출물을 다시 확인한 뒤 이어간다.

## 현재 구성

ML 운영 코드는 독립된 uv 프로젝트와 공용 라이브러리로 나뉜다.

| 경로 | 책임 | 기준 문서 |
|---|---|---|
| `ml/feature_engine/` | Spark 학습 feature mart 생성 | [Feature Engine 설계](feature_engine/DESIGN.md) |
| `ml/training/` | LightGBM 학습, checkpoint, archive, 재학습 판단 | [Training 설계](training/DESIGN.md) |
| `ml/inference/` | 실시간 feature 조립, pinned model 채점, immutable 게시 | [Inference 설계](inference/DESIGN.md) |
| `libs/ml_core/` | 세 프로젝트가 공유하는 schema·feature·채점 계약 | `libs/ml_core/README.md` |
| `loader/evaluation/` | point-in-time 재배치 정책 백테스트 | [재배치 백테스트](REBALANCING_BACKTEST.md) |

과거 문서가 말하던 `ml/common/`, 저장소 공용 `.venv`, `.venv-spark` 구조는 현재
구조가 아니다. 각 프로젝트의 `pyproject.toml`, `uv.lock`, `.venv`를 사용한다.

## 먼저 확인할 것

1. `git status --short`로 기존 사용자 변경을 확인한다.
2. 대상 문서가 가리키는 진입점·설정·테스트가 실제로 존재하는지 `rg`로 찾는다.
3. 설계 설명은 구현과 테스트가 보장하는 범위까지만 작성한다.
4. 실데이터 성능·메모리·소요 시간은 해당 실행의 고정 입력과 산출물이 있을 때만
   검증 결과로 인용한다.

과거 의사결정의 상세 기록은 [history.md](history.md)에 있지만, 현행 사양의 기준은
아니다. 현재 동작은 코드, 테스트, 각 구성요소의 `README.md`와 `DESIGN.md`를 우선한다.

## 검증 명령

저장소 전체 기본 검증은 루트 Makefile이 관리한다.

```bash
make sync-all
make test
```

작업 범위가 한 프로젝트라면 해당 uv 환경에서 먼저 좁게 검증한다.

```bash
uv run --project ml/feature_engine --frozen pytest ml/feature_engine/tests -q
uv run --project ml/training --frozen pytest ml/training/tests -q
uv run --project ml/inference --frozen pytest ml/inference/tests -q
uv run --project libs/ml_core --frozen pytest libs/ml_core/tests -q
```

Spark 교차 parity 테스트는 의존성이 있는 Feature Engine 환경에서 실행한다. Airflow
테스트는 Compose의 `airflow-scheduler` 환경을 사용하는 `make test` 절차를 따른다.

## 운영 계약 요약

- 학습 feature와 serving feature의 schema·순서·dtype 기준은
  `libs/ml_core/model_contract.py`다.
- 학습 결과는 immutable challenger archive이며, 학습 성공만으로 운영 모델이
  바뀌지 않는다.
- 운영 추론은 `models/serving-release/current.json`이 가리키는 rental/return pair와
  dependency checksum을 한 실행 동안 고정한다.
- 운영 tick은 KST 5분 cadence이며, 대여·반납 예측은 horizon 1..12를 함께 게시한다.
- 재학습 판단은 저장된 test baseline과 최근 완결 월을 비교하지만, serving release
  교체에는 별도의 pair 검증과 pointer 게시가 필요하다.
- 대규모 학습과 백테스트의 원천 데이터·모델·결과는 Git에 없을 수 있다. 파일 부재를
  성공 이력이나 현재 성능으로 추정하지 않는다.

## 인계 시 남길 증거

다음 작업자에게는 세션 대화 대신 아래 네 가지를 남긴다.

- 변경한 파일과 변경 이유
- 대조한 구현 진입점과 핵심 계약
- 실행한 정확한 검증 명령과 결과
- 실행하지 못한 검증, 필요한 외부 데이터·서비스, 남은 위험

이 형식을 따르면 일시적인 로컬 경로, 오래된 테스트 개수, 예정 작업이 현행 사양처럼
남는 문제를 피할 수 있다.
