# training — 실행 방법

`feature_engineering`이 만든 feature 테이블(`station_hour_features_2025.parquet`)을 읽어
대여/반납 LightGBM 모델(Poisson+exposure, quantile P10/50/90)을 학습하고,
`training/models/`에 아티팩트를 저장한다.

설계 배경과 각 파일의 상세 로직은 [DESIGN.md](DESIGN.md) 참고.

## 세팅

```bash
cd ml/training
uv sync   # pyproject.toml/uv.lock 기준 .venv 생성 — lightgbm/pandas/numpy + ml_common(editable) 포함
brew install libomp   # macOS에서 LightGBM 실행에 필요
```

`feature_engineering`이 먼저 `station_hour_features_2025.parquet`을 만들어둬야 한다
([feature_engineering/README.md](../feature_engineering/README.md)).

## 학습 실행

```bash
cd ml
./training/.venv/bin/python -m training.train_rental_model   # 대여 모델 -> training/models/rental_*.txt
./training/.venv/bin/python -m training.train_return_model   # 반납 모델 -> training/models/return_*.txt
```

각 명령은 학습 후 poisson deviance/rmse/pinball/커버리지 지표를 출력하고
`training/models/{model_name}_metrics.json`에 저장한다 — 이 값이 다음 달
`monthly_retrain_check.py`가 비교할 baseline이 된다.

## 산출물 (`training/models/`)

| 파일 | 내용 |
|---|---|
| `{rental,return}_poisson.txt` | Poisson booster |
| `{rental,return}_q{10,50,90}.txt` | quantile booster 3개씩 |
| `{rental,return}_station_categories.json` | 학습 시 고정한 station_id 카테고리(순서 포함) — `inference`가 그대로 로드해야 함 |
| `{rental,return}_conformal_correction.json` | split-conformal 보정값 |
| `{rental,return}_metrics.json` | 테스트셋 평가 지표(다음 모니터링의 baseline) |

로컬 개발 시 이 경로가 기본값이고, 실제 배포에서는 `MODELS_ROOT` 환경변수로
override할 수 있다(`inference`도 같은 값을 봐야 하므로 `ml_common.paths`가
두 폴더 공통 기본 경로를 정의 — [ml_common README](../../libs/ml_common/README.md)).

## 하이퍼파라미터 스윕 / 실험 (legacy)

**실제 서비스 운영에 필요한 코드가 아니라 파라미터 튜닝 때 한시적으로 쓰는
분석 도구라 `training/legacy/`로 옮겼다** — 분류 근거는
[../LEGACY_AUDIT.md](../LEGACY_AUDIT.md) 참고. 여전히 그대로 실행 가능하다
(pandas 2차정제 `feature_engineering/legacy/`에 의존하므로 그쪽도 같이 있어야 함):

```bash
./training/.venv/bin/python -m training.legacy.scripts.run_embargo_sweep         # embargo 후보 4개 스윕 (~2시간)
./training/.venv/bin/python -m training.legacy.scripts.build_embargo_candidate --embargo 60 --phase featuremart
./training/.venv/bin/python -m training.legacy.scripts.build_embargo_candidate --embargo 60 --phase train
./training/.venv/bin/python -m training.legacy.scripts.compare_baselines         # naive/seasonal-naive/historical-average/Poisson GLM 대비 비교
```

모든 실험은 `training/models/experiments/{run_id}/`, `data/processed_v2/experiments/`
아래에 챔피언 산출물과 분리해서 저장한다 — 실험이 실패해도 챔피언 아티팩트는 안전하다.
실행 기록은 `training/models/experiments/manifest.jsonl`에 누적된다(`training/legacy/experiment_log.py`).

## 월별 성능 모니터링 / 재학습 트리거

```bash
./training/.venv/bin/python -m training.scripts.monthly_retrain_check              # 점검만 (dry-run)
./training/.venv/bin/python -m training.scripts.monthly_retrain_check --execute    # 기준 미달 시 실제 피처마트 재생성 + 재학습
```

기본은 리포트만 찍는 dry-run이다. `--execute`는 `feature_engineering/spark`의 증분
파이프라인(`feature_engineering/.venv` subprocess)을 실행한 뒤 재학습까지 트리거하고,
챔피언 모델 파일을 그 자리에서 덮어쓴다 — 승격 전 비교가 필요하면 실행 전에
`training/models/`를 백업해둘 것.

## 실험용 노트북 / 청크 학습 (git에 안 올림)

`training/experiments/`에 프로덕션 코드를 전혀 건드리지 않는 실험들이 있다(전부
`training.train_common`/`ml_common`에서 읽기 전용으로 값만 가져다 씀) — 로컬
산출물이라 저장소 `.gitignore`의 `experiments/` 규칙대로 **git에 올리지
않는다**(로컬 디스크에는 그대로 있고, 필요하면 그대로 실행 가능):

- `tick_model_ooc/` — LightGBM 청크 이어학습(근사, `tick_model_walkthrough.ipynb`로
  단계별 실행 가능, Jupyter 커널 `gangnamgu-ml`)
- `tick_model_sampled/` — 월별 표본 추출 후 `train_common.train_target()` 그대로 호출(근사 아님)
- `multi_horizon/` — "horizon을 feature로" 방식의 12시간 앞 배치 예측 실험(history.md 18번)

이전엔 `tick_model_ooc`/`tick_model_sampled`만 `.gitignore` 규칙이 생기기 전에
커밋돼 있던 예외였는데, 이번에 `git rm --cached`로 추적을 끊어 `multi_horizon/`과
같은 상태로 맞췄다 — 이제 실험 폴더는 셋 다 로컬에만 있다. 자세한 내용은
[../LEGACY_AUDIT.md](../LEGACY_AUDIT.md) 참고.

## 검증

```bash
cd ml
./training/.venv/bin/python -m pytest training/tests/ ../libs/ml_common/tests -q
```
