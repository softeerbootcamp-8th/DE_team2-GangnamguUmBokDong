# ML 의사결정 히스토리

> **문서 상태: 역사 기록·비권위 문서**
>
> 이 파일은 초기 pandas 파이프라인부터 uv 프로젝트와 `libs/ml_core` 분리까지의
> 결정 당시 상태를 보존한다. 과거 경로, 환경, 테스트 개수, 성능 수치와 “현재”,
> “미구현”, “다음 단계” 표현은 오늘의 저장소 상태를 뜻하지 않는다.

이 문서는 “당시에 무엇을 왜 결정했는지”를 시간순으로 남긴다. 기록을 현재 구조에
맞춰 소급 수정하면 결정 맥락이 사라지므로 본문은 원문을 유지한다. 특히 `src/`,
`ml/common/`, `.venv-spark`, 삭제된 문서와 실험 폴더 링크는 역사적 문자열이며,
실행 경로나 현행 사양으로 사용하면 안 된다.

## 현재 상태를 확인하는 곳

| 확인 대상 | 현행 기준 |
|---|---|
| 전체 ML 구성과 검증 시작점 | [ML 작업 인계 가이드](SESSION_HANDOFF.md) |
| 학습 데이터와 schema | [데이터 카탈로그](DATA_CATALOG.md) |
| Spark feature mart | [Feature Engine 설계](feature_engine/DESIGN.md) |
| 학습·checkpoint·archive·재학습 | [Training 설계](training/DESIGN.md) |
| 실시간 채점과 publication | [Inference 설계](inference/DESIGN.md), [실시간 Feature](REALTIME_FEATURES.md) |
| MLflow 운영 | [MLflow 설정](MLFLOW_SETUP.md) |
| 재배치 정책 평가 | [재배치 백테스트](REBALANCING_BACKTEST.md) |

현행 동작의 최종 근거는 링크된 문서만으로 끝나지 않는다. 실제 코드와 테스트,
고정 입력이 기록된 실행 산출물을 함께 대조해야 한다. 이 파일의 과거 성능 수치는
현재 모델 품질이나 운영 성능의 근거로 재사용하지 않는다.

## 기록 범위

- 1~13: point-in-time feature, 초기 Spark 이전, 증분 처리와 로컬 자원 실험
- 14~18: profile, 5분 grid, 분산 학습 준비와 multi-horizon 실험
- 19~24: 레거시 문서 정리와 초기 실시간 추론 최적화
- 25~30: 배포 코드 정리, 경로 통합, uv 전환과 `libs/ml_core` 분리

번호 13 뒤에 16이 먼저 기록되고 이후 14~15가 나오는 순서는 원문의 작성 순서를
보존한 것이다. 번호를 현재 작업 우선순위나 구현 완성도로 해석하지 않는다.

---

## 1. 대여(rental) point-in-time censoring — 학습·추론 파이프라인 적용

**문제**: 대여는 반납이 완료돼야 로그에 잡힌다(대여 시작 시점엔 안 잡힘). 그래서
예측 시점 T에서 "직전 1시간 대여량" 같은 피처를 raw(완전한) 값으로 만들면, 학습
데이터(몇 달~몇 년 지나 전부 반납 완료)와 실제 서빙 시점(방금 지난 1시간 중
4~8%만 로그에 보임)의 분포가 어긋난다(train-serving skew).

**이미 만들어져 있던 것**: `src/rolling_window_features.py`/`src/build_rolling_rental_features.py`가
이 문제를 재현하는 point-in-time 카운트(`[T-90분,T-30분)` 윈도우, window=60분/embargo=30분)를
계산할 수 있게 이미 구현·검증돼 있었으나, 실제 학습(`features.py`)·추론
(`predict_single.py`)에는 연결이 안 돼 있었다.

**결정**: 대여에만 적용(반납은 반납 이벤트 자체가 로그 시점이라 이 문제 자체가 없음).
- **학습**: `src/features.py` `_add_rental_lag_rolling()` — `rental_lag_1h`와
  `roll_mean/std_3h·24h`만 point-in-time censored 값으로 대체, `lag_24h/168h`는
  예측 시점엔 이미 완전히 해소된 값이라 raw 유지.
- **추론**: `src/predict_single.py` — 히스토리 소스를 시간 단위
  집계 대신 트립 단위(start_dt/end_dt) 원본으로 교체, `count_visible_in_window()`로
  실제 censoring 규칙 적용. "윈도우 내 0건"과 "데이터 커버리지 밖"을 구분해서
  후자만 fallback(정류소 평소 패턴)으로 처리.
- 재학습 결과: 대여 모델 deviance 0.959→0.962(+0.3%, 거의 그대로), P10~P90 커버리지
  0.851→0.828(이론값 0.80에 더 가까워짐) — **오차 지표 희생은 거의 없이 skew 제거**.

## 2. 임바고(embargo) 파라미터 스윕

**질문**: embargo=30분이 최선인가?

**한 것**: `scripts/run_embargo_sweep.py` + `src/experiment_log.py`(실행마다
run_id/파라미터/git_sha/지표를 `models/experiments/manifest.jsonl`에 기록)로
embargo 0/15/30/45(나중에 60도 추가)분을 스윕.

**결과**: deviance 0.9598~0.9659(0.6% 범위), 커버리지는 45분에서 가장 좋았다가
60분에서 다시 튐. **결론: 이 데이터 규모에서 embargo 0~60분 차이는 노이즈
수준(재학습만 다시 돌려도 0.3~0.5% 차이 남)이라 실질적 영향이 거의 없음.**
챔피언(embargo=30)을 유지.

## 3. 리소스 프로파일링 (RAM/CPU/시간)

`/usr/bin/time -l`로 embargo=60 후보의 피처마트 생성(rolling+features, ~73초,
peak RSS 7.86GB)과 학습(~25분, peak RSS 8.08GB) 리소스를 측정. **주의점**:
- macOS의 "peak memory footprint"(19.7~31GB)는 압축 메모리 등을 포함하는 계정
  방식이라 실제 물리 사용량보다 부풀려 보임 — "maximum resident set size"(RSS)
  쪽이 신뢰할 수 있는 수치.
- `/usr/bin/time -l`이 재는 건 자식 프로세스(우리 파이썬) 것만이지 시스템 전체가
  아님 — 다른 프로세스 영향 없음.
- EC2 16GB로 이 학습 하나만 돌리는 건 RSS 기준(~8GB)으로는 여유 있어 보이지만,
  macOS↔Linux 메모리 할당자 차이·swap 미설정(EMR은 OOM killer가 즉시 죽임)·
  데이터가 계속 늘어날 것 때문에 "무보증"이라고 결론.

## 4. LightGBM vs 전통적 베이스라인 비교

`scripts/compare_baselines.py`: naive persistence, seasonal naive(1주 전),
historical average(climatology), Poisson GLM(선형), LightGBM 챔피언을 같은
테스트셋(2025-12)에서 비교.

**작업 중 발견한 버그**: 첫 버전에서 historical_average가 `station_hourly_profile.parquet`을
그대로 썼는데, 이게 **12월(테스트 기간)까지 포함한 연간 평균**이라 정답을 미리
보는 leakage였다 — 그래서 historical_average가 LightGBM보다 좋게 나오는 이상한
결과가 나왔음. train 스플릿(1~10월)만으로 다시 집계하도록 고쳐서 재확인.

**결론**: LightGBM이 확실히 최선(deviance 0.962, 다음으로 좋은 historical average
대비 약 35% 낮음). 재미있는 지점: 단순 historical average가 "같은 feature를 쓴
선형 Poisson 회귀"보다 낫다 — 선형모델은 station_id를 평균 1개 숫자로 축약해서
정보 손실이 컸음.

## 5. EMR/Spark 이전 결정

**배경**: `SPARK_SCALING.md`(기존 문서)는 "지금 규모(22.6M행)에서 Spark 불필요,
1순위 후보는 `build_population.py`"라고 결론 냈었지만, 조직 요구사항으로 EMR을
써야 함. 사용자가 범위를 명확히 함:

- **1차 정제**(원본 CSV/parquet → station_master/targets/status/weather/population)는
  다른 곳에서 처리 — 이 저장소는 지금 테스트 편의상만 같이 함.
- **2차 정제(피처마트 생성)와 학습만** EMR Spark로 구성.
- 폴더 구조: `feature-engineering`/`training`/`inference` 분리 예정(지금은
  `feature_engine/`만 만듦, training/inference는 아직 손 안 댐).

**학습 자체는 Spark로 분산할지 여부** — 논의 끝에 로컬 LightGBM 유지로 결정:
- LightGBM은 자체 분산 학습 기능이 있지만(Socket/MPI), 이건 Spark/EMR과 무관하게
  별도 클러스터 운영이 필요함 — "EMR 쓰니까 자동으로 분산"이 아님(EMR=클러스터
  인프라, 분산 실행 여부는 각 스텝이 정함).
- 대안(SynapseML, LightGBM-on-Spark)은 `VectorAssembler`로 피처 재조립·exposure
  offset 우회가 필요해 구현 부담이 큼.
- **채택한 절충안**: 피처는 전체 히스토리 기준으로 Spark가 정확히 계산(10년이든
  얼마든 분산 처리), 학습은 항상 "최근 N개월"만 잘라서 로컬 LightGBM — 학습
  데이터량이 히스토리 길이와 무관하게 고정되어 확장성 문제를 해결.

**Station 생애주기 문제(사용자가 직접 제기)**: 기존 pandas 파이프라인은 "2025년에
트립이 1건이라도 있으면 8,760시간 전부"를 그리드에 넣어서, 폐쇄/휴업 기간까지
"수요 0"으로 잘못 학습하고 있었음. 실측: **26개 station이 30일 이상 일찍 재고
기록이 끝남(심하면 2월 이후로 아예 없음), 6개는 중간에 2주 이상 통째로 끊김.**
사용자 지침: "끊긴 지 얼마 안 됐다고 폐쇄로 단정하지 말고, 그냥 데이터 없는
기간만 학습에서 빼라" → **임계값(threshold) 없이 "station_status에 실제 관측
기록이 있는 시간만 그리드로 쓴다"**로 단순화해서 반영.

## 6. `feature_engine/` PySpark 패키지

[feature_engine/](feature_engine/) — `rolling_window_features.py`(censoring
핵심 로직 포팅), `build_rolling_rental_features.py`, `build_merged_table.py`(위 5번
station 활성구간 반영), `build_features.py`, `run_pipeline.py`.

**구현 중 발견하고 고친 버그(구멍/gap 대응)**: station 활성구간 필터링 때문에
그리드에 결측 시간대가 생길 수 있는데, 처음엔 `rowsBetween`(행 개수 기준) Window로
lag/rolling을 짰다 — 구멍이 있으면 "24시간 전"이 실제로는(구멍 때문에) 27시간
전 값을 잘못 가져오는 조용한 버그가 생김. **`rangeBetween`(실제 경과시간 기준)과
self-join(정확히 N시간 전 행을 찾고, 없으면 null) 방식으로 다시 짜서 해결** —
전용 테스트(`test_feature_engine_build_features.py`)로 검증.

**환경 이슈**: PySpark가 메인 venv의 Python 3.14(너무 최신)를 못 지원해서
`.venv-spark` 별도 venv 필요. 처음엔 3.9로 만들었다가, **"EMR 8.0.0 기본값이
Python 3.11"이라는 사용자 정보에 따라 3.11로 재생성.** `pyspark==3.5.3` 고정.

**검증**: 합성 데이터로 pandas 버전과 Spark 버전이 정확히 같은 값을 내는지 대조
(`test_feature_engine_rolling_parity.py`), 실제 2025년 데이터로 전체 파이프라인
End-to-End 실행 성공(로컬 5분 53초 — **EMR 노드 0개, 순수 로컬 `local[*]` 시뮬레이션**;
클러스터 안 쓰니 당연히 pandas보다 느림, 이건 정상).

## 7. 증분(watermark) 피처마트 생성

**사용자 요구**: 파라미터(embargo 등) 조합마다 모델이 다를 수 있으니 조합별로
피처마트를 따로 두고, 매달 "증분만 이어붙이기"(없으면 전체 생성)가 되게.

**핵심 기술적 난제**: lag_168h(7일) 등 때문에 "이번 달 증분만" 뚝 떼어 계산하면
초반 며칠이 과거를 못 봐서 틀림. **해법**: 워터마크(마지막으로 계산된 시각) -
`INCREMENTAL_LOOKBACK_HOURS`(기본 35일, 7일보다 넉넉한 마진)부터 다시 계산해서,
워터마크보다 최신인 행만 골라 append. rolling_rental_features 자체는 영구
저장 안 하고 lookback 구간만 매번 재계산(가볍고 단순).

**구현**: `feature_engine/watermark.py`
(JSON 파일, read/write), `config.py`의 `PARAM_COMBO_ID`로 파라미터 조합별 출력
경로 분리(`w60_e30_t5` 등), `run_pipeline.py`가 워터마크 유무로 전체/증분 분기.

**검증**: 25일 합성 데이터로 "전체 재계산 결과의 뒷부분"과 "증분 append 결과"가
완전히 일치하는지 대조(`test_feature_engine_incremental.py`) — 통과.

## 8. `common_config.py` — 공통 설정 분리

**문제**: `src/config.py`(pandas)와 `feature_engine/config.py`(Spark)가 같은
파라미터(censoring 윈도우, LAG_HOURS, LightGBM 하이퍼파라미터 등)를 따로
하드코딩하고 있어서, 한쪽만 고치고 잊으면 조용히 갈라지는 위험이 있었음.

**해법**: `common_config.py`(ml/ 루트, pandas/pyspark 등 무거운
의존성 없는 순수 상수 모듈)를 만들어 두 config.py가 여기서 값을 가져오도록
리팩터링. 경로처럼 원래 다를 수밖에 없는 값은 각자 파일에 그대로 둠. 두 패키지가
서로 import하지 않는 원칙(EMR엔 `feature_engine/`만 올라가면 됨)은 유지 —
`common_config.py`는 아주 작은 순수 상수 파일이라 같이 올려도 부담 없음.

## 9. 월별 성능 모니터링 / 재학습 트리거

**구현**: `src/monitor_performance.py` —
`evaluate_recent_performance()`(최근 N개월 실측 vs baseline), `decide_retrain()`
(임계값 적용). `scripts/monthly_retrain_check.py` —
CLI, 기본 dry-run(리포트만), `--execute`로 실제 feature_engine(Spark, 별도
venv로 subprocess) → 재학습까지 트리거. `train_common.train_target()`이 이제
학습 끝날 때마다 `models/{model_name}_metrics.json`을 저장해서 다음 모니터링의
baseline이 됨.

**1차 설계(재검토 필요했음)**: baseline(마지막 학습 시점 테스트 성능) 대비 상대
악화율(deviance 10%, 커버리지 드리프트 15%p)로 판정. 절대 수치 대신 상대 악화율을
쓴 이유는 "계절성 때문에 절대 임계값이 계절과 뒤섞인다"였고, deviance를 RMSE 대신
쓴 이유는 "Poisson deviance는 이론상 규모(rate)와 거의 무관해야 한다"였음.

**실측으로 뒤집힌 부분**: 같은 챔피언 모델(재학습 없이)을 2025년 여러 달로
평가해보니, **deviance가 이론과 달리 계절/수요 규모에 강하게 비례**했다
(1월 0.890 vs 6월 1.259, 약 +42%). 즉 **고정된 12월 baseline 대비로 여름을
평가하면 모델이 멀쩡해도 "재학습 필요"로 오탐**이 날 상황이었음 — 아직
`decide_retrain()`에 반영 전에 발견해서 다행히 배포 전에 잡음.

| 월 | 평균 대여량 | deviance | RMSE |
|---|---|---|---|
| 1월 | 0.844 | 0.890 | 1.005 |
| 3월 | 1.423 | 1.063 | 1.418 |
| 6월 | 2.042 | 1.259 | 1.806 |
| 8월 | 1.730 | 1.193 | 1.598 |
| 9월 | 1.991 | 1.234 | 1.804 |
| 12월(현 baseline) | 0.946 | 0.962 | 1.107 |

**다음에 반영할 방향(아직 미구현)**: 고정 baseline 대비가 아니라 **최근 N개월
이동평균(rolling baseline) 대비**로 바꿔서, 계절이 서서히 바뀌는 건 흡수하고
급격한 이탈만 잡도록 재설계 — 그리고 naive baseline 대비 **skill score**
(`1 - deviance_champion/deviance_naive`)가 계절에 덜 민감한지 실측 검증 진행 중.

## 10. 그리드를 시간 단위 → 5분 tick 단위로 전환 (pandas `src/`)

**동기**: "3시 45분 기준으로 앞으로 1시간" 같은 임의의 5분 단위 기준 시각에서
예측하려면 타겟/그리드 자체가 그 해상도를 가져야 한다 — 정시(0분)에만 값이
있는 타겟으로는 이 예측을 원천적으로 표현할 수 없다. `build_targets.py`가
`future_rolling_counts()`(차분 배열, `censored_rolling_counts()`와 반대 방향 —
과거가 아니라 미래를 봄)로 "[T,T+1시간) 시작 건수"를 5분 tick마다 계산하는
sparse step function으로 바뀌었고, `build_merged_table.py`도 그리드를
station_status 관측 시간을 5분 tick으로 펼친 것으로 바꿨다(폐쇄 구간은
자연히 제외 — 5번 항목의 station 생애주기 원칙 계승). 2025년 실데이터
풀스케일(2.68억 행, RAM 60GB+) 실행/검증 완료.

**다음 단계에서 발견한 버그**: `src/features.py`/`predict_single.py`가 여전히
"1행=1시간"을 가정한 채 `LAG_HOURS`/`ROLLING_WINDOWS`를 **행 개수**로 써서
`.shift(N)`/`.rolling(window=N)`을 하고 있었다 — 그리드가 시간당 12행(5분
tick)이 되면서 `lag_24h`가 실제로는 "2시간 전" 값을, `roll_mean_3h`는 "15분
평균"을 담는 조용한 버그였다. (`feature_engine/build_features.py`가 이미
겪은 "rowsBetween 대신 rangeBetween/self-join" 문제와 같은 종류지만, 그건
station 휴업으로 인한 그리드 구멍 때문이었고 이번엔 tick 밀도 변화까지
겹쳤다 — 5분 tick 자체는 이 시점까지 Spark 쪽에 포팅 안 돼 있었음.)

**수정**: `_exact_hour_lag()`(self-join, 정확히 그 시각의 tick이 없으면 null —
행 개수 기준이 아니라 실제 경과시간 기준)로 lag를, `groupby().rolling("Nh",
on="hour_ts")`(pandas 시간 오프셋 rolling)로 rolling을 다시 구현. **roll_mean/std가
"이전 hourly 지점 N개 평균"인지 "윈도우 안 5분 tick 전부(dense) 평균"인지는
설계 선택지였다** — 사용자가 dense를 선택(더 촘촘하지만 인접 tick끼리 1시간
윈도우가 겹쳐 사실상 스무딩에 가까움, 재학습 후 deviance/coverage 재검증
필요). `predict_single.py`의 실시간 서빙 경로(`_lag_rolling_features`/
`_censored_rental_recent`)도 hourly anchor 대신 동일한 dense anchor로 맞춰서
train-serve skew를 막았다.

**부수적으로 발견한 별도 버그**: 위 수정을 검증하며 5분 간격으로 촘촘하게
sweep하는 새 테스트(`test_censored_rolling_counts_matches_serving_function_sweep_5min_ticks`)를
추가했더니, `censored_rolling_counts()`(배치)와 `count_visible_in_window()`(서빙)가
"트립 시작 시각+embargo가 정확히 tick 배수 위에 있는" 경계에서 서로 다른 값을
냄을 발견 — `lo_t = lo_bound.ceil(tick)`이 "start_ts < T-embargo"라는 **엄격한**
부등호 조건을 tick-정렬된 경계에서 깨뜨리는(그 트립을 한 tick 일찍 카운트)
off-by-one이었다. 실제 트립 타임스탬프는 초 단위라 거의 안 걸렸지만(그래서
기존 hourly 스팟체크 테스트가 못 잡음), 그리드가 5분 tick이 되며 정각 조회가
흔해져 드러났다. `future_rolling_counts()`가 이미 같은 종류의 경계를
`floor(tick)+tick`로 올바르게 처리하고 있었던 걸 그대로 적용해 수정 —
**pandas(`src/`)와 Spark(`feature_engine/`) 양쪽에 동일하게 적용**
(안 그러면 `test_feature_engine_rolling_parity.py`가 두 구현의 새로운
불일치로 실패함 — 실제로 그렇게 돼서 잡아냄).

## 11. 5분 tick 데이터의 실전 메모리 한계 — 청크 처리와 out-of-core 학습 실험

**`build_merged_table.py` 메모리 버그**: `station_id`/`date`가 268M행 전체에서
문자열(str) dtype이라 실측 8.8GB(두 컬럼 합)를 먹고 있었음 — `category` dtype으로
바꿔서 1.1GB로(8배 절감). 소스 DataFrame들(master/weather/population)을 함수
맨 앞에서 한꺼번에 읽어 끝까지 붙들고 있던 것도 문제로 지적받아, 각자 실제로
쓰이는 시점 직전에 읽고 `del`로 바로 놓아주게 고침.

**`lookup_count_at_ticks()` 성능 버그**: `pd.merge_asof(..., by=station_col)`가
268M행 규모에서 30분+ 걸림 — macOS `sample`로 프로파일링해보니 내부
`Int64HashTable`을 한 번에 크기를 안 잡고 점진적으로 resize/rehash하는 데
시간의 90%+를 쓰고 있었다(merge_asof 자체의 구현 문제로 보임). station별
`np.searchsorted`(이진 탐색) 직접 구현으로 대체 — 실측 5M행 5.1초(⇒ 268M행
전체는 수십 초~수 분대로 추정, 기존 30분+에서 극적으로 단축).

**`features.py` 자체가 물리 메모리 한계에 부딪힘**: 위 두 버그를 고친 뒤에도
268M행 전체에 `build_features()`를 한 번에 돌리면(self-join 5번 + rolling 4번)
이 저장소가 개발되는 로컬 머신(RAM 18GB)에서 RSS 10GB대에 macOS가 SIGKILL로
죽였다 — 임계값이 고정돼 있지 않고 그 순간 시스템 전체 메모리 상황에 따라
달라지는 것으로 보임(즉 사용자가 겪은 "경고뜨면서 프로세스 다 멈췄었다"는
현상과 같은 종류). **해법**: `build_features_chunked()`(`src/features.py`) —
station이 25개씩 배치로 나눠(lag/rolling이 전부 station별 독립이라 결과는
그대로 같음) parquet 필터 pushdown으로 배치마다 디스크에서 직접 읽고, 배치별
part 파일로 나눠 쓴다(`ParquetWriter` 하나를 계속 붙들지 않음 + 중단돼도 이미
끝난 배치는 건너뛰고 재시작 가능). 실측 전체 104배치, 17분, peak RSS 9.4GB로
성공(268,449,840행, 병합 테이블과 정확히 일치).

**재학습에서 또 다른 벽**: 학습 세트(1~10월)가 약 2.23억 행 × feature 34개
(전부 float64) ≈ 60GB — `build_features_chunked()`처럼 station별로 쪼개는
전략은 여기선 안 통한다(LightGBM은 station을 아우르는 하나의 통합 모델을
학습해야 해서, 쪼개면 서로 다른 여러 모델이 돼버려 챔피언과 비교 불가).
사용자 판단: 프로덕션 코드(`src/train_common.py`)는 절대 안 건드리고, 별도
실험 경로에 **청크 이어학습**(`lgb.train(..., init_model=이전_booster)`)을
구현. `experiments/tick_model_ooc/chunked_training.py` —
시간순으로 청크(기본 30일)를 하나씩 읽어 이어 학습하고, valid셋 재평가로
청크를 넘나드는 수동 조기종료를 구현. **이건 근사다** — 전체 데이터를 한 번에
보는 "진짜" gradient boosting과 수학적으로 동일하지 않다(각 라운드가 그
청크만 보고 그래디언트를 계산해서 후반부 청크에 recency bias가 생길 수
있음). 그래서 이 실험 지표는 "5분 tick 리팩터가 방향성 있게 도움되는지"
가늠하는 참고용이지 챔피언 승격 기준이 아니다 — 진짜 승격 비교는 더 큰
머신(EMR 등)에서 기존 `train_target()`으로 한 번에 학습해야 함.

**디버깅 중 발견한 실제 데이터 구멍**: 청크 스모크 테스트로 하루 단위까지
잘게 쪼개보다가, **2025-01-09~10 이틀이 `station_status`에 관측 기록이 통째로
0건**임을 발견(수집 장애로 추정) — 버그가 아니라 실제 소스 데이터의 결측이라,
`iter_chunks()`가 빈 청크를 에러 없이 건너뛰도록 처리.

**실습용 노트북**: `experiments/tick_model_ooc/tick_model_walkthrough.ipynb` —
피처마트 생성 확인 → 청크 학습 스모크 테스트 → 본 학습(대여/반납) → 챔피언과
지표 비교 → 추론 스모크 테스트까지 단계별로 직접 실행해볼 수 있게 자세한
주석과 함께 구성.

**8% 표본 학습 결과(참고용, 절대 기준 아님)**: 대여 poisson_deviance 0.9623(챔피언)
vs 1.0046(신규, +4.4% 더 나쁨 — 다만 best_iter=800으로 최대 라운드 도달, 조기종료
안 걸림 — 아직 덜 수렴했을 가능성). 반납은 0.9201(챔피언) vs 0.5447(신규, -40.8%
— 대폭 개선). 5분 tick 밀도가 반납 패턴엔 뚜렷이 도움되고, 대여는 아직 불확실 —
표본 학습이라 결정적이지 않음.

## 13. 컬럼 dtype 최적화 (float64/int64 → float32/int8/int16)

**문제**: feature 테이블 43개 컬럼 중 29개가 float64였다 — 실제 값 범위(예:
hour_sin/cos는 [-1,1], lag/rolling은 0~176, is_holiday/is_weekend는 0/1)에 비해
과한 정밀도였다. 학습 세트(2.23억행) 기준 원시 행렬만 약 66GB — "몇 년치를 한
번에 학습"하려는 목표를 감안하면 이것부터 고쳐야 스케일이 맞음.

**해법**: `src/build_merged_table.py`에 `NATIVE_COLUMN_DTYPES`(값 범위 실측 기반 —
bike_count 0~478라 int16, rental/return_count 0~245라 int16, humidity 13~100이라
int8 등)를 만들어 `df.astype(...)`으로 다운캐스트. `src/features.py`의 순환
인코딩/lag/rolling/exposure 계산도 전부 float32로 명시 캐스트. 값은 그대로,
자료형만 줄인 것 — 학습 세트 원시 행렬 기준 약 66GB → 약 29GB(2.3배 절감).

**부수 효과**: `build_merged_table.py` 재실행 peak RSS는 비슷했지만(다운캐스트가
맨 끝에서만 적용돼 중간 merge 단계는 그대로 float64/int64를 씀), `features.py`
재실행 peak RSS는 9.4GB → 7.7GB로 줄었다 — 다운스트림(피처 생성/학습)이 이
파일을 읽어서 쓰는 단계에서 실제로 체감되는 절감.

**테스트 수정**: `tests/dev_features_rental_censoring.py`의 `assert_series_equal`
3곳이 dtype(float32 vs float64) 불일치로 실패 — 값은 맞고 dtype만 의도적으로
다른 것이라 `check_dtype=False` 추가.

## 16. 폴더 구조 재편 — `src`/`feature_engine`/`scripts` → `common`/`feature_engine`/`training`/`inference`

**배경**: feature_engine(피처마트 생성)/training(학습)/inference(서빙)를 서로 다른
인스턴스에 각각 배포하기로 함(5번 항목에서 이미 예고된 방향). 기존 `src/`
패키지 하나에 세 역할이 다 섞여 있어서(build_*.py, train_*.py, predict_*.py가
전부 같은 패키지) 인스턴스별로 나눠 배포하기 어려웠다.

**최종 구조**: `ml/` 아래 정확히 5개 폴더 — `common/`, `feature_engine/`,
`training/`, `inference/`, `data/`(그대로 유지, 실제 배포 시 S3로 대체).

- **`common/`**: 세 인스턴스가 반드시 같은 값/로직을 써야 하는 것만 모음 —
  `common_config.py`+`profiles/`(프로필 시스템), `paths.py`(신규 — `data/processed_v2/*`
  경로와 `MODELS_DIR`), `rolling_window_features.py`(censoring 핵심 로직,
  feature_engine+inference 공유), `trip_events.py`(신규 — 트립 로딩, 마찬가지로
  feature_engine+inference 공유), `model_contract.py`(신규 — `FEATURE_COLUMNS`,
  station_id 카테고리 저장/로드, training+inference 공유), `metrics.py`(신규 —
  poisson deviance/pinball loss), `scoring.py`(신규, `predict_common.py`에서
  채점 로직만 추출 — inference+training/monitor_performance.py+
  training/scripts/compare_baselines.py가 공유).
- **`feature_engine/`**: pandas 로컬 파이프라인(옛 `src/build_*.py`, `features.py`,
  `grid.py`)과 `spark/` 서브패키지(옛 `feature_engine/` 그대로 이동)를
  같이 둠 — 둘 다 "피처마트 생성"이라는 같은 역할의 서로 다른 실행 환경이라
  한 폴더에 묶는 게 자연스러움.
- **`training/`**: 옛 `src/train_common.py`(공유 로직은 `common/`으로 빠지고
  `_split`/`_prepare_xy`/`_conformal_correction`/`train_target()`만 남음),
  `train_rental_model.py`/`train_return_model.py`, `monitor_performance.py`,
  `experiment_log.py`, 스윕/비교/모니터링 스크립트(`scripts/`), 실험 노트북/청크
  학습(`experiments/`), 모델 아티팩트(`models/` → `training/models/`, `common/paths.py`의
  `MODELS_DIR` 기본값).
- **`inference/`**: 옛 `src/predict_*.py`, `build_station_profile.py`,
  `build_population_profile.py`. `predict_common.py`는 CLI 실행기(`run_predict_cli`)만
  남고 실제 채점은 `common/scoring.py`로 이관.

**설계 포인트**: `common/model_contract.py`의 `LAG_ROLLING_FEATURE_COLUMNS`는
`feature_engine/features.py`(실제 계산 로직)가 아니라 `common_config.LAG_HOURS`/
`ROLLING_WINDOWS`에서 직접 재계산한다 — `feature_engine/features.py`가 오히려 이
값을 `common/`에서 import해서 자기가 만드는 컬럼이 스키마와 어긋나지 않게
맞춘다(의존 방향: `common` → `feature_engine`/`training`/`inference`, 반대가
아님 — 그래야 `common/`만 봐도 전체 모델 계약을 알 수 있고 순환 참조가 없음).
`EXPOSURE_STOCKOUT_VALUE`, `BASE_FEATURE_COLUMNS`도 같은 이유로 `common_config.py`로
옮김(feature_engine와 inference 양쪽이 정확히 같은 값을 써야 함 — 전자는 학습
데이터의 exposure, 후자는 서빙 시점 exposure 계산에 씀).

**모듈-속성 접근 주의사항(실제로 겪은 버그)**: `common/trip_events.py`를 처음
`from .paths import TRAIN_MONTHS`(bound-name import) 형태로 짰더니,
`feature_engine/scripts/validate_completion_curve.py`가 진단용으로
`_config.TRAIN_MONTHS`를 임시로 override하는 기존 패턴이 조용히 안 먹히는
문제가 생겼다(각기 다른 모듈에 바인딩된 별개의 이름이라 한쪽을 바꿔도 다른
쪽엔 안 보임). `from . import paths` 형태로 바꿔 항상 `paths.TRAIN_MONTHS`
attribute 접근을 쓰도록 고치고, override하는 스크립트도 `common.paths`를
직접 가리키도록 수정 — 공유 모듈을 쪼갤 때 "누가 이 값을 몽키패치하고
있었는지"를 놓치기 쉬운 함정이었다.

**검증**: 파일 이동/분할 후 pandas 45개 + Spark 12개 테스트 전부 통과(테스트도
각 폴더의 `tests/` 아래로 재배치, import 경로 전부 갱신). 5개 기능 폴더 각각에
`README.md`(실행 방법) + `DESIGN.md`(설계 배경)를 새로 작성, 루트 `README.md`를
새 구조 기준으로 재작성. 기존 `TRAINING.md`/`INFERENCE.md`는 옛 경로 기준이라
레거시 표시만 추가하고 내용은 유지(단계별 로직 설명은 여전히 유효).

**여러 해로 확장할 때의 스케일**: 지금 밀도(1년 2.68억행)로 3년치를 쌓으면
학습 구간만 최적화 후에도 약 88GB 필요 — 이 정도부터 분산 학습(또는 매우 큰
단일 인스턴스)이 실제로 정당화되는 규모. 1년치만이면 최적화 후 128GB급
인스턴스 하나로 충분히 커버됨(로컬 개발 머신은 여전히 부족 — 그래서
청크/표본 기법이 필요).

## 14. 하이퍼파라미터 프로필 시스템

**요구사항**: `common_config.py`의 파라미터/하이퍼파라미터를 여러 "프로필"로
미리 만들어두고, `ML_PROFILE` 환경변수 하나로 전체 조합을 바꿔 낄 수 있게.
`src/config.py`/`feature_engine/config.py`의 인터페이스(`common_config.XXX`
그대로 참조)는 바뀌지 않아야 함.

**구현**: `ml/profiles/{name}.json`(기본 `default.json`)에 기존 상수 전체(ROLLING_*,
TARGET_HORIZON_MINUTES, LAG_HOURS, LGB_PARAMS_COMMON, 모니터링 임계값 등)를 옮기고,
`common_config.py`를 "프로필 로드 -> 개별 환경변수(`ROLLING_EMBARGO_MINUTES=45` 등)로
그 위에 override" 순서의 로더로 재작성. 우선순위: **개별 환경변수 > 프로필 파일**.
기존 `scripts/run_embargo_sweep.py` 같은 스윕 스크립트는 파라미터를 함수 인자로
직접 넘기지 프로필/환경변수를 안 거치므로 영향 없음 — 그대로 하위 호환.
`profiles/embargo45.json`(embargo만 45분으로 다른 예시)으로 메커니즘 검증,
`.venv/bin/python -m pytest tests/`(45개) 전부 통과 확인.

## 15. Spark(`feature_engine/`) 쪽에 5분 tick 그리드 포팅

**배경**: 10번 항목(pandas `src/`)에서 시간 단위 -> 5분 tick 그리드로 전환했지만
Spark 쪽은 그대로였다. 대칭을 맞추는 작업:

- `feature_engine/rolling_window_features.py`에 `future_rolling_counts()`
  포팅(pandas와 동일한 차분 배열 기법, `Window.rowsBetween(unboundedPreceding,
  currentRow)`로 누적합).
- `feature_engine/build_targets.py` 신규 작성 — `src/build_targets.py`의
  Spark 대응. `_normalize_station_no()`를 이 파일에 두고
  `build_rolling_rental_features.py`가 여기서 import하게 정리(pandas 쪽 구조와
  대칭).
- `feature_engine/build_merged_table.py` 재작성 — station_status를 5분
  tick으로 펼치는 `_expand_hourly_to_ticks()`(crossJoin + 초 단위 offset), 타겟
  조회를 단순 join에서 `lookup_count_at_ticks()` 기반 sparse step function 조회로
  변경(RETURN_TARGETS_PARQUET 추가), `NATIVE_COLUMN_DTYPES`의 Spark 타입 매핑
  (ShortType/ByteType/FloatType)으로 다운캐스트 추가.
- `build_features.py`는 **코드 변경 불필요로 확인**됨 — lag는 self-join(exact
  timestamp 매칭), rolling은 `rangeBetween`(실제 경과초 기준)이라 애초에 tick
  밀도와 무관하게 정확했음.
- **중요한 설계 포인트**: 타겟 parquet(sparse step function)은 증분(`since`)
  실행이어도 **절대 필터링하면 안 됨** — 필터링하면 `since` 이전의 마지막
  delta를 잃어버려 그 직후 tick들의 조회값이 깨진다(그리드/날씨/인구처럼 "그
  시점 이후만 있으면 되는" 소스와 근본적으로 다름). 기존(포팅 전) 코드는 타겟도
  `since`로 필터링했었는데, 그건 그때 타겟이 dense join이라 안전했던 것 — sparse
  전환 후엔 반드시 걸러내면 안 되는 부분으로 바뀌었다.

**테스트**: `test_feature_engine_rolling_parity.py`에 `future_rolling_counts`
pandas/Spark 대조 테스트 추가. `test_feature_engine_incremental.py`는 손으로
만든 dense 타겟 fixture를 실제 `build_targets.build_targets()` 호출로 교체(트립
데이터에서 sparse rental/return 타겟을 직접 생성 — 이 fixture 자체가
build_targets.py의 회귀 테스트 역할도 겸함)하고, 그리드가 시간이 아니라 tick
단위가 됐으므로 행 수 검증 공식에 `TICKS_PER_HOUR` 배수를 반영.
`test_feature_engine_build_features.py`는 그리드 밀도와 무관한 헬퍼라 변경
불필요.

**발견한 중요 버그(이 세션에서 실제로 걸림)**: `feature_engine/build_targets.py`를
처음 짜서 테스트했을 때, parquet에서 읽은 트립(실제 배치 실행과 동일한 경로)으로
계산한 타겟의 tick이 실제 트립 시각보다 정확히 9시간(이 개발 머신의 로컬 타임존인
KST=UTC+9) 밀려 나왔다. 원인: `F.unix_timestamp()`가 `timestamp_ntz`(parquet에서
읽은 값, 실제 배치 경로) 입력에 한해 **세션 타임존을 무시하고 항상 UTC로 해석**하는
반면, `F.timestamp_seconds()`로 되돌릴 때는 **세션 타임존으로 표시값을 만든다** —
이 비대칭 때문에 세션 타임존이 UTC가 아니면 왕복이 그만큼 어긋난다. 기존 테스트들은
전부 `spark.createDataFrame(pandas_df)`로 데이터를 만들어써서(parquet 왕복이 없어서)
이 문제를 우연히 피해가고 있었다 — `censored_rolling_counts()` 같은 기존 함수도
실제로는 같은 취약점을 안고 있었다.

**최초 임시 조치(세션 타임존을 UTC로 고정)로 막았다가, 프로젝트가 타임존을
KST(Asia/Seoul)로 쓰기로 결정하면서 재검토** — 세션/JVM 타임존을 그냥 KST로
바꾸기만 하면 `timestamp_ntz` 왕복이 다시 깨진다는 걸 실측으로 확인함(session
tz=UTC일 때만 이 왕복이 우연히 항등이 됨, KST에서는 여전히 어긋남). **근본
해법**: `feature_engine/rolling_window_features.py`에 `_unix_seconds_ntz()`/
`_seconds_to_ntz()` 헬퍼를 추가 —
- 정방향: 타임스탬프 컬럼을 먼저 `timestamp_ntz`로 명시 캐스트한 뒤
  `F.unix_timestamp()`를 적용해 **세션 타임존과 무관하게 항상 같은 epoch 값**을
  얻는다(입력이 parquet-ntz든 `createDataFrame`-tz-aware든 통일됨 — 단, JVM 기본
  타임존과 세션 타임존이 같아야 함, 아래 참고).
- 역방향: `F.timestamp_seconds()` 대신 `timestampadd(SECOND, 초, TIMESTAMP_NTZ
  '1970-01-01 00:00:00')`(순수 wall-clock 구간 연산, 타임존 변환을 아예 거치지
  않음)로 되돌려서 세션 타임존이 무엇이든(UTC든 KST든) 정확한 왕복을 보장한다.

`censored_rolling_counts()`/`future_rolling_counts()`(둘 다 이 파일)와
`build_merged_table.py`의 `_expand_hourly_to_ticks()`를 이 헬퍼로 재작성.
`lookup_count_at_ticks()`도 union 전에 tick 컬럼을 양쪽 다 `timestamp_ntz`로
캐스트하도록 방어적으로 보강(원래 `unix_timestamp`를 안 써서 버그는 없었지만, 타입이
다른 두 입력을 묵시적으로 union하면 타입 승격 과정에서 같은 종류 문제가 재발할 수
있어 방지).

이 수정 덕분에 세션/JVM 타임존을 **KST로 최종 확정**했다 —
`feature_engine/spark_session.py`와 세 feature_engine 테스트 fixture
전부 `os.environ.setdefault("TZ", "Asia/Seoul")`(JVM 기본 타임존, SparkSession
생성 **전에** 설정 — JVM은 뜬 뒤엔 못 바꿈) + `.config("spark.sql.session.timeZone",
"Asia/Seoul")`로 통일. (TZ와 세션 타임존은 항상 **서로 같은 값**이어야 하고, 그
값 자체는 위 헬퍼 덕분에 UTC든 KST든 상관없이 안전 — 이 프로젝트는 원본 데이터가
한국 로컬 시각이라는 점에 맞춰 KST를 택함.)

**검증**: 세션 타임존을 KST로 바꾼 채로 `.venv-spark/bin/python -m pytest
tests/test_feature_engine_*`(12개) 전부 통과, `.venv/bin/python -m pytest
tests/`(45개, feature_engine 제외) 전부 통과 — 프로필 시스템과 Spark 포팅
둘 다 기존 동작 회귀 없음 확인, 타임존 무관 정확성도 실측으로 재검증.

## 17. LightGBM 자체 분산 학습(Socket) 재도입 — 코드만 먼저, 인프라는 나중

**배경**: 5번 항목에서 "EMR 쓴다고 학습이 자동 분산되는 게 아니다"라는 이유로
LightGBM 자체 분산(Socket/MPI)을 채택하지 않기로 했었지만, 16번 항목 마지막
문단에 적어둔 대로 "3년치(~88GB)부터는 분산 학습이 정당화되는 규모"라는 전망이
있었다. 사용자가 이번에 분산 학습을 실제로 쓰기로 결정 — 다만 워커 인프라(머신
IP/포트, 클러스터)는 아직 없어서 "코드부터 먼저" 준비하기로 함.

**구현**: `training/config.py`에 인프라 토폴로지 값(`LGB_TREE_LEARNER`,
`LGB_NUM_MACHINES`, `LGB_MACHINE_RANK`, `LGB_MACHINES`, `LGB_LOCAL_LISTEN_PORT`,
`LGB_TIME_OUT`)을 환경변수로만 추가 — 프로필 파일(`profiles/*.json`)에 안 넣은
이유는 이 값들이 실험 하이퍼파라미터가 아니라 배포 환경마다 달라지는 인프라
설정이라 프로필과 성격이 다르기 때문. 기본값(`tree_learner="serial"`,
`num_machines=1`)은 지금까지와 동일한 단일 머신 학습이라 인프라가 서기 전까지
기존 동작을 안 깨뜨린다.

`train_common.py`는 `station_id`를 `zlib.crc32` 해시로 머신 수만큼 나눠
train/valid를 샤딩하고(`_shard_for_this_machine()`), Poisson/quantile 4개
`lgb.train()` 호출 모두에 분산 파라미터를 얹는다. **함정**: 처음에 "대표 머신만
평가/저장하고 나머지는 조기 리턴"으로 짰다가, 그러면 대표 머신이 quantile
학습(`lgb.train()` 3번 더 호출)에서 다른 머신의 소켓 응답을 무한 대기하는
문제를 발견 — 분산 학습은 모든 머신이 정확히 같은 횟수만큼 `lgb.train()`을
동기 호출해야 하므로, 조기 리턴 대신 파일 저장/최종 지표 계산 코드만
`LGB_MACHINE_RANK == 0`으로 개별적으로 막는 식으로 고쳤다. `hash()` 내장
함수는 `PYTHONHASHSEED`가 프로세스마다 달라 머신 간 station 배정이 어긋날 수
있어 `zlib.crc32`로 대체.

**알려진 한계(의도적으로 범위 밖)**: split-conformal correction은 대표 머신
몫의 검증 샤드만으로 계산된다 — 전체 검증셋 기준으로 정확히 맞추려면 여러
머신의 conformity score를 모으는 별도 집계 단계가 필요한데, 이건 LightGBM
소켓 프로토콜이 제공하는 기능 밖이라 지금은 안 함.

**검증**: `LGB_NUM_MACHINES=1`(기본값) 기준 `training/tests`(9개) +
전체 45개 회귀 테스트 통과 — 분산 코드 경로 자체는 실제 다중 머신 환경 없이는
End-to-End 검증 불가(2026-08-13 기준 인프라 미비). 다음 세션에서 인프라(워커
IP/포트)가 서면 `LGB_MACHINES`에 실제 값을 넣고 여러 머신에서 같은 스크립트를
동시에 띄워 검증해야 함 — training/DESIGN.md 1-1번 항목,
당시 `adr/0001-lightgbm-distributed-training.md` 참고.

## 18. multi-horizon 실험 — "horizon을 feature로" 방식으로 12시간 앞 배치 예측 구현·검증

**배경**: 실서비스 요구사항 — 전체 대여소에 대해 현재 시점부터 12시간 뒤까지
1시간 단위 12개 구간을 5분마다 다시 예측해야 함. 두 가지를 확인해야 했다:

1. **배치추론(전체 대여소를 한 번에 예측)이 되는가** — 결론: 원래도 됐다.
   LightGBM `predict()`는 애초에 벡터화돼 있어서 station_id가 몇 개든 한
   DataFrame에 담아 한 번에 예측 가능 — 이건 모델 설계가 아니라 엔지니어링
   문제였고, 처음부터 병목이 아니었다.
2. **미래 12개 구간(최대 12시간 뒤)을 예측할 수 있는가** — 결론: **안 됐다.**
   기존 챔피언 모델은 "지금부터 앞으로 딱 1시간"만 예측하도록 학습돼 있어서,
   대여소 수와 무관하게 여러 시간 뒤를 뽑아내는 능력 자체가 없었다.

**검토한 세 방법**(대화 내용 기반):
1. horizon(구간)마다 별도 모델 12개 학습
2. **하나의 모델 + "몇 시간 뒤인지"를 feature로 추가** ← 채택
3. 1시간 예측을 재귀적으로 다음 입력에 먹이기 — **기각**: `rental_lag_1h`처럼
   "바로 직전"을 보는 feature는 2번째 구간부터 미래값이 없어서, 예측값을
   실측치처럼 다시 넣어야 하고 반복할수록 오차가 누적된다.

2번을 택한 핵심 이유: lag/rolling(직전 실적) feature는 **항상 "지금"(T0)
기준으로 계산하고 horizon과 무관하게 절대 안 바꾼다** — 그래서 재귀적 예측과
달리 미래값을 가정할 필요가 전혀 없다. 대신 모델에게 "지금 실적이 이렇고,
N시간 뒤를 묻는다"는 정보를 horizon feature로 직접 알려주고, 몇 시간 뒤일
때 그 실적이 얼마나 신뢰할 만한지는 모델이 학습으로 알아서 배우게 한다.

**핵심 설계 — 데이터를 새로 만들 필요가 없었다**: `feature_engine`이 이미 만든
`station_hour_features_2025.parquet`(5분 tick, lag/rolling·날씨·인구·캘린더·
기존 1시간 타겟 전부 계산돼 있음)를 그대로 재사용했다. "T0 기준 h시간 뒤"
학습 행 하나는 이 테이블의 **두 시점을 조합**한 것뿐이다 — 소스 행(T0)에서
`{rental,return}_lag_*`/`roll_mean/std_*`(지금 아는 것)를, 타겟 행(T0+(h-1)시간)
에서 날씨/인구/캘린더/`rental_exposure`/타겟 카운트(그 미래 시점에 실제로
어땠는지)를 가져와 station×hour_ts self-join(시간 오프셋)으로 조합했다 —
새 타겟 계산이나 트립 원본 재처리가 전혀 필요 없었다. h=1이면 두 시점이
같아져서 **기존 챔피언 모델의 학습 행과 정확히 같아진다**는 걸 실측으로
확인(모든 feature 값이 소수점까지 일치) — 그래서 챔피언과의 비교가
apples-to-apples임을 보장할 수 있었다.

**앵커 해상도**: 원본 테이블은 5분 tick(시간당 12행)이지만, 이 실험은
`minute==0`인 정시 행만 앵커로 썼다 — horizon 정의 자체가 1시간 단위 구간이라
앵커도 1시간 단위면 충분하고, 5분 tick으로 앵커를 잡으면 (tick 밀도 12) ×
(horizon 12) = 144배로 데이터가 불어나 로컬 머신(RAM 18GB)에서 감당 불가.

**실제로 겪은 OOM과 교정**: 처음엔 train만 표본(30%, 6,700만행) 뽑고
valid/test는 전체를 다 쓰기로 했다가, 학습 스크립트가 세 split을 동시에
메모리에 올리면서(합계 1.1억행) **실제로 SIGKILL(OOM)을 겪었다** — parquet
압축 크기(디스크 3.37GB)만 보고 메모리를 과소평가한 게 원인(압축 해제된
float32/int8 배열은 그보다 훨씬 큼). train 8%/valid 25%/test 50%로 세 split
전부 표본을 줄여(합계 약 3,480만행 — 기존에 이미 이 머신에서 검증된 규모인
연간 전체 시간 단위 데이터셋 2,260만행의 약 1.5배) 재시도해서 해결했다.
추가로 학습 스크립트도 `pd.read_parquet(..., columns=[...])`로 필요한 컬럼만
읽게 고쳐서(원래는 전체 컬럼을 읽은 뒤 `df[FEATURE_COLUMNS].copy()`로 다시
골라내 이중 보유가 있었음) 메모리를 더 아꼈다.

**격리 원칙**: `training/train_common.py`를 포함해 기존 프로덕션 코드는 한 줄도
안 건드렸다 — `training/experiments/multi_horizon/`에 `train_target_multi_horizon()`
(train_common.train_target()을 복사해 `horizon` feature만 추가)을 따로 만들고,
`_conformal_correction()`처럼 순수 함수만 원본에서 그대로 import해 재사용했다.

**학습 결과** (`horizon` feature 하나만 추가, LightGBM 파라미터/conformal
보정 등 나머지는 챔피언과 동일 조건):

| | 챔피언(h=1 전용) | multi-horizon(h=1) | multi-horizon(h=12) |
|---|---|---|---|
| 대여 poisson_deviance | 0.970 | 1.017 (+4.9%) | 1.072 (+10.5%) |
| 대여 P10~P90 커버리지 | 0.784 | 0.876 | 0.877 |
| 반납 poisson_deviance | 0.891 | **0.791 (-11.2%)** | 1.041 (+16.8%) |
| 반납 P10~P90 커버리지 | 0.857 | 0.902 | 0.893 |

**해석**: h=1~12 전체에서 horizon이 커질수록 deviance가 완만하게(대여 h=1→12
+5.4%, 반납 h=2→12 +3.4%) 나빠지는, 기대했던 그대로의 자연스러운 저하 곡선을
보였다 — 12시간 뒤도 못 쓸 정도로 나빠지지는 않는다는 뜻. 다만 **h=1만
놓고 챔피언과 비교하면 대여는 오히려 챔피언보다 나쁘고**(학습 표본이 8%뿐이라
데이터량 자체가 챔피언의 일부 — 대여는 데이터량에 더 민감한 것으로 보임),
**반납은 오히려 더 좋다**(챔피언보다 -11%) — 흥미롭게도 8% 표본으로도 반납은
챔피언을 능가했다. 커버리지는 둘 다 챔피언보다 높게(과할 정도로 넓게) 나왔는데
`conformal_correction=0.0`(보정 전 raw 커버리지가 이미 목표 0.80을 넘어서 보정이
아예 안 걸림)이라 — 표본이 작아 quantile booster가 상대적으로 보수적인(넓은)
구간을 낸 것으로 추정된다. **이 지표들은 챔피언 승격 판단 기준이 아니다** —
train 8% 표본이라 전체 데이터로 다시 학습하면 달라질 여지가 크다(참고:
history.md 11번 항목의 "8% 표본 학습" 실험도 비슷한 성격의 표본 규모 한계를
겪었음).

**배치추론 실측**(`batch_inference_demo.py`): 대여소 1,247개(표본 추출된 test
셋 기준) × horizon 12개 = 14,964행을 Poisson+P10+P90 한 번에 추론하는 데 20회
반복 중앙값 **371ms**(min 343ms, max 552ms) — 세션 대화에서 추정했던
"수십~수백ms"를 실측으로 확인. 5분(300,000ms) 주기 갱신에 371ms는 완전히
무시할 수준 — 배치추론은 확실히 병목이 아니다.

**다음에 필요하면**: 전체 데이터(표본 없이)로 재학습해서 h=1 지표가 챔피언과
얼마나 가까워지는지 확인, 그리고 실제 서비스에 편입할지는 이 실험 지표가
아니라 그 재학습 결과로 판단해야 함. 산출물/코드는 전부
`training/experiments/multi_horizon/`(README.md/DESIGN.md 참고)에 격리돼 있어
언제든 다시 실행하거나 폐기해도 챔피언에 영향 없음.

## 19. 레거시 문서 4종 삭제 — `ANALYSIS.md`/`SPARK_SCALING.md`/`TRAINING.md`/`INFERENCE.md`

**배경**: 16번 항목에서 `TRAINING.md`/`INFERENCE.md`는 "레거시 표시만 하고 내용은
유지"하기로 했었지만, 이후 각 폴더의 `DESIGN.md`가 충분히 자리잡으면서 굳이
옛 경로(`src/`, `scripts/`) 기준 문서를 같이 들고 갈 이유가 없어졌다. 삭제 전에
`ANALYSIS.md`/`SPARK_SCALING.md`도 같이 대조해봤다.

**대조 결과**:
- `TRAINING.md`/`INFERENCE.md`: 단계별 로직 설명이 각각 `training/DESIGN.md`,
  `inference/DESIGN.md`와 사실상 전부 겹침 — 순수 삭제.
- `ANALYSIS.md`: §3(feature engineering)/§4(모델 학습, 시간 단위 그리드 시절
  결과)는 10번 항목(tick 전환) 이후로 이미 다 무효화된 수치라 버려도 되지만,
  §2(초기 데이터 감사·사용자 스코프 결정·250m 격자 통합·파이프라인 스키마
  함정·최종 병합 테이블 검증)는 이 프로젝트의 가장 첫 결정들이라 다른 어디에도
  없었다 — `DATA_CATALOG.md`가 이 절들을 번호로 직접 인용하고 있었을 정도.
  **`feature_engine/DESIGN.md` 0번 항목으로 이관**하고 원본은 삭제.
- `SPARK_SCALING.md`: §1~3(feature engineering/학습을 Spark로 옮길지 판단)은
  이미 실행 완료된 질문(`feature_engine/spark/` 존재, 학습 쪽은 17번 항목+
  당시 `adr/0001-lightgbm-distributed-training.md`로 결론) — 버려도 됨.
  **§4.3(Kafka+Spark Streaming 대신 Redis로 충분하다는 재검토)만 유일하게
  다른 곳에 없는 미래 아키텍처 결정**이라 `inference/DESIGN.md` 6번 항목으로
  이관하고 원본은 삭제.
- `REALTIME_FEATURES.md`/`DATA_CATALOG.md`는 삭제하지 않음 — `feature_engine/DESIGN.md`가
  전자를 "자세한 설계"로 명시적으로 링크하는 살아있는 원본이고, 후자는 원본
  데이터 자체를 설명하는 유일한 문서라 다른 DESIGN.md와 안 겹침. 옛 경로
  참조(`src/`, 삭제된 문서 링크)만 갱신.

**검증**: 삭제된 4개 문서를 가리키던 링크를 전부 grep으로 찾아
`DATA_CATALOG.md`/`REALTIME_FEATURES.md`/`README.md`의 참조를 갱신 — 남은 죽은
링크 없음을 재확인.

## 20. `predict_single.py`에 N시간 뒤까지 재귀 예측 추가 — 18번 항목에서 기각한
방식을 알고도 다시 채택

**배경**: "지금부터 N시간 뒤까지 1시간 간격으로 예측"이 필요해졌는데, 사용자가
제안한 방식은 18번 항목에서 이미 검토 후 기각했던 **재귀적 다단계 예측**(예측값을
다음 스텝의 lag 입력으로 다시 먹이기)이었다. 오차 누적 문제를 다시 설명하고
대안(horizon을 feature로 추가하는 방식, `training/experiments/multi_horizon/`에
이미 구현·검증됨)을 제시했으나, 사용자가 "부정확도는 감수하고 일단 빨리
완성 — multi-horizon 반영은 다음 단계에서 고려"라고 결정 → **알고도 재귀
방식을 채택**.

**구현**: `inference/predict_single.py`에 `predict_demand_multi_hour()` 추가.
h=1(바로 다음 시간)은 기존 `_build_feature_row()`를 그대로 써서 지금까지의
단일 시점 예측과 정확히 같은 값을 낸다. h>=2부터 `_recursive_lag_rolling_features()`가
`rental_lag_1h`/`roll_mean·std_3h·24h`/`return_lag_1h`/`roll_mean·std_3h·24h`
(총 10개 — `RECURSIVE_FEATURE_KEYS`)만 재귀적으로 덮어쓴다. `lag_24h`/`168h`
4개는 n_hours가 작으면(12 이하) 항상 실측 구간을 가리키므로 손대지 않았다.

**의도적으로 단순화한 부분(정확도보다 속도 우선)**:
- roll_mean/std_3h·24h를 5분 tick dense 평균이 아니라 1시간 간격 점 표본
  N개의 평균/표준편차로 근사했다 — 재귀 체이닝 자체가 예측값(1개 점)만
  다음 입력으로 쓸 수 있어서, 어차피 tick 단위로 세분화할 수 있는 실측이
  없기 때문. 이 근사 때문에 h=1이라도 만약 h>=2 코드 경로를 탔다면 원래
  단일 시점 예측과 정확히 같은 값이 안 나왔을 것이라, h=1만은 기존 경로를
  그대로 두는 방식으로 우회했다(위 참고).
- 날씨/인구는 호출 시점에 준 값을 n_hours 내내 그대로 재사용한다(예보 API
  미연동은 기존부터 있던 한계, REALTIME_FEATURES.md/기존 predict_single.py
  docstring 참고).

**실제로 잡은 버그**: 처음 구현에서 h==1일 때도 `RECURSIVE_FEATURE_KEYS` 10개를
`fallback_fields`에서 무조건 걸러냈다가(재귀 계산 결과로 다시 채워주는 로직이
h>=2에만 있어서), h=1에서 실제로는 profile fallback을 썼는데도
`lag_fallback_used`/`lag_data_freshness`가 이를 누락해서 보고하는 버그가
있었다 — 값 자체는 정확했지만(어차피 h=1은 기존 경로 그대로라 df 값은 안
건드림) 관측성 지표만 틀렸다. `direct predict_rental_demand()` 결과와
h=1 결과의 `lag_fallback_used`가 완전히 일치하는지 대조해서 발견·수정.

**검증**: 실데이터(`ST-2000`, 2025-06-01)로 n_hours=1이 기존 단일 예측과
소수점까지 정확히 일치함을 확인. 재귀가 실제로 값에 영향을 주는지 h=3 결과를
"그 시각을 직접 단일 예측한 값"과 대조해 서로 다름을 확인(체이닝이 실측 대신
이전 예측을 쓰고 있다는 증거). 데이터 커버리지 밖(2026-08) 날짜로도 크래시 없이
profile fallback으로 넘어가는지 확인. 기존 45개 회귀 테스트 전부 통과(변경 없음).

**다음 단계로 남겨둔 것**: 오차 누적이 실제로 얼마나 되는지는 아직 정량 검증
안 함(재학습 없이 코드만 얹은 상태) — 정확도가 중요해지면
`training/experiments/multi_horizon/`(horizon-as-feature, 이미 h=1~12 검증됨)로
교체하는 게 다음 단계 후보.

## 21. `inference/predict_single.py`의 dtype을 학습 데이터(float32/int8/int16)와 통일

**배경**: 사용자가 "학습용 데이터는 다 float32/int8/int16으로 다운캐스트했는데
추론 코드도 그런가?"라고 물어서 확인해보니, `feature_engine`(pandas+Spark)와
`training`은 전부 다운캐스트된 스키마를 그대로 쓰는데 **`inference/predict_single.py`만
아니었다** — Python 스칼라로 feature 행을 새로 조립하다 보니 기본값인
float64/int64로 들어가고 있었다(배치 조회 CLI `predict_common.py`는 parquet를
그대로 읽어서 문제없음). 처음엔 "값 자체는 안 바뀌니(LightGBM이 내부적으로
캐스팅) 실익 없다"고 답했으나, 사용자가 "학습 스키마와 다른 게 무슨 의미가
있냐, 그냥 통일하는 게 낫다"고 지적 — 이 프로젝트가 이미 다른 곳(station_id
카테고리 인코딩 등)에서 지켜온 "학습/서빙 스키마는 정확히 같아야 한다" 원칙에
비춰보면 맞는 지적이라 반영.

**구현**: `feature_engine/build_merged_table.py`에 있던 `NATIVE_COLUMN_DTYPES`를
`common/model_contract.py`로 이관(두 곳 이상이 정확히 같은 값을 써야 하는 계약
→ `common/`이라는 기존 원칙 그대로 적용). 여기에 `FEATURE_COLUMN_DTYPES`(FEATURE_COLUMNS
전체의 dtype, station_id 제외)를 새로 추가 — lag/rolling 33개 컬럼은
`features.py`가 이미 전부 float32로 만들므로 나머지 카테고리(NATIVE_COLUMN_DTYPES
직접 매칭 / hour_sin 등 4개 cyclical / 그 외는 float32)로 분기해서 구성.
`predict_single.py`의 `_build_feature_row()`가 반환 직전 `df.astype(FEATURE_COLUMN_DTYPES)`로
맞추고, `predict_demand_multi_hour()`의 재귀 override(`df[key] = val`)도
`np.float32(val)`로 대입해 값을 덮어쓸 때 다시 float64로 안 돌아가게 했다.

**검증**: `FEATURES_TABLE_PARQUET`에서 읽은 실제 학습 데이터의 dtype과
`FEATURE_COLUMN_DTYPES`를 컬럼별로 전부 대조해 33개(station_id 제외) 전부
일치 확인. `_build_feature_row()` 출력 dtype이 전부 학습과 같아졌음을 재확인.
기존 45개 회귀 테스트 통과 — h=1 예측값은 dtype 정리 전후로 정확히 동일(같은
경로), h>=2(재귀)는 float32 반올림이 적용되면서 마지막 자리수가 미세하게
바뀜(정상 — 이제 학습 때와 동일한 정밀도로 계산됨).

## 22. 전체 정류소 배치 지원 — 3시간 → 4.5분(전체)/2.4분(5시간) 최적화 4단계

**배경**: 12시간 재귀 예측(20번 항목)을 정류소 하나가 아니라 "전체 정류소를
한 번에, 5분 주기로" 돌려야 하는 요구가 생겼다. 최초 구현(정류소별로
`predict_demand_multi_hour()` 순차 호출)을 20개로 스모크 테스트했더니
정류소당 평균 4.35초 — 전체 2,582개면 약 3시간이라 5분 주기에 전혀 못 맞았다.
"LightGBM 자체는 빠른데 왜 느리냐"는 사용자 질문에 답하며 병목을 하나씩 걷어낸
4단계 최적화 과정을 기록한다.

**1단계 — `_rental_visible_at()`이 anchor마다 전체 재스캔**:
`common/rolling_window_features.count_visible_in_window()`는 "소량의 최근
이벤트 버퍼"를 전제로 설계된 함수인데(docstring에 명시), 이 프로젝트에서는
station당 **연간 전체 트립**을 버퍼로 넘기면서 anchor 시각 하나당 그 전체를
boolean mask로 다시 스캔하고 있었다(`roll_mean_24h` 하나에만 anchor 24~288개
필요). station의 트립을 `start_dt` 기준 한 번만 정렬해두고 anchor마다
`np.searchsorted`로 윈도우 경계만 찾는 방식으로 재작성 — 값은 완전히 동일
(무작위 (station,anchor) 450개 대조 확인)하면서 반복 전체 스캔만 없앴다.
`_censored_rental_recent()`(h=1의 기존 dense 경로)도 같은 함수를 써서 자동으로
같이 빨라짐. 동시에 h>=2에서 어차피 재귀 계산으로 덮어쓸 rental "직전 실적"
5개를 `_build_feature_row(skip_rental_recent=True)`로 처음부터 계산 안 하게
했고, `common/scoring.py`의 `load_boosters()`/`load_conformal_correction()`에
`lru_cache`를 달아 프로세스당 한 번만 디스크에서 읽게 했다.

**2단계 — LightGBM `predict()`가 정류소마다 개별 호출**: 정류소별 순차 루프는
`common.scoring.predict()`(booster.predict() 8번: rental/return x
poisson/q10/q50/q90)를 정류소 수(2,582)만큼 반복 호출한다. `predict_demand_multi_hour_all_stations()`를
"정류소별 순차 호출"에서 "시간(h)마다 전체 정류소를 한 DataFrame으로 모아
`predict()`를 딱 한 번만 호출"하는 구조로 재작성 — feature 조립(`_build_feature_row()`/
`_recursive_lag_rolling_features()`, 검증된 로직 그대로)은 정류소별로 유지하고
모델 채점만 배치로 묶었다.

**3단계(실제로 겪은 회귀 버그) — 정렬 캐시 미비**: 1단계에서 "정렬 한 번만"이라고
했지만 정렬 결과 자체를 캐시하지 않아서 `_rental_visible_at()`을 부를 때마다
(재귀 스텝 하나에도 여러 번) 매번 다시 정렬하고 있었다 — 트립이 많은 정류소일수록
느려지는 이유였다. `_rental_events_sorted_by_station` 전역 dict로 station당
정렬 결과를 캐시하자 **테스트 2개가 깨졌다** — 기존 테스트 fixture는
`_rental_events_by_station` 등을 "save 참조 → 테스트 후 restore 참조" 방식으로
리셋하는데, 이 새 캐시는 (재할당이 아니라) station_id 키로 **in-place mutate**되는
캐시라 참조를 그대로 복원해도 이전 테스트가 채워넣은 항목이 남아있었다(같은
station_id를 테스트마다 다른 합성 트립으로 재사용해서 오염). `dev_predict_single_rental_censoring.py`/
`dev_rental_censoring_cross_parity.py`의 fixture에 "이 캐시만 통째로 새 dict로
비운다"는 예외 처리를 추가해 해결 — 공유 모듈을 쪼갤 때의 monkeypatch 함정
(16번 항목)과 같은 종류지만, 이번엔 "참조 복원으로 안 되는 in-place 캐시"라는
새로운 변종.

**4단계 — station_master의 395개 "유령" 정류소**: `station_master.parquet`(2,977개)에는
2025년에 트립이 없어 `station_hourly_profile`/학습 데이터 자체에 없는 정류소가
395개 섞여 있다(5번 항목의 "2025년 트립 1건 이상"만 활성 station으로 채택한
필터와 같은 이유). 이 395개는 fallback도 없어 매 시간마다 NaN + "Mean of empty
slice" 경고를 냈고(전체 실행 로그가 876KB로 불어남), 계산 시간도 낭비했다.
`predict_demand_multi_hour_all_stations()`의 기본 `station_ids`를
`common.model_contract.load_station_dtype("rental").categories`(모델이 실제로
학습한 2,582개)로 바꿔 이 낭비를 없앴다 — 학습 안 된 station_id는 예측 자체가
의미 없으므로.

**효과 실측**: 정류소 1개·캐시 워밍업 제외 순수 계산(n_hours=12) 2.14초 →
0.264초(1단계 후) → (2~4단계 후 배치 경로 자체 재구성). **전체 2,582개
정류소 x 12시간 CLI 실행**(캐시 워밍업 포함, 실측): 3시간 추정 → **4분 31초**
(267초, `--all-stations --n-hours 12`). 실사용 범위인 n_hours=5만 쓰면 워밍업
(~75초, h=1 스텝에 포함) + 이후 스텝당 ~17.6초 × 4 ≈ **2.4분**으로 5분 주기에
여유 있게 들어간다.

**아직 남은 것(필요해지면)**: 지금도 남은 비용은 대부분 "정류소마다 반복되는
파이썬 레벨 feature 조립"(dict 조회, DataFrame 생성)이지 LightGBM 자체가
아니다 — `batch_inference_demo.py`(18번 항목)에서 이미 확인했듯 모델 채점
자체는 전체 정류소를 합쳐도 수백 ms면 끝난다. 매 사이클 새 프로세스를 띄우면
캐시 워밍업(~75초)이 사이클마다 반복되므로, 상시 실행되는 프로세스/서버로
캐시를 유지하면 그 비용도 없앨 수 있다 — 지금은 CLI 1회성 실행이라 보류.
feature 조립 자체를 정류소 축으로 벡터화(지금처럼 정류소별 루프 후 배치
채점이 아니라, 조립부터 전체 정류소를 한 DataFrame으로)하면 더 줄일 수 있지만
재귀 로직과 얽혀 있어 추가 작업이 필요하다.

**검증**: 벡터화 전후 `_rental_visible_at` 값 완전 일치(무작위 450개 대조),
정렬 캐시 도입 후 회귀됐던 테스트 2개 fixture 수정으로 재통과, 배치 경로
결과가 정류소별 개별 호출 결과와 완전히 일치(mismatch 0), 전체 실행 결과
30,984행(2,582 x 12) 전부 NaN 없이 채워짐, 기존 45개 회귀 테스트 전부 통과.

## 23. 22번 항목 후속 — feature 조립도 배치화(4.5분 → 3.1분) + 실패 스킵/로그 + 재귀 embargo 보정

**배경**: 22번 항목 이후에도 "LightGBM은 빠른데 왜 느리냐"는 질문에 cProfile로
다시 뜯어봤더니, 사용자가 코드를 직접 보고 정확히 짚었다 — **모델 채점만
배치로 묶었지 feature를 만드는 부분(`_build_feature_row()`)은 여전히 정류소
2,582개를 파이썬 for문으로 하나씩 돌고 있었다.** 실측: `.astype()` 호출이
전체 실행 시간(35.5초, 200개 정류소×12시간 기준)의 **절반(17.7초)**을 먹고
있었는데, `_build_feature_row()`가 정류소 1개짜리 DataFrame을 만들 때마다
`FEATURE_COLUMN_DTYPES`(33개 컬럼) 캐스팅을 반복했기 때문(200개×12시간
=2,400번의 개별 캐스팅 대신, 시간(h)당 한 번씩 12번만 해도 되는 일이었음).

**해법 1 — feature 조립을 dict 생성과 DataFrame 캐스팅으로 분리**:
`_build_feature_row()`의 계산 로직을 `_build_feature_record()`(dict만
반환, DataFrame 생성/캐스팅 없음)로 빼고, `_build_feature_row()`는 이 dict를
1행 DataFrame으로 감싸는 얇은 래퍼로 남겼다(단일 정류소 API는 동작 그대로).
`predict_demand_multi_hour_all_stations()`는 이제 정류소마다 `_build_feature_record()`로
dict만 모으고, **시간(h)마다 딱 한 번** `pd.DataFrame(records).astype(...)`으로
전체 배치를 만든다. 효과(200개 정류소×12시간, 캐시 워밍업 제외): 35.5초 →
10.1초. 전체 2,582개 x 12시간 실측: **267초(22번 항목 결과) → 185초**.
실사용 범위인 5시간이면 추정 약 2.3분.

**해법 2 — 정류소별 실패는 재시도 없이 스킵 + 큰 소리로 로그**: 기존엔 정류소
하나라도 `_build_feature_row()`에서 예외가 나면 전체 배치가 그대로 죽었다
(try/except가 아예 없었음). 사용자 요청대로 "스킵은 하되 시끄럽게" —
정류소별 try/except로 감싸 실패하면 `sys.stderr`에 `[함수명] SKIP
station=... h=... — 예외타입: 메시지`를 찍고 그 시간대만 결과에서 뺀다.
재시도는 안 하지만 다음 시간대(h+1)에서는 다시 시도된다(직전 실패를 영구히
물고 가지 않음 — `synthetic[sid]`에 그 시간대 값이 없으면 다음 스텝은 그냥
profile fallback으로 자연스럽게 넘어감). `station_ids`에 존재하지 않는
station_id를 섞어 스모크 테스트로 확인.

**해법 3 — 재귀 lag에 완료율(embargo) 보정 추가**: 20번 항목에서 재귀
예측을 채택할 때 "예측값을 다음 스텝 lag 자리에 그냥 넣는다"고만 했는데,
사용자가 "그 자리는 원래 embargo 적용된 값이 들어가야 하는 거 아니냐"고
정확히 짚었다. 실제로 `rental_count`(학습 타겟)는
`feature_engine/build_targets.py`의 `future_rolling_counts()`가 만드는
"`[T,T+60분)`의 완결된 건수"이고, `rental_lag_1h`는 window=60/embargo=30분
기준 `[T-90분,T-30분)`을 **T 시점까지 반납 완료된 것만** 세는 값이다 — 재귀
스텝은 전자(모델이 예측한 완결 건수)를 후자(부분관측+embargo 적용) 자리에
그대로 넣고 있어서, 그 자체로 과대평가 소스였다(오차 누적과는 별개의 문제).

`_get_rental_completion_ratio()`를 추가해 이 완료율을 실측 데이터로 자동
추정 — `[T-60,T)`(직전 스텝 예측이 가장 가깝게 커버하는 구간)의 완결
건수 대비 그 구간을 `_rental_visible_at()`(현재 config의 window/embargo
그대로)로 봤을 때 실제로 보이는 비율을, 무작위 (station,시각) 300개 표본의
`sum(관측)/sum(완결)`로 계산(표본별 비율의 평균이 아니라 합의 비율 —
REALTIME_FEATURES.md가 이미 경고한 "나눗셈이 작은 값에서 극단으로 튀는" 문제
회피). 재귀 스텝에서 rental prefix의 synthetic 값에만 이 비율을 곱한다
(return은 지연 관측 문제가 없어 보정 안 함). 실측 계수 **0.947** — 문서화된
"width=60/embargo=30 채택 설계 종합 완료율 88.2%"(REALTIME_FEATURES.md)와
같은 범위라 타당성 확인. **알려진 잔여 한계**: `[T-60,T)`와 실제 lag가 보는
`[T-90,T-30)`은 30분 어긋나 있어(재귀 예측이 가진 값이 `[T-60,T)`뿐이라
불가피) 완료율만 보정하고 이 시간축 어긋남은 그대로 남는다.

**검증**: `.astype()` 최적화 전후 `predict_demand_multi_hour()`(단일 정류소)
결과가 20개 정류소 기준 완전히 일치, 스킵 로직은 존재하지 않는 station_id로
스모크 테스트, 완료율 계수는 캐시 워밍 후 0.48초에 계산되고 두 번째 호출부터는
캐시로 즉시 반환. 기존 45개 회귀 테스트 전부 통과. 전체 실행 결과 30,984행
전부 NaN 없이 채워짐(재검증).

## 24. 패러다임 전환 — station-outer/anchor-inner를 anchor-outer/station-vectorized로 뒤집기(185초 → 107초)

**배경**: 23번 항목 이후 사용자가 "feature 처리 부분을 근본적으로 더 빠르게
할 수 없냐, 패러다임을 바꿔도 된다"고 요청. 다시 프로파일링해보니 남은 비용의
대부분(`_rental_visible_at` 관련 28~47%)이 여전히 **정류소마다 트립을
조회하는 구조** 자체에서 나오고 있었다.

**핵심 통찰**: `predict_demand_multi_hour_all_stations()`에서는 정류소
2,582개가 전부 **같은 target_ts를 공유**한다(도시 전체의 "지금"). 그런데
지금까지의 구현은 "정류소를 고정하고 anchor 여러 개를 처리"하는 축으로 짜여
있어서, anchor는 몇 개 안 되는데(최대 25개) 정류소 수만큼 반복하는 게
낭비였다. **축을 뒤집어 anchor를 고정하고 정류소 전체를 한 번에 처리**하면
"정류소가 몇 개든 anchor 개수만큼만 반복"하는 구조가 된다.

**구현**:
- `_rental_visible_batch_all_stations(station_ids, anchors)` 추가 — 전체
  트립을 station 무관하게 start_dt 기준 한 번만 정렬해두고(`_get_rental_events_by_station()`이
  같이 캐시), anchor마다 `searchsorted`로 좁은 구간만 슬라이스한 뒤 그 안에서
  `station_id`로 `value_counts()`해서 **전체 정류소의 카운트를 한 번에** 낸다.
  `_rental_visible_at()`과 값이 완전히 같음을 무작위 (station,anchor) 조합
  3,750개로 확인(2025년 트립이 0건인 "유령" 정류소는 실측과 동일하게 NaN
  처리하도록 별도 체크 추가 — 처음엔 이 체크가 없어서 375개 불일치가 났었음).
- `_rental_recent_batch(station_ids, target_ts, synthetic_rental)` 추가 —
  `_censored_rental_recent()`(h=1, dense)와 `_recursive_lag_rolling_features()`의
  rental 분기(h>=2, 1시간 점표본)를 이 배치 함수 위에서 벡터화 재구현 —
  anchor별 배치 결과를 `pd.DataFrame`(index=station, columns=anchor)으로
  모은 뒤 `synthetic.where(synthetic.notna(), real)`로 "synthetic 우선,
  없으면 실측"을 pandas 연산으로 처리하고, `.mean(axis=1)`/`.std(axis=1)`로
  rolling 통계까지 정류소 전체를 한 번에 계산한다. h=1/h>=2 두 경우 모두
  기존 함수와 값이 완전히 일치함을 대조 검증(각 750개 조합, mismatch 0).
- return은 애초에 트립 재계산이 필요 없어(시간 단위 dict 조회, O(1)) 벡터화
  이득이 적다고 판단, `_recursive_return_features()`(return 전용으로 축소한
  기존 로직의 복사본)를 정류소별로 그대로 유지.
- `predict_demand_multi_hour_all_stations()`를 시간(h)마다 위 두 배치 함수를
  **먼저 한 번씩** 호출하고, 정류소별 루프에서는 그 결과를 `.loc[sid]`로
  꺼내 병합만 하도록 재구성(더 이상 정류소별로 트립을 다시 조회하지 않음).

**실제로 겪은 성능 버그(가장 크게 걸림)**: 처음엔 "이 정류소가 트립이 있는
정류소인지" 체크를 `station_index.isin(sids_arr)`로 짰는데, `sids_arr`가
3,700만 건짜리 Arrow 문자열 배열이라 이 **한 번의 호출이 94.5초**나 걸렸다
(pandas가 큰 배열 쪽을 해시테이블로 만드는데 Arrow-backed object 문자열이라
극단적으로 느림 — cProfile로 `pandas/core/arrays/string_arrow.py:isin`
한 줄에 94.587초가 잡혀서 확인). 이미 station별로 그룹화해서 캐시해둔
`_rental_events_by_station`(dict, 키 조회 O(1))가 있는데 그걸 안 쓰고
매번 새로 큰 배열을 해시테이블화한 게 원인 — `sid in events_by_station`
방식으로 교체해서 해결. 디버깅 중 병렬로 띄운 여러 백그라운드 프로세스가
트립 데이터(연간 3,700만 건)를 프로세스마다 중복 적재하면서 메모리 경합까지
겹쳐 문제를 더 헷갈리게 만들었다 — 한 프로세스씩만 띄워서 격리한 뒤에야
정확한 원인을 잡을 수 있었다.

**효과 실측**: 200개 정류소 x 12시간(캐시 워밍업 제외) 87.6ms/station →
18.6ms/station(약 4.7배 추가 개선). **전체 2,582개 정류소 x 12시간 CLI
실행**(캐시 워밍업 포함, 실측): 185.3초(23번 항목) → **106.8초**. 처음
측정했던 "약 3시간"(20번 항목 시작 시점) 대비 총 약 100배. 39건의 "Mean of
empty slice" 경고(프로필 표본이 아예 없는 극소수 station×hour×dow×month
조합)가 남아있지만 최종 결과엔 NaN이 0건이라 fallback이 정상 작동함을
확인했고, 경고 자체는 이번 세션에서 더 손대지 않았다.

**검증**: 벡터화 함수 2개 모두 기존 per-station 함수와 값 완전 일치(수천
조합 대조), `predict_demand_multi_hour_all_stations()`의 최종 결과가
`predict_demand_multi_hour()`(단일 정류소, 안 건드림) 개별 호출과 20개
정류소 x 12시간 기준 완전히 일치(mismatch 0), 전체 실행 결과 30,984행
전부 NaN 없이 채워짐, 기존 45개 회귀 테스트 전부 통과.

## 25. 실서비스용 저장소 정리 — pandas 2차정제/일회성 실험 코드를 각 폴더 `legacy/`로 이동

**배경**: 이 저장소는 연습용이고, 실제 배포할 저장소에는 "진짜 서비스에
필요한 것만" 올리기로 함(사용자 지침). 두 가지를 확정: (1) 피처엔지니어링은
앞으로 Spark 코드만 유지 — 로컬 테스트도 `feature_engine/spark/`를 `local[*]`
단일 노드로 그대로 쓴다. (2) 학습(training)은 원래부터 Spark 기반이 아니었으므로
(LightGBM은 항상 로컬/소켓분산, 5번 항목) 이 원칙에서 제외 — training은
"실제 서비스 자동화 경로가 아닌 일회성 분석/튜닝 도구"만 legacy로 옮긴다.

**한 것**: `feature_engine/build_merged_table.py`/`build_rolling_rental_features.py`/
`features.py`(pandas 2차정제, `spark/`가 parity 테스트로 이미 검증된 동일 로직으로
대체)와 `scripts/validate_completion_curve.py`(일회성 진단 스크립트) + 그 전용
테스트 2개를 `feature_engine/legacy/`로, `training/experiment_log.py` +
`scripts/{build_embargo_candidate,compare_baselines,run_embargo_sweep}.py`(파라미터
튜닝/베이스라인 비교 도구) + `experiments/{tick_model_ooc,tick_model_sampled}/`를
`training/legacy/`로 `git mv`(히스토리 보존). 이동한 파일들의 상대 임포트를
새 깊이에 맞게 수정하고, `feature_engine/scripts/run_build_pipeline.py`(6~7단계
pandas 2차정제 호출 제거, 1~5단계만)와 `run_full_pipeline.py`(dataset 스테이지의
2차정제를 `.venv-spark`의 `feature_engine.spark.run_pipeline`으로 교체)를 새 구조에
맞게 고쳤다. 각 폴더 README/DESIGN.md와 루트 README.md도 갱신.

**검증**: 기존 Spark parity 테스트(`dev_spark_rolling_parity`/`dev_spark_build_features`/
`dev_spark_incremental`, 12개)로 Spark 로직이 pandas 기준 구현과 여전히 정확히
일치함을 재확인, 나머지 회귀 테스트(`common`/`training`/`inference` 34개 +
legacy 11개) 전부 통과 — 파일 이동/임포트 수정이 기존 동작을 깨지 않았음을 확인.
자세한 파일별 분류·근거는 당시 `LEGACY_AUDIT.md`에 남김(경로 불일치는
26번 항목에서 마저 해결).

## 26. Spark 피처마트 산출물 경로를 `training`/`inference`가 읽는 경로와 통일 + `DATA_ROOT` 회귀 버그 발견·수정

**배경**: 25번 항목 정리 중 발견한 미해결 문제 — `feature_engine/spark/run_pipeline.py`의
기본 산출물 경로(`data/processed_v2/spark/{PARAM_COMBO_ID}/station_hour_features_2025.parquet`)와
`training/config.py`(→`common/paths.py`)가 읽는 pandas 시절 챔피언 경로
(`data/processed_v2/station_hour_features_2025.parquet`)가 서로 달라서, 이제
2차정제를 Spark로만 하면 `training`이 그 산출물을 못 찾는 상태였다.

**발견한 회귀 버그**: 경로를 맞추려고 두 config를 대조하다가
`feature_engine/spark/config.py`의 `DATA_ROOT` 기본값 계산 자체가 깨져 있었다는
걸 발견 — `os.path.dirname(os.path.dirname(__file__))`을 두 번만 적용해서
`ml/feature_engine/data`(존재하지 않는 경로)를 가리키고 있었다. 이 파일이 원래
`ml/feature_engine/config.py`(ml 바로 아래, 16번 항목 재편 전)였을 때는
dirname 두 번으로 정확히 `ml/`에 닿았지만, 재편으로 `ml/feature_engine/spark/config.py`
(두 단계 더 깊음)로 옮기면서 dirname 호출 수를 안 늘려서 생긴 버그로 보인다.
단위 테스트가 전부 synthetic tmp 경로로 fixture를 만들어 쓰다 보니 기본 경로
계산 자체는 아무 테스트도 실제로 거치지 않아서 지금까지 안 걸리고 남아있었다.

**수정**:
1. `feature_engine/spark/config.py` — dirname 3번으로 고쳐 `ml/data`를 정확히 가리키게 함.
2. `common/paths.py` — `MERGED_TABLE_PARQUET`/`ROLLING_RENTAL_FEATURES_PARQUET`/
   `FEATURES_TABLE_PARQUET`을 `feature_engine/spark/config.py`와 **정확히 같은 공식**
   (`FEATURE_ENGINEERING_OUTPUT_ROOT`/`FEATURE_PARAM_COMBO_ID` 환경변수, 기본값
   수식도 동일)으로 계산하도록 변경. `training`/`inference`/`feature_engine/config.py`가
   전부 `common/paths.py`를 통해 이 값을 읽으므로(8/16번 항목에서 확립된 기존
   설계 — "세 인스턴스가 정확히 같은 경로를 봐야 한다") 한 곳만 고치면 셋이
   다시 같은 경로를 보게 된다. 1차정제 산출물(`STATION_MASTER_PARQUET` 등)과
   inference fallback 프로필(`STATION_HOURLY_PROFILE_PARQUET` 등)은 파라미터
   조합과 무관하므로 그대로 `PROCESSED_V2_DIR` 루트에 둠.

**검증**: `.venv-spark`의 `feature_engine.spark.config.FEATURES_TABLE_PARQUET`과
`.venv`의 `common.paths`/`training.config`/`inference.config`의 같은 이름
경로가 완전히 동일한 절대경로로 resolve되는지 직접 확인. 기존 회귀
테스트 45개(pandas) + 12개(Spark parity) 재실행 전부 통과 — 경로 변경이
아무 로직도 깨지 않았음.

**다른 파라미터 조합을 실험할 때 주의**: `FEATURE_ENGINEERING_OUTPUT_ROOT`/
`FEATURE_PARAM_COMBO_ID`(또는 그게 파생되는 `ROLLING_WINDOW_MINUTES`/
`ROLLING_EMBARGO_MINUTES`/`ROLLING_TICK_MINUTES`/`ML_PROFILE`) 환경변수를 Spark
실행과 training/inference 실행 양쪽에 **반드시 같은 값으로** 설정해야 한다 —
한쪽만 바꾸면 다시 어긋난다. 상세는 당시 `LEGACY_AUDIT.md` 참고.

## 27. 실험 폴더는 git에 안 올리기로 확정 — `tick_model_ooc`/`tick_model_sampled` 추적 해제

**배경**: 25번 항목에서 `training/experiments/{tick_model_ooc,tick_model_sampled}/`를
`training/legacy/experiments/`로 옮기며 "커밋해서 legacy로 남길지, 아예 git에서
빼서 `multi_horizon/`과 같은 미추적 상태로 맞출지" 정책이 안 정해진 채로
남겨뒀었다(`.gitignore`의 `experiments/` 규칙과 실제 상태가 어긋나 있었음 —
이 둘만 규칙 생기기 전에 이미 커밋된 예외였음). 사용자가 "실험 폴더는 안
올린다"로 확정.

**한 것**: 두 폴더를 `training/legacy/experiments/`가 아니라 `training/experiments/`
(= `multi_horizon/`과 같은 자리)로 되돌리고, `git rm -r --cached`로 git 추적만
끊었다(파일은 로컬에 그대로 있어 계속 실행 가능). 이제 `training/experiments/`
아래 세 실험 폴더(`tick_model_ooc`/`tick_model_sampled`/`multi_horizon`) 모두
`.gitignore`의 `experiments/` 규칙에 똑같이 걸려 git에는 안 올라간다.
`training/legacy/`에는 "실험 산출물"이 아니라 "일회성 분석 CLI 도구"인
`experiment_log.py`/`scripts/{build_embargo_candidate,compare_baselines,run_embargo_sweep}.py`만
남는다.

상세는 당시 `LEGACY_AUDIT.md` 참고.

## 28. `feature_engine`의 pandas는 1차정제까지 전부 legacy — 25번 항목 분류 정정

**배경**: 25번 항목에서 "2차정제(피처엔지니어링)만 Spark로 통일, 1차정제(원본→
station_master/targets/status/weather/population)는 Spark 대응 구현이 없으니
그대로 유지"로 분류했었는데, 사용자가 이걸 정정 — `feature_engine`은 1차정제든
2차정제든 pandas 코드 자체를 안 쓴다. 실제 배포에서는 1차정제를 이 저장소 밖의
다른 시스템이 처리하므로(5번 항목), 본 서비스 저장소의 `feature_engine`에는
`spark/`(2차정제)만 있으면 된다 — 1차정제 pandas는 이 연습 저장소에서 로컬
테스트 입력을 준비하는 용도로만 legacy에 남는다.

**한 것**: `feature_engine/{config,build_station_master,build_targets,
build_station_status,build_weather,build_population,grid}.py`와
`scripts/run_build_pipeline.py`를 `git mv`로 `feature_engine/legacy/`로 이동(남는
게 없어진 `feature_engine/scripts/` 디렉터리는 삭제). 이동한 파일들의 상대
임포트를 재조정하고, `feature_engine.config`를 참조하던 세 곳(`run_build_pipeline.py`,
legacy 테스트 2개, `feature_engine/tests/dev_spark_rolling_parity.py`,
`inference/tests/dev_rental_censoring_cross_parity.py`)을 각각 맞는 대상으로
갱신 — 순수 상수만 쓰는 곳은 `common.common_config`로 legacy 의존을 없애고,
`ROLLING_RENTAL_FEATURES_PARQUET` 경로를 monkeypatch해야 하는
`dev_rental_censoring_cross_parity.py`는 `features.py`가 실제로 읽는 모듈과
같아야 해서 `feature_engine.legacy.config`를 그대로 써야 했다(처음에
`common_config`로 바꿨다가 테스트가 깨져서 원인 파악 후 되돌림). `run_full_pipeline.py`/
루트 `README.md`/`feature_engine/README.md`/`DESIGN.md`/`common/README.md`도
새 구조에 맞게 갱신.

**결과적으로 `feature_engine/` 최상위(legacy 제외)에는 `spark/`, `tests/dev_spark_*.py`,
`__init__.py`, `README.md`, `DESIGN.md`만 남는다.**

**남은 한계**: `inference/tests/dev_rental_censoring_cross_parity.py`는 여전히
`feature_engine.legacy.{config,features}`에 의존한다 — 배치 계산 기준으로
Spark 대신 pandas `features.py`를 쓰기 때문(가볍게 `.venv`만으로 돌 수 있어서,
pandas==Spark parity가 이미 검증돼 있다는 사실에 기대는 간접 검증). 본 서비스
저장소가 `feature_engine/legacy/`를 전혀 안 가져가기로 하면 이 테스트를 어떻게
할지(legacy로 같이 옮기기/Spark 기반으로 재작성/포기) 결정이 필요 — 코드로
판단할 문제가 아니라서 임의로 안 골랐다.

**검증**: 기존 회귀 테스트 45개(pandas) + 12개(Spark parity) 재실행 전부 통과.

상세는 당시 `LEGACY_AUDIT.md` 참고.

## 29. common/training/inference도 실제 참조 관계로 재검증 — `profiles/embargo45.json` legacy로 추가 이동

**배경**: 28번 항목에서 feature_engine 분류를 정정한 뒤, 사용자가 "다른 폴더(training/
inference)는 legacy 없어? common도 봐줘"라고 재차 확인 요청 — 이전에 "전부
유지"라고 답했던 걸 다시 근거 있게 검증하라는 뜻으로 받아들여, "git 추적 여부"가
아니라 각 파일을 실제로 누가 import/참조하는지 grep으로 다시 추적했다.

**확인 결과**: `training/`(config/monitor_performance/train_common/
train_rental_model/train_return_model/monthly_retrain_check)와 `inference/`
(config/predict_common/predict_{rental,return}_demand/predict_single/
build_{station,population}_profile) 전 파일이 실제로 서로 참조되거나
README/`run_full_pipeline.py`의 엔트리포인트로 쓰이고 있음을 재확인 — legacy
추가 후보 없음. `inference/predict_single.py`가 `common/rolling_window_features.py`를
직접 import하지 않는 게 특이해 보였는데, 성능 때문에 자체 벡터화 재구현
(`_rental_visible_at` 등, 22~24번 항목)을 쓰는 의도된 설계였고 그 재구현의
정확성은 별도 테스트가 대조 검증하고 있어 문제 없음.

**추가로 발견한 것**: `common/profiles/embargo45.json` — JSON 파일 자체의
`_comment`가 "프로필 메커니즘 검증용" 예시라고 스스로 밝히고 있었다. 이 값을
실제로 실험하려던 도구(`run_embargo_sweep.py`)는 이미 legacy로 옮겨져 있어서,
이 프로필 파일만 `common/profiles/`에 남아있는 게 일관성이 없었다 —
`common/legacy/profiles/embargo45.json`으로 이동. `common/profiles/`에는
`default.json`(챔피언)만 남는다. `common/README.md`의 예시 명령/파일 참조표도
갱신(`training/scripts/compare_baselines.py` → `training/legacy/scripts/...`
경로도 같이 고쳤다 — 25번 항목 이후 안 고쳐져 있던 걸 발견).

**검증**: 회귀 테스트 45개(pandas) + 12개(Spark parity) 재실행 전부 통과.

상세는 당시 `LEGACY_AUDIT.md` 참고.

## 30. 환경 관리를 uv로 전환 + `common`을 `libs/ml_core/`으로 분리(독립 라이브러리화)

**배경 1(uv)**: 사용자가 환경 관리를 pip+venv(공용 `.venv`/`.venv-spark`,
`requirements.txt`)에서 `uv`로 바꾸기로 함 — `feature_engine`/`training`/`inference`
각자 독립 배포되는 설계(16번 항목)와 맞춰, 폴더별로 독립된 `pyproject.toml`/
`uv.lock`/`.venv`를 쓰도록 전환. `feature_engine`은 `==3.11.*`(pyspark 제약,
EMR 8.0.0 기본값), 나머지 둘은 `>=3.11`. 세 폴더가 실제로 무엇을 import하는지
grep으로 정확히 추적해 의존성을 채웠다(예: inference는 lightgbm을 직접 import
안 하지만 `common.scoring`을 통해 씀 — 직접 import하는 것만 명시).

**배경 2(ml_core 분리)**: 사용자가 "본 레포에서는 `common`을 `ml`과 같은
계층의 `lib/` 폴더 아래 `ml_core`으로 따로 만들어 넣을 것"이라고 확정 —
`lib/`가 이 저장소의 다른 서비스(`client`/`etl`/`infra`/`weather-etl`)와도
공유될 수 있는 자리라 `common`이란 일반적인 이름은 충돌 위험이 있어서
`ml_core`으로 이름 붙임. `ml/common/` → `<repo-root>/libs/ml_core/`로
`git mv`, 패키지 import명도 `common` → `ml_core`으로 전부 변경(처음엔
디렉터리를 `ml-core`으로 만들었다가 오타였다고 정정받아 `ml_core`으로
다시 바꿈 — 디렉터리명/프로젝트명/import명이 전부 `ml_core`으로 통일).
feature_engine/training/inference/legacy/experiments 전체에서 `from common import`/
`common.X`/`common/X` 패턴 60여 곳을 일괄 치환(대부분 perl — macOS 기본
`sed -E`는 `\b`(단어 경계)를 지원하지 않아 처음에 조용히 실패했었음, GNU
sed 없어서 perl로 교체).

**진짜 문제 하나 발견**: `common/paths.py`의 `ML_ROOT = Path(__file__).resolve().parents[1]`은
"내 파일 위치에서 두 칸 위가 `ml/`"라는 가정이었는데, `ml_core`이 `ml/`의
형제 디렉터리가 되면서(조상이 아니라 다른 가지) 이 가정 자체가 깨졌다 — `ml/`은
`__file__`의 parents 체인 어디에도 없다. **cwd 기준으로 바꿔서 해결**
(`ML_ROOT = Path.cwd()`) — 이 저장소 전체가 "`cd ml` 다음에 실행"을 전제로
하므로(모든 README의 실행 명령이 그렇게 시작함) 그 컨벤션에 기대는 게
`ml_core`이 자기 위치를 몰라도 되게(진짜 독립 라이브러리답게) 만드는
가장 단순한 방법이었다. `training/scripts/monthly_retrain_check.py`가
`ML_ROOT`를 쓰고 있어서 심볼 자체는 유지하고 계산 방식만 바꿈 — 이 파일이
쓰던 `SPARK_PYTHON = ML_ROOT / ".venv-spark" / ...`도 uv 전환 이후 안 맞게 된
옛 경로라 `feature_engine/.venv`로 같이 갱신.

**검증**: `libs/ml_core/`은 `ml/pytest.ini`의 `dev_*.py` 규칙을 더 이상
상속받지 못해서(다른 rootdir) 자체 `pyproject.toml`에 같은 규칙을 추가.
세 폴더 모두 `uv sync`로 실제 `.venv` 재생성 + `uv lock`으로 `ml_core`
editable 의존성이 새 경로(`../../libs/ml_core`)로 정확히 잡히는지 확인,
전체 회귀 테스트 57개(`ml_core` 17 + `training` 9 + `inference` 8 +
`feature_engine` legacy 11 + Spark parity 12) 재실행 전부 통과 — 대량 치환이
아무 것도 깨뜨리지 않았음을 확인.

**남은 것**: 이번 작업으로 문서(README/DESIGN.md 등)의 `common/` 참조는
전부 갱신했지만, `history.md`의 과거 항목(1~29번)은 당시 실제 코드가
`common`으로 불렸던 시점의 기록이라 그대로 둔다(이 저장소의 결정 로그
컨벤션 — 과거 기록은 그때 사실을 남기고, 최신 상태는 README/LEGACY_AUDIT.md가
반영). 자세한 파일별 변경 목록은 당시 `LEGACY_AUDIT.md`의
"환경 관리 — uv + `libs/ml_core/`" 절 참고.

---

## 31. Feature Engine Spark DAG 병목 개선 및 EMR 분산 실행 최적화

**배경**: EMR(m4.large 8대, 28 cores) 환경에서 `monthly_retrain` 실행 시 Spark Multi-horizon 생성이 비정상적으로 길어지고 YARN DistributedShell 기본 타임아웃(10분)으로 학습 스텝이 강제 Kill되는 현상이 발생했다.

**원인 및 결정**:
1. **증분 Action 중복 셔플**: `features_increment`에 캐싱 없이 4회 연속 Action(count, write, agg)이 호출되어 상류 Rolling Self-Join 셔플이 4번 반복 실행됨 -> `.cache()` 및 `.unpersist()` 적용.
2. **2단계 Catalyst 누적 계획 병목**: `build_multi_horizon_features.py`에서 순차 Union(깊이 11)을 돌려 Catalyst 재분석 비용이 선형으로 폭증함 -> 균형 이진트리(`_balanced_union_by_name`, 깊이 4)로 개편.
3. **대여/반납 중복 스캔 제거**: `--models rental|return` 옵션을 추가하여 요청된 단일 모델의 Multi-horizon Mart만 단독 생성하도록 분리.
4. **Parquet 파티션 내부 정렬**: `sortWithinPartitions("date", "anchor_ts", "station_no", "horizon")`를 적용해 Parquet 압축률과 다운스트림 `lazy_train_dataset`의 학습 로딩 속도를 최적화.
5. **YARN 타임아웃 확장**: `distributedshell.Client`의 기본 10분 타임아웃을 `-timeout 345600000`(4일)으로 확장.
6. **강제 재생성 플래그 추가**: 워터마크 기반 신선도 스킵을 우회할 수 있는 `--force` 및 Airflow `force_refresh_feature_mart` Param 추가.

**실측 성과 (EMR 1년치 275일치 전체 빌드)**:
- 2단계 Multi-horizon 실행 시간: 7.4분 -> 2.5분 (66.2% 단축)
- 총 CPU 연산 시간(Task Time): 3.0시간 -> 41분 (77.2% 절감)
- JVM GC 시간: 13분 -> 1.3분 (90% 격감)
- S3 읽기량: 27.4 GiB -> 4.9 GiB (82.1% 감소)
- 네트워크 셔플 전송량: 13.0 GiB -> 6.6 GiB (49.2% 감소)
- 총 태스크 수: 4,736개 -> 1,557개 (67.1% 감소)

---

## 32. YARN DistributedShell 기반 EMR 분산 오케스트레이션 정착

**배경**: EMR(m4.large 8대) 환경에서 월간 재학습 오케스트레이터(`monthly_retrain_check`)와 분산 학습 워커 실행 시 마스터 노드 메모리 고갈과 자원 경합 문제가 발생했다.

**원인 및 해결 과정**:
1. **Master 노드 OOM (ExitCode 137)**: EMR Master 노드(m4.large 8GB)는 기본 데몬(ResourceManager, NameNode 등)만으로 ~5.7GB를 점유하여, 오케스트레이터 프로세스가 약 1.5GB만 사용해도 OOM-killer에 의해 강제 종료됨 -> 오케스트레이터 프로세스 자체를 Core 노드의 YARN 컨테이너(`-num_containers 1`)로 격리 실행.
2. **DistributedShell AM 힙 부족 (JNI Error)**: DistributedShell의 기본 100MB AM 힙으로 인해 클래스 로딩 중 OutOfMemoryError가 발생하던 문제를 `-master_memory 1024`로 확장하여 해결.
3. **Spark-submit 래퍼 자원 경합 제거**: Spark-submit으로 오케스트레이터를 감쌀 경우 연산을 안 해도 Dynamic Allocation이 executor를 최대 50개까지 점유하여 실제 분산학습 워커와 경합하던 문제를, 순수 YARN DistributedShell 호출로 전환하여 완전 해결.
4. **중첩 분산 환경 노드 예약 (`_WRAPPER_NODE_RESERVATION = 3`)**: Outer AM, Outer Worker, Inner AM이 서로 다른 노드에 분산 배치되는 최악의 경우를 대비하여 `core_instance_count - 3`개의 워커를 요청하도록 설계(Barrier 타임아웃 방지).
5. **클라이언트 타임아웃 확장**: YARN `distributedshell.Client`의 10분 기본 타임아웃을 `-timeout 345600000`(4일)으로 확장.

---

## 33. 월간 재학습 단일 EMR 생애주기 통합 및 Resize 제거

**배경**: 기존 대여/반납 분리 DAG 및 2단계 클러스터 리사이즈(`resize_emr_cluster()`, 3노드 -> 8노드) 운영 시 노드 프로비저닝 지연과 실행 중 스텝 유실 위험이 존재했다.

**원인 및 결정**:
1. **단일 EMR 클러스터 순차 실행**: `monthly_retrain` 단일 DAG에서 하나의 EMR 클러스터 생애주기 동안 대여 사이클(평가 -> 재학습 -> 승격) 완료 후 반납 사이클을 순차 실행하여 클러스터 2중 기동 오버헤드(15분) 및 동시성 충돌 차단.
2. **동적 Resize 제거**: 스텝 실행 중 노드가 축소/확장될 때 컨테이너가 유실되는 위험을 방지하기 위해, 처음부터 8노드(`TRAINING_CORE_INSTANCE_COUNT = 8`)로 고정 프로비저닝.
3. **테스트 프로필 격리**: 테스트용 프로필(`a-test-sparse-flat`)의 피처마트 경로(`w65_e45_t20`)를 프로덕션 기본(`w60_e40_t20`)과 물리적으로 분리하여 덮어쓰기 데이터 오염 방지.

---

## 34. Multi-Horizon 워터마크 격리 및 평가 캐시 불연속 구간 이중 집계 방지

**배경 및 원인**:
1. **단일 모델 실행 시 공통 워터마크 오염**: `build_multi_horizon_features.py --models return` 단독 실행 시 `return` 모델 전용 워터마크뿐만 아니라 공통 워터마크(`_multi_horizon_watermark.json`)까지 최신으로 갱신되어, 뒤이어 실행된 `rental` 모델이 전용 워터마크가 없음에도 공통 워터마크 fallback을 보고 fresh로 오판하여 생성을 잘못 스킵하는 문제가 존재했다.
2. **평가 캐시 불연속 날짜 이중 집계**: `evaluate_recent_performance_cached()`에서 캐시가 없는 날짜(`missing_days`)가 불연속(예: 08-01, 08-03 결측, 08-02 캐시 존재)일 때 `missing_days[0] ~ missing_days[-1]`(08-01~08-03) 전체를 하나의 shard로 평가하여, 중간에 이미 캐시된 08-02 날짜의 데이터가 최종 `combine_evaluation_shards()`에서 이중 집계되는 버그가 존재했다.

**해결**:
1. **모델 전용 워터마크 독립성 보장**: `build_multi_horizon_features.py`에서 단일 모델 freshness 검사 시 공통 워터마크 fallback을 완전히 제거하고 전용 워터마크만 바라보도록 수정. 공통 워터마크는 `rental`과 `return` 두 모델의 전용 워터마크가 둘 다 최신일 때만 갱신하도록 조건부 갱신 가드 추가.
2. **불연속 날짜 연속 구간 분할 (`_group_contiguous_dates`)**: `missing_days`를 연속된 날짜 구간 리스트(예: `[('08-01', '08-01'), ('08-03', '08-03')]`)로 묶어 결측 구간만 정밀하게 샤드로 평가하고 일자별 캐시를 저장하도록 리팩토링.
3. **회귀 테스트 완비**: `test_model_isolation_when_only_other_model_updated` 및 `test_evaluate_recent_performance_cached_missing_cached_missing_pattern` 추가.



