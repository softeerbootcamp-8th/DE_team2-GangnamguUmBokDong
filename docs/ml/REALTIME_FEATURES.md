# Point-in-Time 대여 카운트 (Train-Serving Skew 대응)

**(2026-08 갱신 — 서로 다른 세 가지 간격을 분리한다)** 운영 추론 호출 주기는
`SERVING_TICK_MINUTES=5`로 고정이다. 과거 feature/target과 rolling을 계산하는
모델 grid는 기본 20분(`GRID_TICK_MINUTES`/`ROLLING_TICK_MINUTES`)이고, 실제 학습
행 밀도는 `TRAIN_ANCHOR_TICK_MINUTES`가 정한다(기본 g20/r20/a20, 비교용
g5/r5/a5 또는 g5/r5/a20 등). g/r은 같은 값으로
`{5, 10, 15, 20, 30, 60}`분을 지원한다. rolling 폭은 60분, 현재 embargo는
40분이다. 정확한 기본값과 허용
조합의 단일 소스는 `libs/ml_core/profile_contract.py`다.

이 문서는 실시간 서빙 롤링 피처에서 발생하는 **우측 절단
(right-censoring) train-serving skew**를 다룬다. 핵심 로직(`ml_core/rolling_window_features.py`,
`feature_engine/spark/build_rolling_rental_features.py`)은 이미 기존 배치 파이프라인
([feature_engine/DESIGN.md](feature_engine/DESIGN.md), [inference/DESIGN.md](inference/DESIGN.md))에 실제로 연결돼 있다 —
학습 쪽은 `feature_engine/spark/build_features.py`의 `_add_rental_lag_rolling`(4-2절), 추론 쪽은
`inference/predict_single.py`의 `_censored_rental_recent`(4-3절)가 각각 이 파일의 함수를
가져다 쓴다. 반납(return)은 지연 관측 문제가 없어 대상이 아니다.

**주의 — 다른 이슈와 혼동 금지**: 정류소 재고 0으로 인한 censored demand는
별개 문제로, 기존 시간 단위 모델에서 이미 Poisson objective + exposure
offset으로 다루고 있다 ([training/DESIGN.md](training/DESIGN.md) 2절). 이 문서가 다루는
건 그것과 무관한, **대여/반납 이벤트 로그 자체의 지연 관측** 문제다.

---

## 1. 문제 정의

실시간 서빙은 대여/반납 로그 API로 이벤트를 받는데, **트립 하나는 반납이
완료돼야 로그에 나타난다** (대여 시작 시점엔 아직 안 잡힘). 그래서 예측
기준 시점 T에서 "최근 N분간 대여량" 같은 롤링 피처를 만들면, 그 구간 동안
대여는 시작됐지만 아직 반납되지 않은 트립은 실제로 존재하는데도 카운트에서
빠진다. 시간이 지나 반납이 뒤늦게 로그에 반영될수록 그 값이 슬금슬금
올라가는 구조다.

과거 이력으로 학습 데이터를 만들 때는 몇 달~몇 년이 지나 트립이 전부 반납
완료된 상태라 이 결측이 안 보인다 — 학습 데이터는 항상 "완전한" 값을 보고,
서빙은 항상 "불완전한(절단된)" 값을 보게 되어 **분포가 어긋난다
(train-serving skew)**.

**검토했다가 기각한 방법**: 관측값 ÷ 완료율로 "추정 실제값"을 보정하는 방식.
완료율 자체의 표준편차가 커서(특히 절단이 심한 구간) 나눗셈이 작은 관측
오차를 훨씬 크게 증폭시키므로 채택하지 않았다.

**채택한 해법**: 보정 대신, 학습 피처를 만들 때도 서빙과 동일하게
"그 순간 실제로 관측 가능했던 값"만 쓰도록 의도적으로 가린다(censoring).
그러면 모델이 이 저평가 패턴 자체를 학습 과정에서 자연스럽게 배운다.

---

## 2. 윈도우 설계 — "5분 단위"는 폭이 아니라 갱신 주기였다

**초기 구현의 실수**: 처음엔 "5분 단위 rolling"이라는 표현을 "윈도우 폭이
5분"이라고 잘못 해석해서 `[T-5, T)`처럼 T에 붙어있는 5분짜리 창을 계산했다.
실측해보니 이 설계는 버킷이 닫히는 순간 완료율이 **4.0%**(대부분 노이즈)에
불과했다. "5분 단위"는 **서빙이 5분마다 값을 다시 계산한다는 뜻**이지, 각
계산이 보는 구간의 폭이 5분이어야 한다는 뜻이 아니었다 — 사용자가 이 오류를
직접 지적해서 재설계했다.

**교정된 설계**: 창의 폭은 넓게(1시간), 가장 최신(완료율 낮은) 구간은
**embargo**로 건너뛴다.

```
윈도우 = [T - embargo - width, T - embargo)
포함 조건: start_ts가 이 구간 안에 있고, end_ts가 결측이 아니며 end_ts <= T
```

현재 기본값: `width=60분, embargo=40분` — "40분 전부터 1시간 40분 전까지"를
본다. 배치 학습 피처는 설정된 model grid(기본 20분)에서 계산하고, 서빙은
각 5분 요청 시각에서 같은 윈도우 정의를 직접 계산한다.

**실측 효과 비교** (2025-06 데이터 기준, 전체 트립 대비 그 순간 관측 가능한 비율):

| 설계 | 완료율 | 비고 |
|---|---|---|
| `width=5, embargo=0` (초기 오류) | **5.15%** | 거의 노이즈 — 폐기 |
| `width=60, embargo=30` (교정) | **88.2%** | 약 17배 개선 |

이 표는 embargo 30분을 채택했을 당시의 2025-06 실측 기록이다. 이후 운영
기본값은 40분으로 보수적으로 늘어났으며, 현재 프로필의 성능은 학습 실행별
MLflow 지표로 확인한다. 폭을 넓혀 표본을 늘리고 최신 구간(완료율이 낮은
구간)을 건너뛰면 신호 대 잡음비가 완전히 달라진다. `embargo=0, width=tick`으로 두면 예전의
"버킷이 닫히는 순간" 방식과 동일해지므로, 그 특수 케이스는 핵심 개념을
보여주는 단위 테스트로 남겨뒀다 (5절 `add_censored_visibility`).

---

## 3. 핵심 규칙 (학습·서빙이 반드시 동일하게 지켜야 하는 계약)

구현은 [`ml_core/rolling_window_features.py`](../../libs/ml_core/rolling_window_features.py) 하나에 몰아뒀다(`ml/`과 별도로 관리되는 `libs/ml_core/` 라이브러리):

| 함수 | 용도 |
|---|---|
| `floor_to_window(ts, window_minutes)` | 타임스탬프를 tick 단위로 내림 |
| `add_censored_visibility(trips, window_minutes, ...)` | **개념 설명/단위 테스트용**: `embargo=0`인 특수 케이스 — 트립마다 버킷 배정 + "버킷이 닫히는 순간 관측 가능했는가"를 벡터화 계산 |
| `count_visible_in_window(events, as_of, window_minutes, embargo_minutes, ...)` | **서빙용**: 임의의 단일 시각 `as_of` 기준 `[as_of-embargo-window, as_of-embargo)` 관측 가능 카운트 (소량의 최근 이벤트 버퍼에 대해 즉시 계산) |
| **`censored_rolling_counts(trips, window_minutes, embargo_minutes, tick_minutes, ...)`** | **배치(학습 데이터 생성)용**: 모든 tick T에 대한 point-in-time 카운트를 한 번에 계산 |
| `lookup_count_at_ticks(cumulative, query_ticks, ...)` | `censored_rolling_counts()`의 sparse 결과에서 원하는 (station, tick) 값을 조회 |

**`censored_rolling_counts()`의 알고리즘 — 차분 배열(difference array)**: 트립
하나가 카운트에 잡히는 T는 한 tick이 아니라 **연속된 여러 tick**이다 (윈도우가
그 트립의 시작 시각을 담고 있는 동안, 그리고 `end_ts<=T`가 성립하는 동안만).
이걸 트립마다 tick 단위로 펼치면 느리므로, "카운트가 +1 되는 시작 tick"과
"-1 되는 종료+1 tick" 딱 2개 이벤트만 기록한 뒤 station별로 시간순
누적합(cumsum)하는 차분 배열 기법으로 O(트립 수)에 계산한다. 결과는
**sparse한 step function**이라 특정 tick의 값은 `lookup_count_at_ticks()`로
조회한다 (`pd.merge_asof(direction="backward")` 기반).

> **구현 함정 — `merge_asof(by=...)`는 station별이 아니라 `on` 컬럼 전체가
> 전역적으로 정렬돼 있어야 한다.** `[station, tick]` 순으로 정렬하면(각
> station 내부는 정렬돼 있어도) pandas가 "keys must be sorted" 에러를 낸다.
> `tick` 하나만으로 전역 정렬해야 `by=station`이 정상 동작한다. 여러
> station이 섞인 실데이터로 검증하다가 발견한 문제라, station 1개짜리
> 테스트만 돌렸으면 못 잡았을 실수였다 — 그래서 4절의 브루트포스 검증이
> 중요했다.

두 함수(배치/서빙)는 인터페이스는 다르지만 **핵심 필터 조건은 동일**하다.
이 조건이 갈라지면 정확히 이번에 고치려는 skew가 다시 생기므로, 둘 중
하나만 고치는 일이 없도록 반드시 이 파일을 통해서만 로직을 바꿔야 한다.

### 학습·서빙 파이프라인 공유에 대해

현재 운영 추론 진입점은 `inference.predict_single`이며 Airflow가 5분마다
`--all-stations`로 실행한다. 사용자 요청을 받는 API 계층과는 분리돼 있지만,
학습과 이 추론 경로는 다음 규칙으로 같은 rolling 정의를 공유한다.

1. **서빙 모듈이 Python(FastAPI)으로 만들어진다면** `rolling_window_features.count_visible_in_window()`를
   그대로 import해서 쓰는 걸 강력히 권장한다 — 같은 저장소(`ml/src/`) 안에
   있으므로 의존성만 걸면 된다. `predict_single.py`(4-3절)가 이미 이 방식으로
   실시간 서빙을 흉내내고 있으니, 실제 서빙 모듈을 만들 때 그 코드를 참고하면 된다.
2. **다른 스택으로 만들어진다면** 최소한 2~3절의 윈도우 정의(`width=60,
   embargo=40`)와 핵심 규칙(`end_ts <= 기준시각`)을 코드 리뷰 체크리스트에
   명시하고, 이 문서를 링크로 남겨서 대조 확인해야 한다.
3. 이번 학습 파이프라인(`build_rolling_rental_features.py`)과 향후 서빙
   모듈이 **같은 정의를 안 쓰게 되는 순간이 바로 이 skew가 재발하는
   시점**이라는 걸 팀 전체가 인지하고 있어야 한다 — 이 문서를 만든 이유.

---

## 4. 학습 데이터 생성 — `feature_engine/spark/build_rolling_rental_features.py`

**`load_rental_trip_events()`** (공개 함수 — `predict_single.py`도 재사용, 5절 참고):
2025년 12개월 대여이력 parquet에서 `station_master` 크로스워크로 매칭된 트립만
추린다 (`build_targets.py`의 `_normalize_station_no()` 재사용 — 로직 중복을 피함).

**`build_rolling_rental_features()`**: `censored_rolling_counts()`로 모든
station의 point-in-time 카운트를 `ROLLING_TICK_MINUTES` grid(기본 20분)에서
계산한다.

- **소스**: 날짜별 Archive `bike_rental_history`; station 매칭은
  `silver_source.read_rental_trips()`가 담당하고 누락 partition은 fail-closed한다.
- **출력**: S3의 profile-scoped rolling feature 경로 —
  `station_id, tick, count` sparse step function.
- **과거 실측**: 로컬 2025 데이터의 5분 실험은 46,930,933행/2,580개 정류소였고,
  당시 완료율은 2절 표의 88.2%였다. 이는 현행 기본 20분 산출물 크기가 아니다.

**브루트포스 교차검증**: 임의의 (station, 시각) 4개를 골라 "trips를 직접
필터링해서 센 값"과 `lookup_count_at_ticks()` 조회값을 대조해 전부 일치함을
확인했다 — 위에서 언급한 `merge_asof` 정렬 버그를 여기서 처음 잡아냈다.

**dense rolling grid는 안 만든다** — 과거 5분 예시의 station×tick 전체 조합은
2,580개 정류소 × 105,120틱 ≈ 2.71억 행이다. 이를 미리 채우지 않고 sparse
step function을 만든 뒤, 실제 base feature grid(g20 또는 선택한 g5 등)의
tick에서 `lookup_count_at_ticks()`로 조회한다.

---

## 4-2. 학습 파이프라인 반영 — `feature_engine/spark/build_features.py`

**(2026-08 갱신) 지금은 대여 lag 피처가 `rental_lag_1h` 하나뿐이다** —
`roll_mean/std_3h·24h`, `rental_lag_24h/168h`는 피처 중요도 분석 후 전부
제거됐다(현재 전체 피처 목록의 단일 소스는 `libs/ml_core/common_config.py`/
`model_contract.py`). 아래는 그 제거 전, `rental_lag_1h`가 여럿 중 하나였던
시절 기준의 원래 설명이다 — `rental_lag_1h`를 point-in-time censored 값으로
만드는 핵심 원리(가장 최근 구간만 censoring 처리하면 충분한 이유)는 지금도
그대로 유효하다:

- 시간 단위 그리드에서 시각 T의 버킷 경계는 항상 T로부터 0분 또는 60분 배수만큼
  떨어져 있다 — 즉 `rental_lag_1h`(T-1h~T, 경과 0분)만 실측 완료율이 낮은
  수준으로 심하게 노출돼 있다. `T-2h`부터는 경과 60분(실측 ~92~94%)로 이미
  이 설계 자체가 "충분히 좋다"고 받아들인 수준이라 추가 처리하지 않는다 —
  그래서 censoring 처리 대상이 애초에 `rental_lag_1h` 하나로 좁혀져 있었고,
  나머지 lag/rolling이 없어진 지금도 이 부분 로직은 그대로 재사용된다.
- 지금 `feature_engine/spark/build_features.py`가 `rolling_rental_features`
  (4절 산출물)를 `lookup_count_at_ticks()`로 station_no/hour_ts별로 조회해
  그 값을 `rental_lag_1h`에 직접 넣는다(추가 shift 불필요, 정의상 이미
  embargo 이전 정보만 씀). `return_lag_1h`는 이 파일이 전혀 건드리지 않는다
  (반납은 지연 관측 문제 없음, raw 값 그대로).
- 두 산출물(rolling 결과와 feature 테이블)이 조용히 어긋나는 사고를 막는
  freshness 가드가 있다 — 최신 tick 정합성을 확인 후 어긋나면 명시적으로
  실패한다.

## 4-3. 추론 파이프라인 반영 — `inference/predict_single.py`

`predict_single.py`는 실시간 서빙을 흉내내는 단일 시점 예측 모듈이라(3절 참고),
학습과 다른 이유로 같은 문제를 겪는다 — 이전에는 히스토리를 시간 단위로 이미
집계된 병합 테이블에서만 가져왔는데, 그 집계 자체가 censoring을 반영하지
않은 "완전한" 값이었다. `rental_lag_1h`(2026-08 갱신 — 예전엔 대여 쪽 4개
피처였다)는 트립 단위 원본으로 다시 계산한다:

- `_get_rental_events_by_station()`이 `load_rental_trip_events()`(4절)로 전체
  트립(station_id, start_dt, end_dt)을 한 번 로드해 station별로 캐시한다 —
  `_get_history_by_station()`(시간 단위 집계, `return_lag_1h`용)과 별개의 캐시.
- `_censored_rental_recent()`가 3절의 서빙용 함수
  `count_visible_in_window(events, as_of, window_minutes=60, embargo_minutes=40)`을
  `target_ts` 기준으로 호출해서 `rental_lag_1h`를 계산한다 — 배치
  (`feature_engine/spark/build_features.py`)와 정의(윈도우/embargo)가 동일하다
  (`tests/dev_rental_censoring_cross_parity.py`가 둘의 출력이 일치함을 확인).
- fallback 판정: 시간 단위 집계는 "히스토리에 없으면 NaN"이 곧 결측이었지만,
  트립 단위 소스에서는 "윈도우에 트립 0건"(정상 관측값 0)과 "그 시점 자체가
  로드된 트립 데이터 커버리지 밖"(진짜 결측)을 구분해야 한다. 로드 시점에
  캐시해둔 `(start_dt.min(), start_dt.max())` 커버리지 범위로, anchor의 윈도우가
  그 범위를 전혀 안 겹치면 fallback(profile 대체), 겹치면 0을 포함한 실제
  카운트를 그대로 쓴다.
- 이 두 함수는 아직 CLI/대화형 시뮬레이션용이다 — 3절이 이미 강조하듯,
  실제 실시간 서빙 모듈이 Python으로 만들어지면 `count_visible_in_window()`를
  그대로 import하는 걸 권장하고, 다른 스택이면 최소한 이 문서의 윈도우 정의를
  코드 리뷰에서 대조해야 한다.

---

## 5. 실측 검증 — `feature_engine/scripts/validate_completion_curve.py`

"시간이 지날수록 관측 완료율이 올라간다"는 주장 자체를, 가장 단순한 특수
케이스(`embargo=0, width=5`, 즉 `add_censored_visibility`)로 실제 2025-06
데이터에서 확인했다. 버킷이 닫힌 시점(elapsed=0)부터 60분 후까지 5분
간격으로, 그 순간 관측 가능했던 비율을 station×버킷별로 계산해 평균/표준
편차를 냈다 (표본이 5건 미만인 버킷은 노이즈가 커서 제외):

| elapsed(분) | 평균 완료율 | 표준편차 |
|---|---|---|
| 0 | 4.0% | 9.7%p |
| 5 | 29.4% | 27.6%p |
| 10 | 50.1% | 31.9%p |
| 15 | 61.8% | 31.8%p |
| 20 | 68.8% | 30.4%p |
| 30 | 77.5% | 26.8%p |
| 40 | 83.3% | 23.3%p |
| 50 | 88.3% | 19.3%p |
| 60 | 92.3% | 15.4%p |

> **구현 함정 — 파이썬 기본 인자는 import 시점에 한 번만 평가된다.**
> `compute_completion_curve(trips, window_minutes=config.ROLLING_WINDOW_MINUTES)`처럼
> 기본값을 config 상수에서 가져오게 짜뒀었는데, 2절 재설계 때
> `ROLLING_WINDOW_MINUTES`를 5→60(윈도우 폭 의미로 재정의)으로 바꾸자 이
> 완료율 곡선 계산까지 **조용히 60분 버킷으로 바뀌어버렸다** (에러 없이
> 그냥 다른 숫자가 나옴 — 60.3%/68.1% 같은 값). "예전엔 4.0%였는데 왜 지금은
> 60%가 나오지?"를 역추적하다 발견했다. 지금은 이 함수의 기본값을 `5`로
> 하드코딩해 config 값과 분리했다 — **설명용 상수와 실제 채택 설계의 상수는
> 절대 같은 config 변수를 공유하면 안 된다**는 교훈.
**단조증가(0%→92%) 확인** — 우측 절단이 시간 경과로 서서히 해소되는 패턴이
실제 데이터에서도 명확히 나타난다. 이 결과는 `tests/dev_completion_curve_integration.py`에
자동 회귀 테스트로도 들어가 있다. (이 곡선은 특수 케이스 설명용이고,
`width=60/embargo=30`을 쓰던 당시의 종합 완료율은 2절의 88.2%다. 현재
운영 기본값은 `width=60/embargo=40`이다.)

**대조**: 4-2/4-3절 작업을 시작하기 전, 별도로(다른 방식·다른 구간 경계로)
측정된 "경과시간별 반납완료율" 표(0~5분 8.0% ~ 55~60분 94.1%, 5분 간격 12개
구간)도 있었는데, bin 경계와 집계 방식(구간 vs 단일 시점, station별 vs 전체)이
달라 숫자가 정확히 같지는 않지만 형태(0분 근처는 한 자릿수%, 60분 근처는
90%대)가 위 표와 완전히 일치한다 — 서로 다른 두 측정이 같은 현상을 가리키고
있음을 재확인했다. 이 문서의 표(위, `compute_completion_curve` 기준)가
`dev_completion_curve_integration.py`에 회귀 테스트로 고정돼 있는 쪽이라
공식 기준으로 유지한다.

---

## 6. 테스트 (2026-08 갱신 — 경로가 바뀌었고, feature_engine legacy pandas 테스트는 삭제됨)

```bash
cd ml
./training/.venv/bin/python -m pytest ../libs/ml_core/tests/dev_rolling_window_features.py -q
./inference/.venv/bin/python -m pytest inference/tests/dev_predict_single_rental_censoring.py inference/tests/dev_rental_censoring_cross_parity.py -q
```

`dev_features_rental_censoring.py`/`dev_completion_curve_integration.py`(옛
`feature_engine/legacy/` pandas 구현 전용 테스트)는 그 legacy 코드 자체가
삭제되면서 같이 없어졌다. 지금 남아 있는 것:

| 파일 | 위치 | 확인 내용 |
|---|---|---|
| `dev_rolling_window_features.py` | `libs/ml_core/tests/` | `add_censored_visibility`(개념 검증) + `censored_rolling_counts`/`lookup_count_at_ticks`(실제 채택 설계) — 창 진입/이탈 경계, 반납 지연 시 시야 게이팅, embargo+폭보다 느린 트립은 영구 제외, 배치·서빙 두 함수가 같은 결론을 냄 |
| `dev_predict_single_rental_censoring.py` | `ml/inference/tests/` | `_censored_rental_recent`이 `count_visible_in_window`와 일치, 윈도우 내 0건은 fallback 아님, 커버리지 밖은 fallback |
| `dev_rental_censoring_cross_parity.py` | `ml/inference/tests/` | 배치(`feature_engine/spark/build_features.py`)와 서빙(`predict_single.py`)이 같은 트립에 대해 `rental_lag_1h`를 정확히 같은 값으로 계산하는지 대조 — 3절이 요구하는 "핵심 필터 조건은 동일해야 함"을 코드로 검증 |

---

## 7. 다음 단계 후보 (2026-08 기준 갱신)

1. 기본 g20/r20/a20 모델과 g5/r5/a5 또는 g5/r5/a20 모델을 동일한 공통 5분 test set에서
   비교해 학습 비용 대비 5분 추론 성능을 검증한다. 현재 코드는 각 조합을 재현하고
   산출물을 격리하지만, 정확도 우열 자체는 별도 평가 대상이다.
2. `count_visible_in_window()`를 실제 서빙 모듈에서 import하거나 로직을
   포팅 — `predict_single.py`는 여전히 "실시간 서빙을 흉내내는 배치/CLI"이지
   상시 구동 서버가 아니다(§6 "실시간 트립 카운트 스토어" 참고, 이 저장소엔
   아직 그런 서버가 없다) — 여전히 미해결.
3. S3 파티션 저장으로 전환 — **완료**, collector가 Silver를 S3에 쌓고
   `feature_engine/spark/silver_source.py`가 그걸 직접 읽는다
   ([feature_engine/DESIGN.md](feature_engine/DESIGN.md) §7 참고).
4. 날씨·인구 등 다른 실시간 feature에도 비슷한 지연 관측 이슈가 있는지 점검 —
   날씨는 별도로 "관측 vs 예보" 문제가 있다는 게 밝혀져 처리됐다
   ([inference/DESIGN.md](inference/DESIGN.md) §3 "날씨는 lag와 다르게
   다룬다" 참고).
5. `width`/`embargo` 튜닝 — **완료**(60분/40분으로 조정, 아래 §8은 그 튜닝에
   쓰였던 스윕 인프라의 기록이며 지금은 삭제됨).

---

## 8. (2026-08 기준 삭제됨) embargo/window 하이퍼파라미터 스윕 — 당시 실험 인프라

**아래 절이 설명하는 `training/experiment_log.py`/`training/scripts/run_embargo_sweep.py`/
`models/experiments/` 인프라는 저장소에서 완전히 삭제됐다.** embargo/window
튜닝이라는 목적 자체는 달성됐고(7절 5번), 그 이후의 실험 기록/격리는
[MLFLOW_SETUP.md](MLFLOW_SETUP.md)의 MLflow 통합으로 대체됐다 — "나중에
MLflow 같은 정식 tracker로 옮기더라도"라고 예전에 적어뒀던 그 이관이 실제로
일어났다. 아래는 당시 그 스윕이 어떻게 동작했는지의 기록으로만 남긴다.

7절 5번("width/embargo 튜닝 필요성은 실제 모델 성능으로 평가해봐야 안다")을
실행하기 위해, `config.py`의 `ROLLING_WINDOW_MINUTES`/`EMBARGO_MINUTES`/`TICK_MINUTES`를
하드코딩 대신 **호출부에서 override 가능한 파라미터**로 열어뒀었다:

| 함수 | 새 파라미터 |
|---|---|
| `build_rolling_rental_features.build_rolling_rental_features()` | `window_minutes`, `embargo_minutes`, `tick_minutes`, `output_path`, `trips`(재로딩 방지용 캐시 주입) |
| `features.build_features()` / `_add_lag_rolling()` / `_add_rental_lag_rolling()` / `_rental_visible()` | `rolling_parquet_path` — None이면 챔피언 산출물(`config.ROLLING_RENTAL_FEATURES_PARQUET`) |
| `train_common.train_target()` / `station_categories_path()` / `load_station_dtype()` | `models_dir` — None이면 챔피언 경로(`config.MODELS_DIR`) |

인자를 안 주면 전부 기존(챔피언) 동작과 100% 동일했다 — 실험은 별도 경로에만
쓰고 챔피언 아티팩트는 절대 덮어쓰지 않는 원칙.

**`training/experiment_log.py`**: 실행 1건(파라미터 조합 + git sha/dirty + 평가 지표 +
아티팩트 경로)을 `models/experiments/manifest.jsonl`에 한 줄씩 append했다.

**`training/scripts/run_embargo_sweep.py`**: `window=60분, tick=5분`은 고정하고
`embargo` 후보(`[0,15,30,45]`분)마다 rolling 피처 재생성 → features 재생성 →
대여 모델 재학습을 반복해, 후보당 `models/experiments/rental_embargo{N}/`에
저장하고 manifest에 기록한 뒤 마지막에 비교표를 냈다. 후보당 ~25~30분(트립
로딩은 후보 간 공유해 1회만) — 4개 기준 약 2시간.

이 인프라는 향후 "EMR에서 최근 1개월 정확도 평가 → 기준 미달 시 재학습 → 그래도
미달이면 embargo/window 등을 스스로 조정하며 재학습"하는 자동화 루프의 재료가
된다는 취지로 만들어졌었다: 코드는 하나로 유지하고(파이프라인 폴더를 버전별로
복제하지 않음), 파라미터·모델·지표를 실행마다 버전 태그처럼 남겨서 챔피언-챌린저 비교와 승격
로직을 그 위에 얹는 구조다.
