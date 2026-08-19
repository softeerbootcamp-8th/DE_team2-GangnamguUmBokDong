# feature_engine — 설계 문서

실행 방법은 [README.md](../../../ml/feature_engine/README.md), 원본 데이터 상세는 [DATA_CATALOG.md](../DATA_CATALOG.md),
결정의 배경/시행착오는 [history.md](../history.md)를 참고. 이 문서는 "지금 코드가
왜 이렇게 짜여 있는지"에 집중한다.

**파일 위치 안내(2026-08 갱신)**: 아래 설계 설명이 언급하는 파일명
(`build_station_master.py`/`build_merged_table.py`/`build_rolling_rental_features.py`/
`features.py`/`config.py`/`grid.py` 등)은 원래 pandas 구현 기준으로 쓰였다.
**`feature_engine/legacy/`(pandas 1차정제 + 옛 2차정제)는 저장소에서 완전히
삭제됐다** — 옮겨진 게 아니라 없어졌다. collector가 S3 Silver에 원본을 직접
쌓게 되면서, 1차정제에 해당하던 작업(정류소 마스터/타겟/재고/날씨/인구 재집계)
자체를 `feature_engine/spark/silver_source.py` + `run_pipeline.py`의
`_refresh_primary_tables()`가 Spark로 매 실행마다 다시 만든다 — 별도의 pandas
1차정제 단계 자체가 없어졌다. **본 서비스 코드는 여전히 `feature_engine/spark/`뿐**이다.
아래 설계 의도/로직은 대체로 그대로 유효하지만(2차정제 핵심 알고리즘은 parity
테스트로 검증된 그대로), 파일 경로는 전부 `feature_engine/spark/` 기준으로
읽을 것 — pandas 파일명이 나오면 그 로직이 지금은 어느 Spark 모듈에
대응하는지 [feature_engine/README.md](../../../ml/feature_engine/README.md)의
표를 참고. 5분 tick/피처 목록 등 구체적인 수치는 이후 세션에서 피처 축소와
프로필화로 또 한 번 바뀌었다 — 이 문서에서 그 변경을 반영한 부분은
"(2026-08 갱신)"으로 표시해뒀고, 나머지(그리드 설계 이유, dtype 다운캐스트
근거, Spark 포팅 함정 등 인과관계 설명)는 여전히 유효하다.

## 0. 초기 데이터 감사 및 스코프 결정

(구 `ANALYSIS.md`에서 이관 — 이후 나온 결정들의 전제가 되는 가장 초기 스코프
확정 내용이라 여기 남긴다. 그 이후의 결정들은 [history.md](../history.md)를 참고.)

**0.1 초기 감사에서 발견한 문제**: `data/`에 있던 실데이터를 원 설계와
대조해보니 소스별 커버 기간이 제각각이었다 — 대여이력은 2024-01~2026-06(30개월,
트립 단위)인데 정류소 재고 스냅샷·날씨 관측·생활인구는 전부 2025년 한 해만
있었다. 추가로 "날씨 예보" 데이터(`data/raw_forecast/`)가 실제로는 관측(ASOS)
데이터의 중복본이라 서빙 시점 예보치가 없었고, 생활인구가 행정동 단위인데
정류소→행정동 매핑 테이블이 없었다.

**0.2 사용자 결정 사항**: 위 문제들을 확인해 스코프를 확정했다 — ① 학습 기간을
4개 소스가 모두 겹치는 유일한 구간인 2025-01-01~12-31로 한정, ② 생활인구는
행정동 대신 250m 격자 원본(`data/raw_people/250m/`)을 직접 사용, ③ 날씨는
예보 없이 관측치로 학습/추론 모두 진행(train-serve skew는 알려진 한계로 남김,
예보 API 연동은 후속 과제). **(2026-08 갱신) ③은 이제 절반만 유효하다** —
학습은 지금도 target_ts 시점의 실제 관측 날씨(ground truth)로만 배운다(추론
시점엔 미래라 실측이 없어 학습과 근본적으로 다르게 다뤄야 함). 추론 쪽은
horizon>1(미래 시점)이면 예보(`weather_short_term_forecast`)를 먼저 시도하고
없으면 관측으로 fallback하도록 바뀌었다 — `inference/DESIGN.md`의 "날씨" 절
참고. 다만 collector의 예보 자동 수집 스케줄이 아직 없어(수동 트리거만
가능) 실제로는 여전히 관측 fallback을 타는 경우가 많다.

**0.3 250m 격자 생활인구 통합**: KT 데이터매뉴얼을 읽고 격자 ID(`다사52255325`
형태, 행정안전부 국가지점번호 체계)를 EPSG:5179 좌표로 직접 역산하는 공식을
찾아 `grid.py`(옛 pandas 1차정제 코드 — 지금은 삭제됨, 격자 좌표 역산 공식
자체는 재구현 시 참고용 기록으로만 남김)에 구현(행정동 매핑·shapefile 불필요, 매뉴얼 예시값으로
self-test 검증). 그 외 주의점: 집계는 "그 시간대에 가장 오래 체류한 격자"
기준 1시간당 1명(체류인구와는 다른 개념), 집계값 3 이하는 K-익명성 마스킹
(`"*"` → 2로 대체, KT 예시 코드와 동일), 정류소가 실제로 속한 격자(2,273개)로
먼저 필터링 후 concat(전체 격자 10,021개×365일×3종을 다 로드하면 감당 불가).

**0.4 파이프라인 모듈 및 스키마 함정** (`feature_engine/scripts/run_build_pipeline.py`가
아래 순서로 `build_*.py`를 실행):

| 단계 | 발견한 문제 | 해결 |
|---|---|---|
| `build_station_master.py` | 정류소 마스터 CSV가 헤더 2줄(한글/영문 코드) 구조 | `skiprows=[1]`로 영문 코드 행 스킵 |
| `build_targets.py` | 대여이력 parquet는 컬럼명이 `start_st`/`end_st`(5자리 대여소번호)이고 0-padding이 불일치(`"2183"` vs `"02191"`), 반납 미완료 트립은 `"\N"` 문자열 | `station_master.station_no`(zfill(5)) 크로스워크로 매칭 |
| `build_station_status.py` | `거치대수량` 컬럼명과 달리 실제로는 그 시각 주차된 자전거 수(시간별 변동)이지 capacity가 아님 | 반납이 거치대 상태와 무관하게 항상 성공해 재고가 capacity를 초과하는 overflow가 실제로 관측됨(버그 아님) |
| `build_weather.py` | 서울 전체가 ASOS 단일 관측소(108) 값 공유 | station과 무관하게 hour 기준 broadcast join |
| `build_population.py` | 0.3절 참고 | 격자 필터링 후 concat, 마스킹 대체, 중복 합산 |
| `build_merged_table.py` | 소스별로 station 커버리지가 다름 | "2025년 트립 1건 이상 있었던 정류소"만 활성 station으로 채택 |

모든 원본 인코딩은 cp949(`data/raw*`)이고, 이미 UTF-8로 디코딩된
`data/processed/utf8_*`만 예외 — 놓치면 한글 컬럼명이 깨진다.

**0.5 최종 병합 테이블 및 검증**(당시 시간 단위 그리드 기준, 이후 §1에서 tick
단위 그리드로 전환 — 지금은 5분): 2,582개 활성 정류소 × 8,760시간 = 22,618,320행. `rental_count`/
`return_count` 합계가 원본 트립 집계와 정확히 일치, 재고 스냅샷 매칭률 98.91%,
생활인구 매칭률(pop_total>0) 99.69% — station 수(2,582)가 기존 EDA의
전체 정류소 수(2,835)보다 적은 건 "2025년 트립 1건 이상"만 포함해서다(정상).

## 1. 그리드 — station × 5분 tick

`build_merged_table.py`가 만드는 그리드의 각 행은 `(station_id, hour_ts)`이고
`hour_ts`는 `GRID_TICK_MINUTES`(운영 계약 **5분**, 단일 소스는
`libs/ml_core/profile_contract.py`) 단위 tick이다(컬럼명은 하위 호환을 위해 그대로 유지 — 항상
정시라고 가정하면 안 됨). 두 가지 이유가 겹쳐서 시간 단위가 아니라 tick
그리드를 쓰게 됐다:

- **임의 시각 예측**: "3시 40분 기준 앞으로 1시간" 같은 예측을 하려면 타겟/그리드
  자체가 그 해상도를 가져야 한다. `build_targets.py`의 `future_rolling_counts()`가
  "[T, T+1시간) 시작 건수"를 tick마다 계산하는 sparse step function을 만든다.
- **station 생애주기**: `station_status`(재고 스냅샷)에 실제 관측이 있는 시간만
  tick으로 펼쳐서 그리드로 쓴다 — 폐쇄/휴업 구간은 그리드에 아예 안 들어가서
  "서비스 없음"이 "수요 0"으로 잘못 학습되지 않는다.

sparse 타겟/rolling 카운트를 그리드의 특정 tick에서 조회하려면
`ml_core.rolling_window_features.lookup_count_at_ticks()`를 쓴다 — "그 tick 이하
중 가장 최근 delta 이후의 값"을 찾는 as-of 조회다.

## 2. lag/rolling — 시간 기준(gap-aware), tick 밀도 무관

그리드가 tick 단위(시간당 여러 행, 현재 5분=시간당 12행)가 되면서
"N번째 이전 행 == N시간 전"이 더 이상 성립하지 않는다(예전 시간 단위
그리드에서는 항상 dense해서 성립했음). 그래서:

- **lag**: `_exact_hour_lag()`(pandas)/self-join(Spark) — "정확히 N시간 전 tick"을
  찾는다. 그 tick이 그리드에 없으면(station 휴업 구멍) null — 조용히 엉뚱한 시점
  값을 가져오지 않는다.
- **rolling**: pandas는 `groupby().rolling("Nh", on="hour_ts")`(시간 오프셋 윈도우),
  Spark는 `rangeBetween`(실제 경과초 기준) — 둘 다 행 개수가 아니라 실제 경과
  시간으로 윈도우를 잰다. **의도적으로 "dense"를 선택**했다: 윈도우 안의 모든
  tick을 평균한다(N개 시간별 지점만이 아니라) — 인접 tick끼리 창이 겹쳐 사실상
  스무딩에 가깝다.

`feature_engine/spark/build_features.py`는 애초에 self-join/rangeBetween 기반으로
짜여 있어서 tick 밀도 변화의 영향을 받지 않는다 — station 휴업으로 인한 그리드
구멍(과거 문제)과 tick 밀도(새 문제)가 같은 해법(행 개수 대신 실제 시간 기준)으로
동시에 해결된다.

## 3. 대여(rental) point-in-time censoring

대여는 반납이 완료돼야 로그에 잡힌다(대여 시작 시점엔 안 잡힘) — 그래서 "직전
1시간 대여량" 같은 피처를 raw 값으로 만들면 학습 데이터(몇 달~몇 년 뒤 전부 반납
완료)와 서빙 시점(방금 지난 데이터의 4~8%만 로그에 보임)의 분포가 어긋난다
(train-serving skew). `build_rolling_rental_features.py`가 `[T-embargo-window,
T-embargo)` 윈도우(기본 프로필: window 60분, embargo 40분 —
"100분 전~40분 전")로 그 시점에 실제로 관측 가능했던 값만 계산해서
(`ml_core.rolling_window_features.censored_rolling_counts()`), `features.py`가
그 값을 `rental_lag_1h`(대여 모델의 유일한 lag 피처)로 대체한다. 반납은 반납
이벤트 자체가 로그 시점이라 이 문제가 없어서 raw 값(`return_lag_1h`)을 그대로
쓴다. 자세한 설계는 [REALTIME_FEATURES.md](../REALTIME_FEATURES.md).

**(2026-08 갱신) `roll_mean/std_3h·24h`, `lag_24h/168h` 등 나머지 lag/rolling
피처는 전부 제거됐다** — 지금 모델 feature는 대여/반납 각각
`rental_lag_1h`/`return_lag_1h` 딱 1개씩뿐이다(피처 중요도 분석 후 대폭
축소, 현재 전체 피처 목록의 단일 소스는 `libs/ml_core/common_config.py`의
`BASE_FEATURE_COLUMNS` + `libs/ml_core/model_contract.py`). 그래서 이 섹션이
언급하는 "여러 lag/rolling 컬럼을 관리하는" 인프라(프로필의 `LAG_HOURS`/
`ROLLING_WINDOWS` 목록 등)는 더 이상 없다 — 한쪽당 컬럼 1개만 하드코딩돼 있다.

## 4. 메모리 — 배치 처리와 dtype 다운캐스트

268M행(2025년 전체, 이 섹션이 작성될 당시의 tick 그리드 기준 — 이후 tick
간격이 바뀌어 정확한 행 수는 달라졌다) 규모에서 로컬 머신(RAM 18GB)이 겪은
두 문제와 해법:

- **`build_features()`를 전체에 한 번에 돌리면 SIGKILL** — station이 25개씩
  독립적으로 계산 가능(lag/rolling이 station별 groupby)하다는 점을 이용해
  `build_features_chunked()`가 배치 단위로 디스크에서 읽고 part 파일로 나눠 쓴다.
  배치 실패해도 이미 끝난 part는 건너뛰고 재시작 가능.
- **원시 float64/int64가 과함** — `NATIVE_COLUMN_DTYPES`(build_merged_table.py)가
  값 범위 실측 기반으로 float32/int8/int16으로 다운캐스트. 학습 세트 원시 행렬
  기준 약 66GB → 약 29GB.

## 5. Spark 포팅 — pandas와 반드시 일치해야 하는 부분

`feature_engine/spark/`는 EMR 배포용 별도 구현이다(pandas와 서로 import 안 함).
핵심 규칙이 조용히 갈라지지 않도록 대조 테스트(`tests/dev_spark_rolling_parity.py`)가
합성 데이터로 pandas/Spark 결과를 비교한다. 포팅 중 겪은 문제:

- **merge_asof 없음** → Spark는 Window의 `last(ignorenulls=True)`(forward-fill)로 대체.
- **timestamp_ntz vs timestamp(tz-aware) 비대칭**: `F.unix_timestamp()`는
  `timestamp_ntz` 입력을 세션 타임존과 무관하게 항상 UTC로 해석하지만,
  `F.timestamp_seconds()`로 되돌릴 땐 세션 타임존을 쓴다 — 이 비대칭 때문에
  세션 타임존이 UTC가 아니면(이 프로젝트는 KST) 초 단위 왕복 변환이 조용히
  어긋난다. `feature_engine/spark/rolling_window_features.py`의
  `_unix_seconds_ntz()`/`_seconds_to_ntz()`가 `timestampadd()`(순수 wall-clock
  연산, 타임존 변환 자체가 없음)로 이 문제를 근본적으로 없앴다 — 세션
  타임존이 무엇이든(KST든 UTC든) 정확하다. 초 단위 정수 ↔ 타임스탬프 왕복이
  필요한 새 Spark 코드는 반드시 이 두 헬퍼를 쓸 것.
- **JVM/세션 타임존은 반드시 서로 같은 값(KST)으로 고정** — `spark_session.py`가
  `TZ=Asia/Seoul` env(SparkSession 생성 **전**)와
  `spark.sql.session.timeZone=Asia/Seoul`을 같이 설정한다.

## 6. 하이퍼파라미터 프로필

censoring 윈도우/LightGBM 파라미터 등은 `ml_core/common_config.py`가
`ML_PROFILE` 환경변수(기본 `default`)로 `ml_core/profiles/{이름}.json`을 읽어
제공한다. 개별 환경변수(`ROLLING_EMBARGO_MINUTES=45` 등)는 프로필 값 위에
추가로 덮어쓸 수 있다 — 상세는 [ml_core README](../../../libs/ml_core/README.md).

## 7. Silver 소스 전환 + horizon-as-feature로 대여/반납 테이블 분리 (2026-08 신규)

이 문서의 §0~§5는 원래 로컬 pandas 1차정제(`data/processed_v2/*`) 산출물을
전제로 쓰였다. 지금은 그 1차정제 단계 자체가 없다 — collector가 S3
Silver(`silver/station/`, `silver/bike_rental_history/`,
`silver/bike_station_realtime/`, `silver/weather_ultra_short_live/`,
`silver/living_population_grid/`)에 원본을 쌓고,
`feature_engine/spark/silver_source.py`가 그걸 직접 읽어 §0.4의 `build_*.py`가
하던 재집계를 매 `run_pipeline.py` 실행마다 Spark로 다시 한다("processed_v2"는
이제 그 중간 산출물이 놓이는 S3 키 prefix 이름으로만 남음).

날씨 중간 산출물은 시간별 최신값 하나가 아니라 실제 collection tick별 유효 서울
격자 평균을 보존한다. 병합 단계는 weather만 `lead`/`sequence`로 5분 tick에 먼저
펼쳐 다음 관측 직전 또는 최대 3시간까지 과거 방향으로 채운 뒤 station grid와
exact join한다. 최신 tick을 같은 시간 전체에 붙이지 않으므로 미래 관측 누수가
없고, 최대 3시간 lookback하는 inference와 freshness 경계도 같다. window 첫 tick을
위해 weather source만 시작점 이전 3시간 context를 읽되 최종 feature window는
`[since, until)` 그대로다.

또한 최종 산출물이 테이블 1개(`station_hour_features_2025.parquet`)가
아니라 **대여/반납 각각의 multi-horizon 테이블 2개**다 —
`build_multi_horizon_features.py`가 tick 단위 테이블(horizon=1)의 각 행을
"지금(anchor_ts)"으로 놓고 horizon=1..`HORIZON_COUNT`(기본 12, 1시간
단위)만큼 self-join해 확장한다: lag(`rental_lag_1h`/`return_lag_1h`)는
anchor_ts 기준으로 고정하고, 날씨/인구/캘린더/타겟 카운트는 그 미래
target_ts(anchor_ts+(horizon-1)시간) 기준 실제 값을 쓴다 — 별도 모델을
horizon마다 두거나 예측을 재귀적으로 다음 입력에 먹이는 대신, "horizon"
자체를 평범한 입력 feature로 모델에 알려주는 방식이다. 대여/반납은 서로
상대방의 lag를 안 보므로 이 self-join과 결과 테이블 모두 완전히 분리해서
순차로 만든다(원본의 최대 `HORIZON_COUNT`배 행 수로 불어나는 무거운
연산이라, 둘을 동시에 메모리에 안 띄우려는 목적도 있음).
