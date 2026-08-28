# Feature Engine 설계

> 현재 상태: **운영 코드와 일치**
>
> 기준 구현: `ml/feature_engine/spark/`
>
> 실행 방법: [feature_engine README](../../../ml/feature_engine/README.md)

Feature Engine은 확정된 Archive fact와 최신 정류소 dimension을 읽어
`station × model tick` 피처를 만들고, 이를 대여·반납 multi-horizon 학습 테이블로
확장한다. 현재 구현은 Spark 전용이다. 삭제된 pandas/로컬 전처리 과정과 과거 실험은
[history.md](../history.md)에만 기록한다.

## 1. 책임과 경계

```text
Archive fact + current Silver station dimension
                 │
                 ▼
        spark.run_pipeline
  원천 재집계 → station×tick 병합 → lag/target 생성
                 │
                 ▼
       horizon=1 tick feature table
                 │
                 ▼
 spark.build_multi_horizon_features
                 │
        ┌────────┴────────┐
        ▼                 ▼
  rental train mart  return train mart
```

두 단계는 독립 실행한다. multi-horizon self-join은 행 수가 최대
`HORIZON_COUNT`배 증가하므로 base feature 재생성과 최종 학습 mart 생성을 분리했다.

## 2. 입력 데이터 계약

| 용도 | 실제 입력 |
|---|---|
| 정류소 속성·격자 | 최신 `silver/station_master_enriched/dt=.../hh=.../HHMM.parquet` |
| 대여·반납 이력 | `archive/bike_rental_history/dt=YYYY-MM-DD.parquet` |
| 정류소 재고 | `archive/bike_station_realtime/dt=YYYY-MM-DD.parquet` |
| 날씨 관측 | `archive/weather_ultra_short_live/dt=YYYY-MM-DD.parquet` |
| 생활인구 | `archive/living_population_grid/dt=YYYY-MM-DD.parquet` |

과거 fact는 날짜별 Archive 파일만 읽고 Silver fallback은 하지 않는다. 요청 범위 중
일부 날짜 partition만 없으면(수집 공백 등) 그 날짜만 건너뛰고 경고를 남긴 채
계속하며, 요청 범위 **전체**가 다 없거나 스키마가 불일치할 때만 실패한다(월간
재학습이 하루치 결측으로 통째로 실패하지 않도록 2026-08에 완화됨,
`feature_engine/spark/silver_source.py::_read_archive_daily`). 정류소 마스터만
historical snapshot이 아닌 최신 current dimension을 사용하므로 과거 좌표·거치대 수
변경을 시점별로 복원하지 못한다.

`processed_v2/`는 로컬 입력 폴더가 아니라 Spark가 원천을 재집계해 쓰는 S3 중간
prefix다. source ID와 컬럼 매핑의 단일 기준은 `libs/ml_core/silver_schema.py`다.

## 3. 시간축과 타겟

base 테이블의 논리 grain은 `(station_no, hour_ts)`다. `hour_ts`는 정시만 뜻하지
않으며 `GRID_TICK_MINUTES` 간격의 model tick을 담는다.

| 설정 | 내장 기본값 | 의미 |
|---|---:|---|
| `GRID_TICK_MINUTES` | 20분 | base feature/target grid |
| `ROLLING_TICK_MINUTES` | 20분 | rolling 계산 anchor 간격 |
| `TRAIN_ANCHOR_TICK_MINUTES` | 20분 | 학습에 남길 anchor 간격 |
| `TARGET_HORIZON_MINUTES` | 60분 | 한 라벨의 구간 `[T, T+60분)` |
| `HORIZON_COUNT` | 12 | 최대 12시간 ahead |

grid는 `{5, 10, 15, 20, 30, 60}`분만 지원하며 rolling tick과 같아야 한다.
학습 anchor는 base grid 이상의 배수이고 한 시간과 하루를 나눠야 한다. 온라인 추론
호출 주기 5분은 학습 grid와 별개의 계약이다.

정류소 재고가 관측된 활성 구간만 grid로 펼친다. 운영하지 않은 구간을 수요 0으로
학습하지 않으며, 정확히 한 시간 전 행이 없으면 다른 과거 행으로 대체하지 않고
lag를 null로 둔다.

## 4. Point-in-time 피처

대여 이력은 반납 완료 후 Archive에 나타날 수 있다. 학습 시점에 보이는 최신 트립을
그대로 세면 온라인 시점에는 알 수 없던 대여까지 사용하게 된다.

- `rental_lag_1h`: `[T-embargo-window, T-embargo)`에 시작했고 `end_dt <= T`인
  트립만 센다. 기본값은 window 60분, embargo 40분이다.
- `return_lag_1h`: 반납 시각 자체가 이벤트 확정 시각이므로 직전 1시간 반납을 센다.
- weather: collection tick별 서울 격자 평균을 다음 관측 전까지 과거 방향으로만
  채운다. 최대 stale 허용치는 3시간이며 window 시작 전 3시간도 조회한다.
- target: `[T, T+60분)` 전체가 데이터 경계 안에서 완결되는 tick만 남긴다.

모델 feature의 단일 기준은 `libs/ml_core/common_config.py`의
`BASE_FEATURE_COLUMNS`와 `libs/ml_core/model_contract.py`다.

| 공통 feature | 모델별 feature |
|---|---|
| `station_no`, `capacity`, `lat`, `lon`, `temp`, `precip`, `pop_total`, `minute`, `dow`, `is_holiday`, `day`, `horizon` | 대여: `rental_lag_1h` / 반납: `return_lag_1h` |

`station_id`, `hour`, `date`는 식별·조회·split용이며 모델 feature는 아니다. 과거의
`lag_24h`, `lag_168h`, 여러 rolling mean/std 컬럼도 현행 모델에는 없다.

## 5. Multi-horizon 구성

한 행의 `anchor_ts=T0`에 대해 horizon `h`의 target 시각은
`T0 + (h-1)시간`이다.

- lag는 anchor 시점 값으로 고정한다.
- 날씨·인구·캘린더·라벨은 target 시점 값을 붙인다.
- target 시점 행이 없으면 inner join 결과에서도 제외한다.
- `date`는 target 시점에서 가져와 라벨 발생일 기준으로 split한다.
- 대여와 반납은 상대 모델의 lag를 사용하지 않고 별도 테이블로 분리 생성한다 (`--models rental` / `--models return`으로 원하는 모델만 선택 생성 가능).
- 12개 horizon 병합은 균형 이진트리(Balanced Binary Tree) Union을 사용하여 Catalyst 최적화 깊이를 $O(N)$에서 $O(\log N)$으로 단축한다.
- `date` 파티션 내부는 `(date, anchor_ts, station_no, horizon)`으로 정렬(`sortWithinPartitions`)하여 Snappy 압축률과 읽기 I/O를 최적화한다.

horizon=1에서는 `anchor_ts == target_ts`이며 base tick 테이블과 값이 같아야 한다.
최종 테이블은 `date`로 repartition한 뒤 날짜별 Parquet partition으로 overwrite한다.

## 6. 전체 빌드와 증분 보정

`run_pipeline.py`와 `build_multi_horizon_features.py`는 피처 파라미터 조합별 watermark를 관리한다.

- **사전 Freshness 스킵**: 워터마크의 `updated_at`이 최근 24시간 이내이고 데이터가 최신 윈도우까지 채워져 있으면 무거운 Spark 계산을 건너뛴다.
- watermark가 없거나 `TRAIN_WINDOW_START`와 `TRAIN_WINDOW_END`를 명시하면 전체
  overwrite한다.
- 그 외에는 watermark에서 `INCREMENTAL_LOOKBACK_HOURS`만큼 되돌아가 날짜
  자정으로 내린 뒤 해당 날짜 partition을 다시 계산해 overwrite한다.
- 계산 시작점보다 하루 앞의 context도 읽어 경계 부근 lag/rolling을 보존한다.
- 구버전 flat Parquet가 날짜 partition과 섞여 있으면 즉시 실패한다.

기본 lookback은 840시간(35일)이다. 장시간 대여가 뒤늦게 반납되면 이미 발행된 과거
`rental_count`가 바뀔 수 있으므로 append가 아닌 재계산이 필요하다. 학습의 최신 데이터
제외 기준은 별도 `TRAINING_SAFETY_MARGIN_DAYS` 계약을 따른다.

산출물은 `processed/features/{FEATURE_PARAM_COMBO_ID}`로 window·embargo·tick 조합을
격리하고, 최종 mart는 다시 `training_anchor_a{N}`으로 분리한다. 경로 공식은
`feature_engine/spark/config.py`와 `libs/ml_core/paths.py`가 동일해야 한다.

## 7. Spark 구현 원칙

- timestamp 계산은 행 순서가 아닌 실제 경과시간 기준이다.
- Spark session과 JVM timezone은 모두 `Asia/Seoul`로 맞춘다.
- `timestamp_ntz` 변환은 `rolling_window_features.py`의 전용 helper를 사용한다.
- historical fact는 S3A 경로로만 읽으며 로컬 파일 fallback을 두지 않는다.
- `ML_PROFILE` 미지정 시 내장 프로필을 사용하고, 명시한 원격 프로필이 없거나
  유효하지 않으면 fail-closed한다.
- 다중 액션이 호출되는 중간 DataFrame(`features_increment`, `anchor`, `target`)은 명시적 `.cache()` 및 완료 후 `.unpersist()`/`clearCache()`로 셔플 I/O와 메모리를 최적화한다.


## 8. 검증 기준

```bash
cd ml
./feature_engine/.venv/bin/python -m pytest \
  feature_engine/tests/dev_silver_source.py \
  feature_engine/tests/dev_spark_rolling_parity.py \
  feature_engine/tests/dev_spark_build_features.py \
  feature_engine/tests/dev_spark_incremental.py \
  feature_engine/tests/dev_spark_multi_horizon_parity.py -q
```

변경 시 다음 불변조건을 유지한다.

1. censored rolling의 기준 구현과 Spark 결과가 같다.
2. 증분 재계산 결과가 같은 범위의 전체 재빌드와 같다.
3. 늦게 도착한 트립이 과거 날짜 partition에 반영된다.
4. horizon=1 결과가 base tick feature와 같다.
5. Archive 요청 범위 전체 누락·스키마 오류·경로 불일치는 fallback 없이 실패한다
   (요청 범위 중 일부 날짜만 없는 경우는 실패가 아니라 건너뛰고 계속한다 — §2 참고).
