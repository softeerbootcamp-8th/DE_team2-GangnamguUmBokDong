# 생활인구 Normalizer 구현

> **현재 구현:** `normalizer/`와 Airflow `realtime_tick`, `station_master` DAG가 사용하는
> 공간 정규화 경로다. 코드 확인일: 2026-08-24.

## 해결하는 문제

**전역을 덮는 생활인구는 늦고, 실시간 인구는 121개 주요 장소만 제공된다.**

Normalizer는 nowcaster의 250m 격자 baseline과 5분 단위 POI 실시간 인구를 EPSG:5179
공간 교차로 결합한다. 겹치지 않는 격자는 baseline을 유지하고, 겹치는 영역만 POI의
인구 밀도로 보정해 `living_population_normalized` Silver를 만든다.

## 두 실행 경로

| CLI | Airflow | 출력 |
| --- | --- | --- |
| `main.py --window-start` | `realtime_tick`의 `run_normalizer` | 현재와 향후 최대 12시간의 정규화 격자 |
| `station_master.py --window-start` | 일일 `station_master`의 `enrich_station_master` | 인구·기상 격자가 붙은 대여소 master |

Normalizer는 독립 uv project이며 Collector 내부 구현을 import하지 않는다. S3와 source
snapshot 공통 계약은 `libs/core`를 사용한다.

## 입력과 출력

```text
nowcaster nowcast.parquet ─┐
                           ├─ main.py ─→ living_population_normalized
population_realtime ───────┘

bike_station_master ──────┐
bike_station_realtime ────┼─ station_master.py ─→ station_master_enriched
latest successful nowcast ┘
```

- 현재·미래 정규화는 대상 날짜의 exact nowcast가 없으면 실패한다.
- Station master 보강은 정적인 CELL geometry만 필요하므로 미래 파일을 제외한 최신 성공
  nowcast를 사용할 수 있다.
- Collector 입력은 source snapshot authority가 가리키는 Parquet을 읽는다. EMPTY는
  실패하며, 현재 `population_realtime`만 검증된 exact PARTIAL fallback을 사용할 수 있다.

## 공간 계약

### 250m CELL

`CELL_ID`는 국가지점번호 형식이며 `grid.py`가 남서쪽 꼭짓점을 EPSG:5179로 변환해
`250m × 250m` Polygon을 만든다.

```text
X = 700,000 + 동서 문자 index × 100,000 + 동서 숫자 × 10
Y = 1,300,000 + 남북 문자 index × 100,000 + 남북 숫자 × 10
```

예시 회귀값은 `다사53815262 → (953810.0, 1952620.0)`이다.

### POI

`poi.py`는 repository의 121개 장소 Shapefile을 읽고 WGS84에서 EPSG:5179로 변환한다.
유효하지 않은 geometry는 `make_valid`로 복구하고, 결과가 여러 Polygon이면 가장 큰
조각을 사용한다. Polygon을 얻지 못하면 실패한다.

## 밀도 합성

`merge.py`는 STRtree로 후보를 좁힌 뒤 실제 intersection 면적을 계산한다.

```text
w = intersection_area / cell_area
new_density = (1 - w) × current_density + w × poi_density
new_population = new_density × cell_area
```

한 CELL에 여러 POI가 겹치면 POI 전체 면적 내림차순으로 적용한다. 넓은 권역을 먼저
반영하고 좁은 hotspot을 나중에 적용해 국소 밀도가 희석되는 것을 줄인다.

- 현재 시각: 마지막으로 적용된 POI의 성비를 사용하고, 성별 내부 연령 비율은 기존
  CELL 분포를 유지한다.
- 미래 시각: `FCST_n_*`에는 성비가 없으므로 총량만 보정하고 baseline의 28개 성·연령
  비율을 유지한다.
- 출력 수량은 deterministic rounding을 거친다.

## 시간 처리

- Living population의 `TT`는 공백 포함 문자열, 1·2자리 문자열 또는 정수를 0~23으로
  엄격히 정규화한다.
- 같은 `CELL_ID`, `TT`의 여러 `H_DNG_CD` component는 SPOP과 28개 연령 컬럼을 합산한다.
- 미래 target은 slot 번호가 아니라 `FCST_n_TIME` 실제 값을 사용한다.
- `FCST_YN != "Y"`, 필수 예측값 누락, 현재 이전 또는 12시간 초과 target은 제외한다.
- 여러 POI의 target 합집합을 정렬해 최대 현재+12시간 범위를 생성한다.
- 과거 실행이 더 최신 source window가 만든 미래 파일을 덮지 않도록 S3 generation
  metadata를 비교한다.

## Station master 보강

`station_master.py`는 다음 컬럼을 생성한다.

| 컬럼 | 계산 |
| --- | --- |
| `station_id` | Master `RNTLS_ID` |
| `station_no` | 정확한 `ST-<숫자>` suffix, 양의 int16 |
| `station_name` | Realtime 명칭 → master 주소 fallback |
| `capacity` | Realtime `rackTotCnt` |
| `lat`, `lon` | 유효한 master 좌표 → realtime fallback |
| `grid_id` | Station Point와 250m CELL Polygon 공간 조인 |
| `weather_nx`, `weather_ny` | `core.weather_grid.latlon_to_grid` |

WGS84 유효 범위는 위도 `36.5..38.5`, 경도 `125.5..128.5`다. `grid_id` coverage가
`MIN_GRID_COVERAGE=0.95`보다 낮으면 snapshot 전체를 실패시킨다. Weather grid는 산술
변환이므로 생활인구 coverage 밖에서도 값이 나올 수 있다.

## S3 경로

```text
silver/living_population_grid/dt=YYYY-MM-DD/hh=00/nowcast.parquet
silver/population_realtime/dt=YYYY-MM-DD/hh=HH/HHMM.parquet
silver/living_population_normalized/dt=YYYY-MM-DD/hh=HH/HHMM.parquet
silver/station_master_enriched/dt=YYYY-MM-DD/hh=HH/HHMM.parquet
_manifest/<output_source_id>/dt=YYYY-MM-DD/hh=HH/HHMM.json
```

Manifest에는 입력·출력과 매칭 수, 미래 target, 최신 generation 때문에 건너뛴 target 등
실행 근거를 기록한다.

## 실행과 검증

```bash
cd normalizer

uv run --frozen python main.py \
  --window-start 2026-08-24T09:05:00+09:00

uv run --frozen python station_master.py \
  --window-start 2026-08-24T03:00:00+09:00

uv run --frozen pytest -q
```

두 CLI는 timezone offset을 포함한 ISO 8601 시각을 요구한다. 실제 S3 쓰기까지 검증하려면
운영 bucket이 아닌 격리된 test object store를 사용한다.
