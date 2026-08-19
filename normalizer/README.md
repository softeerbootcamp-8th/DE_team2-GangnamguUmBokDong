# normalizer

서울시 250m 격자 생활인구 베이스라인(`living_population_grid`)과 서울 주요 121개 핫스팟 실시간 인구(`population_realtime`)를 공간 교차(Spatial Merge)하여, 5분 주기의 실시간 250m 격자 인구(`living_population_normalized`)를 산출하는 정규화 모듈입니다.

---

## 1. 배경 및 도입 목적

- **격자 데이터의 한계**: 250m 격자 인구는 서울 전역을 촘촘히 커버하지만, 공표 지연으로 인해 실시간 인파 변화를 즉각 반영하지 못합니다.
- **실시간 POI 데이터의 한계**: 5분 단위로 수집되어 매우 신선하지만, 서울 시내 주요 121개 핫스팟 영역에만 국한되어 있습니다.
- **해결 방안 (공간 합성)**:
  1. 250m 격자의 전역 공간 커버리지와 121개 POI의 5분 주기 실시간성을 결합합니다.
  2. POI 핫스팟과 겹치는 격자는 실시간 인구 밀도로 가중 보정하고, 겹치지 않는 일반 격자는 베이스라인 인구를 그대로 유지합니다.

---

## 2. 파일별 역할

| 파일 | 역할 |
|---|---|
| `main.py` | CLI 진입점, 베이스라인 날짜 결정(`strict`/`latest`), 시간대(`TT`) 필터링 및 파이프라인 실행 |
| `grid.py` | 국가지점번호 `CELL_ID`를 EPSG:5179 좌표로 변환하고 250m 정사각 격자 폴리곤 생성 |
| `poi.py` | 121개 POI Shapefile 로딩, 위상 오류(`make_valid`) 복구, EPSG:5179 좌표계 변환 및 메모리 캐싱 |
| `merge.py` | `STRtree` 공간 조인, 면적 가중 밀도 합성, 연령·성별 재분배, 면적 내림차순 순차 갱신 |
| `station_master.py` | 대여소 master에 생활인구 250m `CELL_ID`(`grid_id`, STRtree 공간 조인)와 기상청 5km 격자(`weather_nx`/`weather_ny`, `core.weather_grid`)를 보강 |
| `storage.py` | S3 실버 읽기/쓰기 및 실행 메타데이터 Manifest JSON 저장 |

---

## 3. 공간 합성 알고리즘 및 산식

### ① 공간 교차 탐색 (`find_overlaps`)
- **1단계 (R-Tree)**: `STRtree` 공간 인덱스로 격자와 외접 사각형(Bounding Box)이 닿는 POI 후보군을 $O(\log N)$으로 고속 추출합니다.
- **2단계 (정밀 교차)**: 후보 POI들에 대해서만 정밀 Polygon `intersection()`을 수행하여 실제 겹치는 면적($m^2$)을 계산합니다.

### ② 면적 가중 밀도 보정 (`_update_density`)
격자 면적($62,500m^2$) 대비 POI와 겹친 면적 비율($w$)을 가중치로 선형 보간합니다:

$$D_{new} = (1 - w) \cdot D_{current} + w \cdot D_{poi}$$
$$\text{SPOP}_{new} = D_{new} \times 62,500m^2$$

### ③ 연령대 및 성비 재분배 (`_redistribute_ages`)
- **성비**: POI 실시간 관측치의 남녀 성비(`MALE_PPLTN_RATE`, `FEMALE_PPLTN_RATE`)를 적용하여 새 남녀 총인구를 결정합니다.
- **연령대**: 각 성별 내부의 14개 연령대(00~70세 이상) 분포는 기존 격자의 고유 비중을 유지하여 비례 배분합니다.

### ④ 다중 POI 중첩 시 적용 순서 및 원리 (`merge_cell`)
하나의 250m 격자에 여러 POI 핫스팟이 동시에 걸쳐 있는 경우, **면적 내림차순**으로 정렬하여 순차 적용합니다:

1. **넓은 면적 POI 인구 선반영**: 면적이 넓은 광역 POI의 전반적인 인파 밀도를 먼저 격자에 반영합니다.
2. **좁은 면적 POI 인구 후반영(희석 방지)**: 좁고 집중도가 높은 국소 POI의 실시간 밀도를 마지막에 덮어씀(Overwrite)으로써, 핵심 상권의 뾰족한 인파 특성이 넓은 권역의 평균값에 묻혀 희석되는 현상을 방지합니다.
3. **최종 성비 확정**: 해당 격자에 가장 밀접하고 구체적인 특성을 가진 **가장 작은 POI의 실시간 남녀 성비**를 격자의 최종 성비로 채택하여 연령대를 재분배합니다.

---

## 4. S3 입출력 경로 구조

```text
s3://<bucket>/
├── silver/
│   ├── living_population_grid/dt=YYYY-MM-DD/                   # [입력 1] 베이스라인 격자 인구
│   ├── population_realtime/dt=YYYY-MM-DD/hh=HH/HHMM.parquet    # [입력 2] 5분 실시간 POI 인구
│   ├── living_population_normalized/dt=YYYY-MM-DD/hh=HH/       # [출력] 5분 정규화 격자 인구
│   │   └── HHMM.parquet
│   │
│   ├── bike_station_master/dt=YYYY-MM-DD/hh=HH/HHMM.parquet    # [입력 3] 일 1회 대여소 master
│   ├── bike_station_realtime/dt=YYYY-MM-DD/hh=HH/HHMM.parquet  # [입력 4] 좌표·이름·거치대 수 보완용
│   └── station_master_enriched/dt=YYYY-MM-DD/hh=HH/            # [출력] 격자 보강 대여소 master
│       └── HHMM.parquet
│
└── _manifest/<source_id>/dt=YYYY-MM-DD/hh=HH/
    └── HHMM.json                                               # 실행 결과 메타데이터 (매칭 건수 등)
```

`_manifest`의 `<source_id>`는 파이프라인별로 `living_population_normalized` 또는
`station_master_enriched`다.

---

## 5. 대여소 마스터 격자 보강 (`station_master.py`)

`bike_station_master`(따릉이 API 원본)에는 격자 식별자가 없다. 인구·날씨 피처를 대여소에
붙이려면 격자 조인이 필요하므로, 하루 1회(`station_master` DAG) 미리 계산해
`station_master_enriched` Silver로 굳혀 둔다.

| 컬럼 | 타입 | 출처 | 결측 조건 |
|---|---|---|---|
| `station_id` | string | master `RNTLS_ID` | 없음(없는 행은 제외) |
| `station_no` | string | master `ADDR2` | `ADDR2` 없을 때 |
| `station_name` | string | 실시간 `stationName` → master `ADDR1` → `ADDR2` | 셋 다 없을 때 |
| `capacity` | int64 | 실시간 `rackTotCnt` | 해당 실시간 행이 없을 때 |
| `lat` | double | master `LAT` → (무효 시) 실시간 `stationLatitude` | 숫자 변환 실패 시. `0.0`은 그대로 실린다 |
| `lon` | double | master `LOT` → (무효 시) 실시간 `stationLongitude` | 위와 동일 |
| `grid_id` | string | 생활인구 250m `CELL_ID` (EPSG:5179 폴리곤 `STRtree` 공간 조인) | 좌표 무효 **또는** 어느 격자 폴리곤에도 속하지 않을 때 |
| `weather_nx` | int64 | 기상청 5km 격자 X (`core.weather_grid.latlon_to_grid`) | 좌표 무효일 때만 |
| `weather_ny` | int64 | 기상청 5km 격자 Y (같은 함수) | 좌표 무효일 때만 |

두 격자 컬럼의 결측 조건이 다르다. `weather_nx`/`weather_ny`는 좌표가 유효하면 순수 산술로
항상 채워지는 대신 운영 수집 격자(현재 34개) 밖의 번호일 수 있고, `grid_id`는 좌표가
유효해도 생활인구 baseline 커버리지 밖이면 `None`이다. 따라서 `weather_nx`가 채워진 행 수는
항상 `grid_id`가 채워진 행 수 이상이며, `MIN_GRID_COVERAGE = 0.95`(`grid_id` 매핑률) 게이트가
둘을 함께 보호한다 — 좌표가 대량으로 깨지면 두 격자가 같이 비므로 이 게이트에서 실패한다.

**좌표 유효성 판정**: `_valid_wgs84`가 위도 36.5~38.5, 경도 125.5~128.5를 요구한다. master의
`LAT`/`LOT`가 `0`인 대여소는 이 검사에서 걸러지므로 적도상 엉뚱한 격자가 계산되지 않는다.

---

## 6. 실행 방법

```bash
# 1. 의존성 설치
uv sync

# 2. 정규화 파이프라인 1회 실행 (기본 strict 모드: 당일 베이스라인 필수)
uv run python main.py --window-start 2026-08-15T14:05:00+09:00

# 최신 가용 파티션 폴백 모드 실행 (Airflow 재시도용)
uv run python main.py --window-start 2026-08-15T14:05:00+09:00 --baseline-date-mode latest

# 3. 대여소 마스터 격자 보강 1회 실행
uv run python station_master.py --window-start 2026-08-15T03:00:00+09:00 --baseline-date-mode latest

# 4. 단위 테스트 실행
uv run pytest
```
