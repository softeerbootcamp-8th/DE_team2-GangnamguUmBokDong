# collector 초기 로드(bootstrap) 구현 계획

> **보관 문서:** bootstrap을 task 단위로 구현하던 당시 계획이다. 현재 사용법과 동작은 `collector/bootstrap/__main__.py`, 현재 설정은 `collector/bootstrap/mappings/*.yaml`을 기준으로 한다.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** collector 배포 이전 기간의 `archive/`를 과거 CSV와 과거 조회 API로 한 번 채우는 1회성 부트스트랩을 만든다.

**Architecture:** `collector/bootstrap/` 패키지가 입력 방식(`csv` / `history_api`)을 플러그로 갖고, 날짜 단위로 원시 행을 만들어 collector의 `validate_batch()`로 검증한 뒤 `compaction.archive_schema()`/`conform()`으로 스키마를 맞춰 `archive/{source_id}/dt=YYYY-MM-DD.parquet`에 쓴다. 재개는 archive 존재 여부로 판단한다.

**Tech Stack:** Python 3.11, pydantic v2, pyarrow, httpx, pytest + moto

설계 근거는 `docs/collector/bootstrap-design.md`에 있다. 판단이 갈렸던 지점의 이유가 궁금하면 그쪽을 본다.

## Global Constraints

- 작업 디렉터리는 `collector/`다. 테스트는 `cd collector && uv run --frozen pytest`로 돌린다.
- `collector/pyproject.toml`이 `pythonpath = ["."]`이므로 임포트는 `from bootstrap.config import ...` 형태다.
- 소스 설정(`collector/sources/*.yaml`)은 **건드리지 않는다**. bootstrap 설정은 `collector/bootstrap/mappings/`에 따로 둔다.
- `collector/tests/conftest.py`의 `_s3_env`·`_bucket` 픽스처가 autouse다. `collector/tests/` 아래 테스트는 moto S3를 자동으로 받는다. 버킷 이름은 `tests.conftest.TEST_BUCKET`.
- 커밋 메시지는 `type: 한글 설명` 형식이다(`fix:`, `feature:`, `docs:`, `refactor:`). Co-Authored-By 트레일러를 넣지 않는다.
- 새 파일에는 이 레포 관례대로 한글 docstring을 단다. "무엇을"보다 "왜"를 적는다.
- 시간대는 KST(`ZoneInfo("Asia/Seoul")`) 하나만 쓴다.

---

## File Structure

| 파일 | 책임 |
|---|---|
| `collector/compaction.py` (수정) | `_META_FIELDS`에 `_source_kind` 추가, compaction이 `"collector"` 주입 |
| `collector/storage.py` (수정) | `archive_exists()` 추가 |
| `collector/bootstrap/config.py` | 매핑 설정 스키마와 로더 |
| `collector/bootstrap/csv_source.py` | CSV 한 번 훑어 날짜별 Arrow 테이블로 분리 |
| `collector/bootstrap/api_source.py` | 과거 조회 API를 시간 단위로 병렬 호출 |
| `collector/bootstrap/runner.py` | 날짜 루프·검증·archive 적재·manifest |
| `collector/bootstrap/__main__.py` | CLI |
| `collector/bootstrap/mappings/*.yaml` | 소스별 매핑 설정 |

---

### Task 1: `_source_kind` 메타 컬럼

archive에 compaction 산출물과 bootstrap 산출물이 섞이는데, `_window_start`의 의미가 서로 다르다(수집 시각 vs 발생 시각). 구분 수단을 먼저 넣는다. 나중에 붙이면 이미 쌓인 파일을 전부 다시 써야 한다.

**Files:**
- Modify: `collector/compaction.py:64` (`_META_FIELDS`), `compact_date` 내부
- Modify: `collector/storage.py` (`archive_exists` 추가)
- Test: `collector/tests/test_compaction.py`, `collector/tests/test_compaction_run.py`, `collector/tests/test_archive_storage.py`

**Interfaces:**
- Consumes: 없음
- Produces:
  - `compaction.SOURCE_KIND_COLLECTOR: str = "collector"`
  - `compaction.SOURCE_KIND_BOOTSTRAP: str = "bootstrap"`
  - `archive_schema(config)`가 `_source_kind`(string)를 포함한 스키마 반환
  - `storage.archive_exists(source_id: date) -> bool`

- [ ] **Step 1: 실패하는 테스트를 쓴다**

`collector/tests/test_compaction.py`의 `TestArchiveSchema`에 추가:

```python
    def test_source_kind_is_part_of_the_schema(self):
        """archive에 두 출처가 섞이므로 구분 컬럼이 스키마에 있어야 한다."""
        config = _config(columns={"a": ColumnSpec(types=("str",))})

        schema = archive_schema(config)

        assert schema.names == ["a", "_row_status", "_window_start", "_source_kind"]
        assert schema.field("_source_kind").type == pa.string()
```

`collector/tests/test_archive_storage.py`에 클래스 추가:

```python
class TestArchiveExists:
    def test_false_when_absent(self):
        assert storage_module.archive_exists("test_source", DAY) is False

    def test_true_after_write(self):
        write_archive("test_source", DAY, _table())

        assert storage_module.archive_exists("test_source", DAY) is True
```

같은 파일 임포트에 `import storage as storage_module`을 추가한다.

- [ ] **Step 2: 실패를 확인한다**

Run: `cd collector && uv run --frozen pytest tests/test_compaction.py::TestArchiveSchema::test_source_kind_is_part_of_the_schema tests/test_archive_storage.py::TestArchiveExists -v`
Expected: FAIL — 스키마에 `_source_kind`가 없고, `archive_exists`가 없어 `AttributeError`

- [ ] **Step 3: 구현한다**

`collector/compaction.py`의 `_META_FIELDS`를 바꾼다:

```python
# 검증 엔진이 붙이는 `_row_status`, 이 모듈이 붙이는 `_window_start`, 그리고 출처를
# 나타내는 `_source_kind`. 셋 다 언더스코어 접두 메타 컬럼이라는 관례를 따른다.
#
# `_source_kind`가 필요한 이유는 `_window_start`의 의미가 출처마다 다르기 때문이다.
# compaction은 "언제 수집했는지"(5분·10분 해상도), bootstrap은 "언제 일어났는지"
# (시간 해상도)를 넣는다. 특히 bike_station_realtime은 행에 다른 시각 컬럼이 없어
# 이 값이 유일한 시각인데, 과거는 시간 단위·현재는 5분 단위가 된다.
SOURCE_KIND_COLLECTOR = "collector"
SOURCE_KIND_BOOTSTRAP = "bootstrap"

_META_FIELDS = [
    ("_row_status", pa.string()),
    ("_window_start", pa.string()),
    ("_source_kind", pa.string()),
]
```

`_read_conformed`에서 `_source_kind`도 채운다:

```python
def _read_conformed(key: str, schema: pa.Schema) -> pa.Table:
    """silver 하나를 읽어 메타 컬럼을 붙이고 목표 스키마에 맞춘다."""
    table = read_parquet(key, as_pandas=False)
    if table is None:
        raise ValueError(f"silver를 읽지 못했다: {key}")
    started = window_start_from_key(key)
    table = table.append_column("_window_start", pa.array([started] * table.num_rows, type=pa.string()))
    table = table.append_column(
        "_source_kind", pa.array([SOURCE_KIND_COLLECTOR] * table.num_rows, type=pa.string())
    )
    return conform(table, schema)
```

`collector/storage.py`에 추가한다. `object_exists`를 `core.s3` 임포트 목록에 넣는다:

```python
def archive_exists(source_id: str, day: date) -> bool:
    """해당 날짜의 archive가 이미 있는지 확인한다.

    bootstrap이 재개 판단에 쓴다 — 상태 파일을 따로 두지 않고 결과물의 존재로 판정한다.
    """
    return object_exists(archive_key(source_id, day))
```

- [ ] **Step 4: 통과를 확인한다**

Run: `cd collector && uv run --frozen pytest tests/test_compaction.py tests/test_archive_storage.py -v`
Expected: PASS

- [ ] **Step 5: 깨진 기존 테스트를 고친다**

`tests/test_compaction.py`의 `test_meta_columns_appended`가 `["a", "_row_status", "_window_start"]`를 단언한다. `"_source_kind"`를 끝에 추가한다.

`tests/test_compaction_run.py`의 두 곳에서 컬럼 목록을 단언한다:
- `TestCompactDate::test_archive_is_readable_with_declared_schema`
- `TestDedup::test_preserves_declared_schema`

둘 다 `["sta", "cnt", "_row_status", "_window_start"]` → `["sta", "cnt", "_row_status", "_window_start", "_source_kind"]`

`TestCompactDate`에 값 검증도 추가한다:

```python
    def test_marks_rows_as_collector_sourced(self):
        config = _config()
        _put_silver("t_source", 5)

        compact_date(config, DAY, today=TODAY)

        table = read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
        assert set(table.column("_source_kind").to_pylist()) == {"collector"}
```

- [ ] **Step 6: 전체 테스트를 돌린다**

Run: `cd collector && uv run --frozen pytest -q`
Expected: PASS (전체 통과)

- [ ] **Step 7: 커밋**

```bash
git add collector/compaction.py collector/storage.py collector/tests/test_compaction.py \
        collector/tests/test_compaction_run.py collector/tests/test_archive_storage.py
git commit -m "feature: archive에 출처 구분 컬럼 _source_kind 추가

compaction과 bootstrap이 같은 archive에 쓰는데 _window_start의 의미가 다르다.
compaction은 수집 시각, bootstrap은 발생 시각이다. 특히 bike_station_realtime은
행에 다른 시각 컬럼이 없어 이 값이 유일한 시각인데 해상도까지 달라진다.
나중에 붙이려면 쌓인 파일을 전부 다시 써야 하므로 먼저 넣는다.

재개 판정용 storage.archive_exists()도 함께 추가한다."
```

---

### Task 2: bootstrap 설정 스키마와 매핑 파일

**Files:**
- Create: `collector/bootstrap/__init__.py`, `collector/bootstrap/config.py`
- Create: `collector/bootstrap/mappings/bike_rental_history.yaml`, `collector/bootstrap/mappings/bike_station_realtime.yaml`
- Test: `collector/tests/test_bootstrap_config.py`

**Interfaces:**
- Consumes: 없음
- Produces:
  - `bootstrap.config.WindowSpec` — `from_column: str`, `format: str`
  - `bootstrap.config.BootstrapConfig` — `kind`, `window`, `encoding`, `na_values`, `column_map`, `value_map`, `service`, `time_format`, `page_size`
  - `bootstrap.config.load(source_id: str) -> BootstrapConfig`

- [ ] **Step 1: 실패하는 테스트를 쓴다**

`collector/tests/test_bootstrap_config.py`:

```python
"""bootstrap 매핑 설정 로딩 테스트.

운영 설정(collector/sources/*.yaml)과 분리한 이유는 수명이 다르기 때문이다 —
bootstrap 설정은 한 번 쓰고 버리는데 운영 yaml은 5분마다 읽히고 오래 유지된다.
"""

import pytest
from pydantic import ValidationError

from bootstrap import config as bootstrap_config


class TestLoad:
    def test_loads_csv_kind_for_rental(self):
        cfg = bootstrap_config.load("bike_rental_history")

        assert cfg.kind == "csv"
        assert cfg.encoding == "cp949"
        assert cfg.window.from_column == "RENT_DT"

    def test_rental_column_map_covers_all_csv_headers(self):
        cfg = bootstrap_config.load("bike_rental_history")

        assert cfg.column_map["자전거번호"] == "BIKE_ID"
        assert cfg.column_map["대여대여소ID"] == "RENT_STATION_ID"
        assert len(cfg.column_map) == 16

    def test_rental_value_map_is_the_verified_mapping(self):
        """빈도로 추정하면 USR_002/USR_003이 뒤집힌다. 조인으로 확정한 값이다."""
        cfg = bootstrap_config.load("bike_rental_history")

        assert cfg.value_map["USR_CLS_CD"] == {
            "내국인": "USR_001", "외국인": "USR_002", "비회원": "USR_003",
        }

    def test_loads_history_api_kind_for_station(self):
        cfg = bootstrap_config.load("bike_station_realtime")

        assert cfg.kind == "history_api"
        assert cfg.service == "bikeListHist"
        assert cfg.window.from_column == "stationDt"

    def test_station_time_format_is_ten_digits(self):
        """8자리를 주면 API가 에러 없이 최신 스냅샷을 반환한다 — 조용히 틀린 데이터가 들어온다."""
        cfg = bootstrap_config.load("bike_station_realtime")

        assert cfg.time_format == "%Y%m%d%H"

    def test_unknown_source_raises(self):
        with pytest.raises(FileNotFoundError):
            bootstrap_config.load("nonexistent_source")


class TestValidation:
    def test_history_api_requires_service(self):
        with pytest.raises(ValidationError):
            bootstrap_config.BootstrapConfig(
                kind="history_api",
                window={"from_column": "stationDt", "format": "%Y%m%d%H"},
                time_format="%Y%m%d%H",
            )

    def test_history_api_requires_time_format(self):
        with pytest.raises(ValidationError):
            bootstrap_config.BootstrapConfig(
                kind="history_api",
                window={"from_column": "stationDt", "format": "%Y%m%d%H"},
                service="bikeListHist",
            )

    def test_csv_requires_column_map(self):
        with pytest.raises(ValidationError):
            bootstrap_config.BootstrapConfig(
                kind="csv",
                window={"from_column": "RENT_DT", "format": "%Y-%m-%d %H:%M:%S"},
            )

    def test_unknown_field_is_rejected(self):
        with pytest.raises(ValidationError):
            bootstrap_config.BootstrapConfig(
                kind="csv",
                window={"from_column": "RENT_DT", "format": "%Y-%m-%d %H:%M:%S"},
                column_map={"a": "A"},
                oops=1,
            )
```

- [ ] **Step 2: 실패를 확인한다**

Run: `cd collector && uv run --frozen pytest tests/test_bootstrap_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bootstrap'`

- [ ] **Step 3: 구현한다**

`collector/bootstrap/__init__.py` (빈 파일):

```python
```

`collector/bootstrap/config.py`:

```python
"""bootstrap 매핑 설정 스키마와 로더.

운영 설정(`collector/sources/*.yaml`)과 파일을 나눈 이유는 **수명**이다. bootstrap
설정은 한 번 쓰고 버리는데, 운영 yaml은 5분마다 읽히고 오래 유지된다. 다 쓴 25줄이
운영 파일에 영구히 남으면 나중에 읽는 사람이 "이건 지금도 쓰이나"를 매번 판단해야 한다.

검증에 필요한 `columns`·`policies`는 여기 없다 — 그건 운영 yaml의 것이고,
`config.loader.load()`로 따로 가져와 합쳐 쓴다.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, model_validator

_MAPPINGS_DIR = Path(__file__).parent / "mappings"


class WindowSpec(BaseModel):
    """행이 속한 시간대를 어느 컬럼에서 어떻게 읽을지."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    from_column: str
    format: str


class BootstrapConfig(BaseModel):
    """소스 하나의 초기 로드 설정.

    `kind`에 따라 쓰는 필드가 갈린다. 한 모델에 둔 이유는 공통 필드(`window`)가 있고
    파일 하나로 읽히는 게 자연스럽기 때문이다. 필수 여부는 validator가 가른다.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["csv", "history_api"]
    window: WindowSpec

    # kind: csv
    encoding: str = "utf-8"
    na_values: tuple[str, ...] = ()
    column_map: dict[str, str] = {}
    value_map: dict[str, dict[str, str]] = {}

    # kind: history_api
    service: str | None = None
    time_format: str | None = None
    page_size: int = 1000

    @model_validator(mode="after")
    def _require_kind_fields(self) -> BootstrapConfig:
        if self.kind == "csv" and not self.column_map:
            raise ValueError("kind=csv면 column_map이 필수다")
        if self.kind == "history_api":
            if not self.service:
                raise ValueError("kind=history_api면 service가 필수다")
            if not self.time_format:
                raise ValueError("kind=history_api면 time_format이 필수다")
        return self


def load(source_id: str) -> BootstrapConfig:
    """해당 소스의 bootstrap 설정을 읽는다.

    args:
        source_id: 소스 id. `mappings/{source_id}.yaml`을 찾는다.
    returns:
        검증된 설정
    raises:
        FileNotFoundError: 그 소스의 매핑 파일이 없을 때. 초기 로드 대상이 아니라는 뜻이다.
    """
    path = _MAPPINGS_DIR / f"{source_id}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"bootstrap 매핑 설정이 없다: {path}")
    return BootstrapConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
```

`collector/bootstrap/mappings/bike_rental_history.yaml`:

```yaml
# 서울특별시 공공자전거 대여이력 정보_YYMM.csv
#
# 이 vintage(2606)는 결측을 빈 문자열로 표기한다. `\N`은 0건이었고, collector의
# _judge_column이 빈 문자열을 결측으로 판정하므로 na_values 없이 처리된다.
# 다른 vintage가 `\N`을 쓸 가능성이 있어 항목은 남긴다.
kind: csv
encoding: cp949
na_values: []

column_map:
  자전거번호: BIKE_ID
  대여일시: RENT_DT
  대여 대여소번호: RENT_ID
  대여 대여소명: RENT_NM
  대여거치대: RENT_HOLD
  반납일시: RTN_DT
  반납대여소번호: RTN_ID
  반납대여소명: RTN_NM
  반납거치대: RTN_HOLD
  이용시간(분): USE_MIN
  이용거리(M): USE_DST
  생년: BIRTH_YEAR
  성별: SEX_CD
  이용자종류: USR_CLS_CD
  대여대여소ID: RENT_STATION_ID
  반납대여소ID: RETURN_STATION_ID

# API 수집분은 코드를, CSV는 한글을 쓴다. archive를 한 체계로 통일한다.
# 이 대응은 같은 날 API 1,000건과 CSV를 (자전거번호, 대여일시)로 조인해 확정했다.
# 빈도로 추정하면 USR_002/USR_003이 뒤집힌다.
value_map:
  USR_CLS_CD:
    내국인: USR_001
    외국인: USR_002
    비회원: USR_003
  SEX_CD:
    m: M
    f: F

window:
  from_column: RENT_DT
  format: "%Y-%m-%d %H:%M:%S"
```

`collector/bootstrap/mappings/bike_station_realtime.yaml`:

```yaml
# bikeListHist 과거 조회 API. 2023-08부터 조회된다.
#
# time_format이 10자리인 것이 중요하다. 8자리(20260817)를 주면 API가 에러 없이
# 무시하고 최신 스냅샷을 반환한다 — 조용히 틀린 데이터가 들어온다.
#
# 응답 필드명이 bike_station_realtime.yaml의 컬럼과 같아서 column_map이 없다.
kind: history_api
service: bikeListHist
time_format: "%Y%m%d%H"
page_size: 1000

window:
  from_column: stationDt
  format: "%Y%m%d%H"
```

- [ ] **Step 4: 통과를 확인한다**

Run: `cd collector && uv run --frozen pytest tests/test_bootstrap_config.py -v`
Expected: PASS (11개)

- [ ] **Step 5: 커밋**

```bash
git add collector/bootstrap/__init__.py collector/bootstrap/config.py \
        collector/bootstrap/mappings/ collector/tests/test_bootstrap_config.py
git commit -m "feature: bootstrap 매핑 설정 스키마와 소스별 설정 파일 추가

운영 yaml과 파일을 나눈 이유는 수명이다. bootstrap 설정은 한 번 쓰고 버리는데
운영 yaml은 5분마다 읽히고 오래 유지된다.

USR_CLS_CD 대응표는 같은 날 API 1,000건과 CSV를 (자전거번호, 대여일시)로
조인해 확정한 값이다. 빈도로 추정하면 USR_002/USR_003이 뒤집힌다."
```

---

### Task 3: CSV 입력

733MB / 418만 행 파일을 다룬다. 날짜마다 파일을 다시 읽으면 31번 훑게 되어 월당 15~30분이 걸리므로 **한 번만 읽고 날짜별로 나눈다**. 행 순서가 `대여일시` 기준으로 완전히 정렬돼 있지 않아(`00:18:46` 행이 `00:30` 이후에 나오는 것을 확인했다) "날짜가 바뀌면 flush"는 쓸 수 없다.

**Files:**
- Create: `collector/bootstrap/csv_source.py`
- Test: `collector/tests/test_bootstrap_csv_source.py`

**Interfaces:**
- Consumes: `bootstrap.config.BootstrapConfig`
- Produces:
  - `csv_source.read_by_date(cfg: BootstrapConfig, csv_dir: Path, days: set[date]) -> dict[date, list[dict]]`
    — 물리 컬럼명으로 rename하고 `value_map`을 적용한 행을 날짜별로 반환한다

- [ ] **Step 1: 실패하는 테스트를 쓴다**

`collector/tests/test_bootstrap_csv_source.py`:

```python
"""CSV 입력 테스트: 인코딩·컬럼 매핑·값 매핑·날짜 버킷팅."""

from datetime import date

import pytest

from bootstrap.config import BootstrapConfig
from bootstrap.csv_source import read_by_date

HEADER = "자전거번호,대여일시,이용자종류,성별\n"


def _cfg(**overrides):
    fields = {
        "kind": "csv",
        "encoding": "cp949",
        "column_map": {
            "자전거번호": "BIKE_ID", "대여일시": "RENT_DT",
            "이용자종류": "USR_CLS_CD", "성별": "SEX_CD",
        },
        "value_map": {
            "USR_CLS_CD": {"내국인": "USR_001", "외국인": "USR_002", "비회원": "USR_003"},
            "SEX_CD": {"m": "M", "f": "F"},
        },
        "window": {"from_column": "RENT_DT", "format": "%Y-%m-%d %H:%M:%S"},
    }
    fields.update(overrides)
    return BootstrapConfig.model_validate(fields)


def _write(tmp_path, body, name="a.csv", encoding="cp949"):
    (tmp_path / name).write_text(HEADER + body, encoding=encoding)
    return tmp_path


class TestReadByDate:
    def test_renames_headers_to_physical_columns(self, tmp_path):
        d = _write(tmp_path, "SPB-1,2026-06-01 00:10:00,내국인,M\n")

        result = read_by_date(_cfg(), d, {date(2026, 6, 1)})

        assert list(result[date(2026, 6, 1)][0].keys()) == [
            "BIKE_ID", "RENT_DT", "USR_CLS_CD", "SEX_CD",
        ]

    def test_applies_value_map(self, tmp_path):
        d = _write(tmp_path, "SPB-1,2026-06-01 00:10:00,비회원,f\n")

        row = read_by_date(_cfg(), d, {date(2026, 6, 1)})[date(2026, 6, 1)][0]

        assert row["USR_CLS_CD"] == "USR_003"
        assert row["SEX_CD"] == "F"

    def test_leaves_unmapped_values_alone(self, tmp_path):
        d = _write(tmp_path, "SPB-1,2026-06-01 00:10:00,내국인,M\n")

        row = read_by_date(_cfg(), d, {date(2026, 6, 1)})[date(2026, 6, 1)][0]

        assert row["SEX_CD"] == "M"

    def test_buckets_rows_by_date(self, tmp_path):
        d = _write(tmp_path,
            "SPB-1,2026-06-01 00:10:00,내국인,M\n"
            "SPB-2,2026-06-02 01:10:00,내국인,M\n"
            "SPB-3,2026-06-01 23:10:00,내국인,M\n")

        result = read_by_date(_cfg(), d, {date(2026, 6, 1), date(2026, 6, 2)})

        assert len(result[date(2026, 6, 1)]) == 2
        assert len(result[date(2026, 6, 2)]) == 1

    def test_survives_rows_that_are_not_date_sorted(self, tmp_path):
        """실측 CSV는 대여일시 순으로 완전히 정렬돼 있지 않다."""
        d = _write(tmp_path,
            "SPB-1,2026-06-01 00:30:00,내국인,M\n"
            "SPB-2,2026-06-02 00:00:00,내국인,M\n"
            "SPB-3,2026-06-01 00:18:46,내국인,M\n")

        result = read_by_date(_cfg(), d, {date(2026, 6, 1)})

        assert len(result[date(2026, 6, 1)]) == 2

    def test_skips_dates_not_requested(self, tmp_path):
        d = _write(tmp_path,
            "SPB-1,2026-06-01 00:10:00,내국인,M\n"
            "SPB-2,2026-06-05 00:10:00,내국인,M\n")

        result = read_by_date(_cfg(), d, {date(2026, 6, 1)})

        assert set(result) == {date(2026, 6, 1)}

    def test_reads_multiple_files_in_the_directory(self, tmp_path):
        _write(tmp_path, "SPB-1,2026-06-01 00:10:00,내국인,M\n", name="a.csv")
        _write(tmp_path, "SPB-2,2026-06-01 02:10:00,내국인,M\n", name="b.csv")

        result = read_by_date(_cfg(), tmp_path, {date(2026, 6, 1)})

        assert len(result[date(2026, 6, 1)]) == 2

    def test_ignores_non_csv_files(self, tmp_path):
        _write(tmp_path, "SPB-1,2026-06-01 00:10:00,내국인,M\n")
        (tmp_path / "notes.txt").write_text("무시", encoding="utf-8")

        result = read_by_date(_cfg(), tmp_path, {date(2026, 6, 1)})

        assert len(result[date(2026, 6, 1)]) == 1

    def test_requested_date_with_no_rows_is_absent(self, tmp_path):
        d = _write(tmp_path, "SPB-1,2026-06-01 00:10:00,내국인,M\n")

        result = read_by_date(_cfg(), d, {date(2026, 6, 1), date(2026, 6, 9)})

        assert date(2026, 6, 9) not in result

    def test_na_values_become_empty_string(self, tmp_path):
        """빈 문자열은 collector 검증 엔진이 결측으로 판정한다."""
        d = _write(tmp_path, "SPB-1,2026-06-01 00:10:00,내국인,\\N\n")

        row = read_by_date(_cfg(na_values=["\\N"]), d, {date(2026, 6, 1)})[date(2026, 6, 1)][0]

        assert row["SEX_CD"] == ""

    def test_unparseable_window_column_raises(self, tmp_path):
        d = _write(tmp_path, "SPB-1,날짜아님,내국인,M\n")

        with pytest.raises(ValueError):
            read_by_date(_cfg(), d, {date(2026, 6, 1)})
```

- [ ] **Step 2: 실패를 확인한다**

Run: `cd collector && uv run --frozen pytest tests/test_bootstrap_csv_source.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bootstrap.csv_source'`

- [ ] **Step 3: 구현한다**

`collector/bootstrap/csv_source.py`:

```python
"""과거 CSV를 읽어 날짜별 원시 행으로 나눈다.

## 왜 한 번만 읽는가

대여이력 월 파일이 733MB / 418만 행이다. 날짜마다 파일을 다시 훑으면 31번 읽게 되어
월당 15~30분이 걸린다. 한 번만 읽고 날짜별로 버킷팅한다.

행 순서가 완전히 정렬돼 있지 않다 — `00:18:46` 행이 `00:30` 이후에 나오는 것을
실측으로 확인했다. 따라서 "날짜가 바뀌면 flush"는 쓸 수 없고 파일 끝까지 읽어야
한 날짜가 끝났다고 확정할 수 있다.

## 메모리

요청한 날짜의 행만 담는다. 월 파일 전체를 요청하면 그만큼 메모리를 쓰므로, 아주 큰
입력에서는 `--from/--to`로 구간을 나눠 여러 번 돌리는 것이 안전하다.
"""

from __future__ import annotations

import csv
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

from bootstrap.config import BootstrapConfig


def read_by_date(cfg: BootstrapConfig, csv_dir: Path, days: set[date]) -> dict[date, list[dict]]:
    """디렉터리의 CSV들을 한 번씩 훑어 요청한 날짜별 행으로 나눈다.

    헤더는 `column_map`으로 물리 컬럼명이 되고, `value_map`에 있는 값은 변환된다.
    `na_values`에 해당하는 값은 빈 문자열이 되어 collector 검증 엔진이 결측으로
    판정한다(`_judge_column`이 `raw_value == ""`를 결측으로 본다).

    args:
        cfg: 해당 소스의 bootstrap 설정
        csv_dir: CSV들이 있는 디렉터리. `*.csv`만 읽는다.
        days: 담을 날짜 집합. 여기 없는 날짜의 행은 버린다.
    returns:
        `{날짜: 행 리스트}`. 행이 하나도 없는 날짜는 키 자체가 없다.
    raises:
        ValueError: 시각 컬럼을 설정된 형식으로 파싱할 수 없을 때.
    """
    buckets: dict[date, list[dict]] = defaultdict(list)
    na_values = set(cfg.na_values)

    for path in sorted(csv_dir.glob("*.csv")):
        with path.open(encoding=cfg.encoding, errors="replace", newline="") as handle:
            for raw in csv.DictReader(handle):
                row = {
                    physical: ("" if raw.get(header) in na_values else (raw.get(header) or ""))
                    for header, physical in cfg.column_map.items()
                }
                day = _row_date(row, cfg)
                if day not in days:
                    continue
                for column, mapping in cfg.value_map.items():
                    if column in row and row[column] in mapping:
                        row[column] = mapping[row[column]]
                buckets[day].append(row)

    return dict(buckets)


def _row_date(row: dict, cfg: BootstrapConfig) -> date:
    """행이 속한 날짜를 시각 컬럼에서 뽑는다."""
    raw = row.get(cfg.window.from_column, "")
    try:
        return datetime.strptime(raw, cfg.window.format).date()
    except ValueError as exc:
        raise ValueError(
            f"시각 컬럼 '{cfg.window.from_column}'을 '{cfg.window.format}' 형식으로 "
            f"읽을 수 없다: {raw!r}"
        ) from exc
```

- [ ] **Step 4: 통과를 확인한다**

Run: `cd collector && uv run --frozen pytest tests/test_bootstrap_csv_source.py -v`
Expected: PASS (11개)

- [ ] **Step 5: 커밋**

```bash
git add collector/bootstrap/csv_source.py collector/tests/test_bootstrap_csv_source.py
git commit -m "feature: bootstrap CSV 입력 추가

733MB 월 파일을 날짜마다 다시 읽으면 31번 훑게 되어 월당 15~30분이 걸린다.
한 번만 읽고 날짜별로 버킷팅한다. 행 순서가 대여일시 기준으로 완전히 정렬돼
있지 않아(00:18:46 행이 00:30 이후에 나오는 것을 확인) 날짜 전환으로 flush할
수 없고 파일 끝까지 읽어야 한다."
```

---

### Task 4: 과거 조회 API 입력

**Files:**
- Create: `collector/bootstrap/api_source.py`
- Test: `collector/tests/test_bootstrap_api_source.py`

**Interfaces:**
- Consumes: `bootstrap.config.BootstrapConfig`
- Produces:
  - `api_source.fetch_by_date(cfg: BootstrapConfig, day: date, *, client: httpx.Client, concurrency: int = 4, max_retries: int = 2) -> list[dict]`
  - `api_source.FetchFailed` — 그 날짜를 포기해야 할 때 던지는 예외

- [ ] **Step 1: 실패하는 테스트를 쓴다**

`collector/tests/test_bootstrap_api_source.py`:

```python
"""과거 조회 API 입력 테스트: 시각 형식·페이지네이션·재시도."""

import json
from datetime import date

import httpx
import pytest

from bootstrap.api_source import FetchFailed, fetch_by_date
from bootstrap.config import BootstrapConfig


def _cfg(page_size=2):
    return BootstrapConfig.model_validate({
        "kind": "history_api",
        "service": "bikeListHist",
        "time_format": "%Y%m%d%H",
        "page_size": page_size,
        "window": {"from_column": "stationDt", "format": "%Y%m%d%H"},
    })


def _body(total, rows):
    return json.dumps({
        "rentBikeStatus": {
            "list_total_count": total,
            "RESULT": {"CODE": "INFO-000", "MESSAGE": "ok"},
            "row": rows,
        }
    }).encode()


def _row(hour, station):
    return {"stationId": station, "stationDt": f"202608{17}{hour:02d}"}


@pytest.fixture(autouse=True)
def _api_key(monkeypatch):
    monkeypatch.setenv("SEOUL_OPENAPI_KEY", "secret-key-123")


class TestTimeFormat:
    def test_requests_ten_digit_hour(self):
        """8자리를 주면 API가 조용히 최신 스냅샷을 반환하므로 형식을 못 박는다."""
        seen = []

        def handler(request):
            seen.append(str(request.url))
            return httpx.Response(200, content=_body(1, [_row(0, "ST-1")]))

        client = httpx.Client(transport=httpx.MockTransport(handler))
        fetch_by_date(_cfg(), date(2026, 8, 17), client=client, concurrency=1)

        assert "/2026081700/" in seen[0]
        assert all(len(u.rstrip("/").rsplit("/", 1)[-1]) == 10 for u in seen)

    def test_covers_all_twenty_four_hours(self):
        seen = []

        def handler(request):
            seen.append(str(request.url).rstrip("/").rsplit("/", 1)[-1])
            return httpx.Response(200, content=_body(1, [_row(0, "ST-1")]))

        client = httpx.Client(transport=httpx.MockTransport(handler))
        fetch_by_date(_cfg(), date(2026, 8, 17), client=client, concurrency=1)

        assert sorted(seen) == sorted(f"20260817{h:02d}" for h in range(24))


class TestPagination:
    def test_follows_total_count_across_pages(self):
        def handler(request):
            url = str(request.url)
            if "/1/2/" in url:
                return httpx.Response(200, content=_body(3, [_row(0, "ST-1"), _row(0, "ST-2")]))
            return httpx.Response(200, content=_body(3, [_row(0, "ST-3")]))

        client = httpx.Client(transport=httpx.MockTransport(handler))
        rows = fetch_by_date(_cfg(page_size=2), date(2026, 8, 17), client=client, concurrency=1)

        assert len(rows) == 24 * 3

    def test_empty_hour_contributes_nothing(self):
        def handler(request):
            return httpx.Response(200, content=json.dumps(
                {"CODE": "INFO-200", "MESSAGE": "해당하는 데이터가 없습니다."}).encode())

        client = httpx.Client(transport=httpx.MockTransport(handler))
        rows = fetch_by_date(_cfg(), date(2026, 8, 17), client=client, concurrency=1)

        assert rows == []


class TestRetry:
    def test_retries_transient_failure_then_succeeds(self):
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(500, content=b"boom")
            return httpx.Response(200, content=_body(1, [_row(0, "ST-1")]))

        client = httpx.Client(transport=httpx.MockTransport(handler))
        rows = fetch_by_date(_cfg(), date(2026, 8, 17), client=client, concurrency=1)

        assert len(rows) == 24

    def test_raises_after_retries_are_exhausted(self):
        def handler(request):
            return httpx.Response(500, content=b"boom")

        client = httpx.Client(transport=httpx.MockTransport(handler))

        with pytest.raises(FetchFailed):
            fetch_by_date(_cfg(), date(2026, 8, 17), client=client, concurrency=1, max_retries=1)

    def test_api_key_is_not_in_the_error_message(self):
        def handler(request):
            return httpx.Response(500, content=b"boom")

        client = httpx.Client(transport=httpx.MockTransport(handler))

        with pytest.raises(FetchFailed) as excinfo:
            fetch_by_date(_cfg(), date(2026, 8, 17), client=client, concurrency=1, max_retries=0)

        assert "secret-key-123" not in str(excinfo.value)
```

- [ ] **Step 2: 실패를 확인한다**

Run: `cd collector && uv run --frozen pytest tests/test_bootstrap_api_source.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bootstrap.api_source'`

- [ ] **Step 3: 구현한다**

`collector/bootstrap/api_source.py`:

```python
"""과거 조회 API에서 하루치 원시 행을 받아온다.

## 시각 인자는 10자리다

`bikeListHist`는 `YYYYMMDDHH`를 받는다. 8자리(`20260817`)를 주면 **에러 없이 무시하고
최신 스냅샷을 반환한다** — 조용히 틀린 데이터가 들어오므로 형식을 설정으로 고정하고
테스트로 못 박는다.

## 병렬도

시간당 7페이지, 호출당 약 0.9초다. 1년이면 61,320콜이라 순차로는 15시간이 걸린다.
기본 병렬도를 4로 잡는다 — 서울 열린데이터광장은 공공 서비스이므로 기본을 보수적으로
두고 필요할 때 명시적으로 올린다.

## 인증키

URL 경로에 그대로 박히므로 예외 메시지에 노출되지 않게 가린다.
"""

from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime

import httpx

from bootstrap.config import BootstrapConfig

_BASE_URL = "http://openapi.seoul.go.kr:8088"
_SUCCESS_CODES = {"INFO-000", "INFO-200"}


class FetchFailed(Exception):
    """재시도 후에도 실패해 그 날짜를 포기해야 할 때."""


def _api_key() -> str:
    return os.environ["SEOUL_OPENAPI_KEY"]


def _mask(url: str) -> str:
    """URL 경로에 실린 인증키를 가린다."""
    return url.replace(_api_key(), "***")


def _page(cfg: BootstrapConfig, stamp: str, start: int, end: int, client: httpx.Client) -> tuple[list[dict], int]:
    """페이지 하나를 받아 (행, 총건수)를 반환한다."""
    url = f"{_BASE_URL}/{_api_key()}/json/{cfg.service}/{start}/{end}/{stamp}/"
    response = client.get(url)
    response.raise_for_status()
    body = json.loads(response.content)

    code = body.get("CODE")
    if code is not None and code not in _SUCCESS_CODES:
        raise FetchFailed(f"{_mask(url)} → {code} {body.get('MESSAGE')}")
    if code == "INFO-200":
        return [], 0

    wrapper = next(iter(body.values()))
    return wrapper.get("row", []), int(wrapper.get("list_total_count", 0))


def _hour(cfg: BootstrapConfig, stamp: str, client: httpx.Client, max_retries: int) -> list[dict]:
    """한 시각을 페이지 끝까지 받아온다. 일시적 실패는 재시도한다."""
    last_error: Exception | None = None
    for _ in range(max_retries + 1):
        try:
            rows, total = _page(cfg, stamp, 1, cfg.page_size, client)
            start = cfg.page_size + 1
            while start <= total:
                more, _ = _page(cfg, stamp, start, start + cfg.page_size - 1, client)
                rows.extend(more)
                start += cfg.page_size
            return rows
        except (httpx.HTTPError, json.JSONDecodeError, FetchFailed) as exc:
            last_error = exc
    raise FetchFailed(f"{stamp} 조회 실패: {_mask(str(last_error))}")


def fetch_by_date(
    cfg: BootstrapConfig,
    day: date,
    *,
    client: httpx.Client,
    concurrency: int = 4,
    max_retries: int = 2,
) -> list[dict]:
    """그 날짜의 24시간을 병렬로 조회해 행을 모아 반환한다.

    args:
        cfg: 해당 소스의 bootstrap 설정
        day: 조회할 날짜
        client: 재사용할 HTTP 클라이언트
        concurrency: 동시에 처리할 시각 수
        max_retries: 시각 하나당 재시도 횟수
    returns:
        24시간치 행. 순서는 보장하지 않는다.
    raises:
        FetchFailed: 어느 한 시각이라도 재시도 후 실패했을 때. 부분 결과를 쓰지 않기
            위해 날짜 전체를 포기한다.
    """
    stamps = [datetime(day.year, day.month, day.day, hour).strftime(cfg.time_format) for hour in range(24)]
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        chunks = list(pool.map(lambda s: _hour(cfg, s, client, max_retries), stamps))
    return [row for chunk in chunks for row in chunk]
```

- [ ] **Step 4: 통과를 확인한다**

Run: `cd collector && uv run --frozen pytest tests/test_bootstrap_api_source.py -v`
Expected: PASS (7개)

- [ ] **Step 5: 커밋**

```bash
git add collector/bootstrap/api_source.py collector/tests/test_bootstrap_api_source.py
git commit -m "feature: bootstrap 과거 조회 API 입력 추가

bikeListHist는 시각 인자가 YYYYMMDDHH 10자리다. 8자리를 주면 에러 없이 무시하고
최신 스냅샷을 반환해 조용히 틀린 데이터가 들어오므로 테스트로 못 박는다.

한 시각이라도 재시도 후 실패하면 날짜 전체를 포기한다 - 부분 결과를 archive에
남기지 않기 위해서다. 재개가 archive 존재 여부 기반이라 다음 실행이 다시 한다."
```

---

### Task 5: runner

**Files:**
- Create: `collector/bootstrap/runner.py`
- Test: `collector/tests/test_bootstrap_runner.py`

**Interfaces:**
- Consumes: `bootstrap.config`, `bootstrap.csv_source`, `bootstrap.api_source`, `compaction.archive_schema`/`conform`/`SOURCE_KIND_BOOTSTRAP`, `storage.archive_exists`/`write_archive`/`write_archive_manifest`/`list_silver_objects`, `validation.engine.validate_batch`, `validation.types.RunContext`
- Produces:
  - `runner.DateResult` — `day`, `status`("loaded"|"skipped"|"failed"), `rows`, `dropped`, `archive_key`, `silver_present`, `error`
  - `runner.group_by_window(rows: list[dict], cfg: BootstrapConfig) -> dict[str, list[dict]]`
  - `runner.load_date(scfg, bcfg, day, rows, *, force=False) -> DateResult`

- [ ] **Step 1: 실패하는 테스트를 쓴다**

`collector/tests/test_bootstrap_runner.py`:

```python
"""bootstrap runner 테스트: 그룹핑·검증 재사용·재개·silver 겹침·스키마 일치."""

from datetime import date, datetime

import pyarrow as pa

from bootstrap.config import BootstrapConfig
from bootstrap.runner import group_by_window, load_date
from compaction import archive_schema
from config.schema import ColumnSpec, Policies, Quality, Schedule, SourceConfig
from config.schema import Storage as StorageConfig
from core.s3 import read_parquet
from storage import read_archive_manifest, write_silver
from tests.conftest import KST

DAY = date(2026, 6, 1)


def _source_config(**overrides):
    fields = {
        "source_id": "t_source",
        "description": "테스트 소스",
        "adapter": "t_adapter",
        "schedule": Schedule(interval="5m"),
        "storage": StorageConfig(bronze_format="json", silver_format="parquet", partition=("dt", "hh")),
        "quality": Quality(max_drop_ratio=0.9, max_missing_ratio=0.0, allow_empty=False),
        "policies": Policies(
            required_missing="drop_row", required_outlier="drop_row",
            optional_missing="keep_null", optional_outlier="set_null",
        ),
        "columns": {
            "BIKE_ID": ColumnSpec(types=("str",), required=True),
            "RENT_DT": ColumnSpec(types=("str",), required=True),
        },
        "config_version": "v1",
    }
    fields.update(overrides)
    return SourceConfig(**fields)


def _bootstrap_config():
    return BootstrapConfig.model_validate({
        "kind": "csv",
        "column_map": {"자전거번호": "BIKE_ID", "대여일시": "RENT_DT"},
        "window": {"from_column": "RENT_DT", "format": "%Y-%m-%d %H:%M:%S"},
    })


def _rows(*times):
    return [{"BIKE_ID": f"SPB-{i}", "RENT_DT": t} for i, t in enumerate(times)]


class TestGroupByWindow:
    def test_groups_by_hour(self):
        rows = _rows("2026-06-01 09:05:00", "2026-06-01 09:55:00", "2026-06-01 10:01:00")

        groups = group_by_window(rows, _bootstrap_config())

        assert sorted(groups) == ["2026-06-01T09:00:00+09:00", "2026-06-01T10:00:00+09:00"]
        assert len(groups["2026-06-01T09:00:00+09:00"]) == 2

    def test_window_is_kst_iso8601(self):
        groups = group_by_window(_rows("2026-06-01 00:00:00"), _bootstrap_config())

        assert list(groups) == ["2026-06-01T00:00:00+09:00"]


class TestLoadDate:
    def test_writes_archive_with_bootstrap_source_kind(self):
        result = load_date(_source_config(), _bootstrap_config(), DAY, _rows("2026-06-01 09:05:00"))

        assert result.status == "loaded"
        table = read_parquet("archive/t_source/dt=2026-06-01.parquet", as_pandas=False)
        assert set(table.column("_source_kind").to_pylist()) == {"bootstrap"}

    def test_window_start_comes_from_the_record_hour(self):
        load_date(_source_config(), _bootstrap_config(), DAY, _rows("2026-06-01 09:05:00"))

        table = read_parquet("archive/t_source/dt=2026-06-01.parquet", as_pandas=False)
        assert table.column("_window_start").to_pylist() == ["2026-06-01T09:00:00+09:00"]

    def test_schema_matches_what_compaction_produces(self):
        """같은 소스의 archive는 출처가 달라도 스키마가 같아야 한다."""
        scfg = _source_config()
        load_date(scfg, _bootstrap_config(), DAY, _rows("2026-06-01 09:05:00"))

        table = read_parquet("archive/t_source/dt=2026-06-01.parquet", as_pandas=False)
        assert table.schema == archive_schema(scfg)

    def test_required_missing_row_is_dropped(self):
        rows = [{"BIKE_ID": "", "RENT_DT": "2026-06-01 09:05:00"},
                {"BIKE_ID": "SPB-1", "RENT_DT": "2026-06-01 09:05:00"}]

        result = load_date(_source_config(), _bootstrap_config(), DAY, rows)

        assert result.rows == 1
        assert result.dropped == 1

    def test_manifest_records_counts_and_issues(self):
        rows = [{"BIKE_ID": "", "RENT_DT": "2026-06-01 09:05:00"},
                {"BIKE_ID": "SPB-1", "RENT_DT": "2026-06-01 09:05:00"}]

        load_date(_source_config(), _bootstrap_config(), DAY, rows)

        manifest = read_archive_manifest("t_source", DAY)
        assert manifest["source_kind"] == "bootstrap"
        assert manifest["rows"] == 1
        assert manifest["dropped"] == 1
        assert manifest["column_issues"]["BIKE_ID"]["missing"] == 1
        assert "silver_signature" not in manifest

    def test_skips_when_archive_already_exists(self):
        load_date(_source_config(), _bootstrap_config(), DAY, _rows("2026-06-01 09:05:00"))

        result = load_date(_source_config(), _bootstrap_config(), DAY, _rows("2026-06-01 10:05:00"))

        assert result.status == "skipped"

    def test_force_overwrites_existing_archive(self):
        load_date(_source_config(), _bootstrap_config(), DAY, _rows("2026-06-01 09:05:00"))

        result = load_date(
            _source_config(), _bootstrap_config(), DAY,
            _rows("2026-06-01 10:05:00", "2026-06-01 11:05:00"), force=True,
        )

        assert result.status == "loaded"
        assert result.rows == 2

    def test_flags_silver_overlap_but_still_writes(self):
        """compaction 구역을 침범해도 막지는 않는다 — 결과 요약에 남긴다."""
        write_silver("t_source", datetime(2026, 6, 1, 9, 5, tzinfo=KST),
                     pa.table({"BIKE_ID": ["SPB-9"], "RENT_DT": ["2026-06-01 09:05:00"]}))

        result = load_date(_source_config(), _bootstrap_config(), DAY, _rows("2026-06-01 09:05:00"))

        assert result.status == "loaded"
        assert result.silver_present is True

    def test_no_silver_means_no_flag(self):
        result = load_date(_source_config(), _bootstrap_config(), DAY, _rows("2026-06-01 09:05:00"))

        assert result.silver_present is False

    def test_empty_rows_writes_nothing(self):
        result = load_date(_source_config(), _bootstrap_config(), DAY, [])

        assert result.status == "skipped"
        assert read_parquet("archive/t_source/dt=2026-06-01.parquet", as_pandas=False) is None
```

- [ ] **Step 2: 실패를 확인한다**

Run: `cd collector && uv run --frozen pytest tests/test_bootstrap_runner.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bootstrap.runner'`

- [ ] **Step 3: 구현한다**

`collector/bootstrap/runner.py`:

```python
"""날짜 하나를 검증해 archive에 적재한다.

## 왜 시간대로 먼저 그룹핑하는가

`_window_start`의 원천이 `RENT_DT`(대여이력)와 `stationDt`(재고)인데, `stationDt`는
`bike_station_realtime.yaml`의 컬럼이 아니다. `_process_columns`가 `config.columns`만
순회하므로(`validation/engine.py`) `validate_batch`가 이 값을 떨어뜨린다.

검증 전에 시간대로 그룹을 나눠두면 그룹마다 시각이 상수가 되어, 검증이 행을 폐기해도
정렬이 깨지지 않는다. 그룹 처리 후 상수를 컬럼으로 붙이면 된다.

## quarantine을 쓰지 않는다

대신 `validate_batch`가 주는 `column_issues`·`policy_actions`를 manifest에 남긴다.
행 단위 원본은 안 남지만 "어느 컬럼에서 몇 건이 왜 빠졌는지"는 알 수 있다.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pyarrow as pa

import storage
from bootstrap.config import BootstrapConfig
from compaction import SOURCE_KIND_BOOTSTRAP, archive_schema, conform
from config.schema import SourceConfig
from validation.engine import validate_batch
from validation.types import RunContext

logger = logging.getLogger(__name__)

_KST = ZoneInfo("Asia/Seoul")


@dataclass(frozen=True)
class DateResult:
    """날짜 하나의 적재 결과.

    `status`는 `loaded`·`skipped`(이미 있거나 행이 없음)·`failed` 중 하나다.
    """

    day: date
    status: str
    rows: int | None = None
    dropped: int | None = None
    archive_key: str | None = None
    silver_present: bool = False
    error: str | None = None


def group_by_window(rows: list[dict], cfg: BootstrapConfig) -> dict[str, list[dict]]:
    """행을 그것이 속한 시간대로 나눈다. 키는 KST ISO8601 문자열이다."""
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        moment = datetime.strptime(row[cfg.window.from_column], cfg.window.format)
        window = moment.replace(minute=0, second=0, microsecond=0, tzinfo=_KST)
        groups[window.isoformat()].append(row)
    return dict(groups)


def load_date(
    scfg: SourceConfig,
    bcfg: BootstrapConfig,
    day: date,
    rows: list[dict],
    *,
    force: bool = False,
) -> DateResult:
    """그 날짜의 행을 검증해 archive에 쓴다.

    args:
        scfg: collector 소스 설정. 컬럼 스펙과 정책을 여기서 가져온다.
        bcfg: bootstrap 매핑 설정
        day: 대상 날짜
        rows: 물리 컬럼명으로 정규화된 원시 행
        force: archive가 이미 있어도 다시 쓴다
    returns:
        이 날짜의 처리 결과. 예외를 던지지 않는다.
    """
    if not rows:
        return DateResult(day=day, status="skipped")
    if not force and storage.archive_exists(scfg.source_id, day):
        return DateResult(day=day, status="skipped")

    silver_present = bool(storage.list_silver_objects(scfg.source_id, day))
    if silver_present:
        # compaction의 구역이다. 막지는 않지만 로그 한 줄은 대량 적재에서 묻히므로
        # 결과에도 남겨 실행 요약에 집계되게 한다.
        logger.warning(
            f"stage=bootstrap source={scfg.source_id} date={day} "
            "silver_present=true 다음 compaction이 이 archive를 덮어쓴다"
        )

    schema = archive_schema(scfg)
    tables: list[pa.Table] = []
    kept = dropped = 0
    column_issues: dict[str, dict[str, int]] = {}

    try:
        for window, group in sorted(group_by_window(rows, bcfg).items()):
            started = datetime.fromisoformat(window)
            ctx = RunContext(
                source_id=scfg.source_id,
                window_start=started,
                window_end=started + timedelta(hours=1),
                attempt=1,
            )
            outcome = validate_batch(group, scfg, ctx)
            kept += outcome.counts.get("kept", 0)
            dropped += outcome.counts.get("dropped", 0)
            for column, counts in outcome.column_issues.items():
                merged = column_issues.setdefault(column, {"missing": 0, "outlier": 0, "type_error": 0})
                for kind, value in counts.items():
                    merged[kind] = merged.get(kind, 0) + value
            if not outcome.silver_rows:
                continue
            table = pa.Table.from_pylist(outcome.silver_rows)
            table = table.append_column("_window_start", pa.array([window] * table.num_rows, type=pa.string()))
            table = table.append_column(
                "_source_kind", pa.array([SOURCE_KIND_BOOTSTRAP] * table.num_rows, type=pa.string())
            )
            tables.append(conform(table, schema))
    except Exception as exc:  # noqa: BLE001 — 어느 예외든 이 날짜만 실패로 격리한다
        logger.error(f"stage=bootstrap status=failed source={scfg.source_id} date={day} reason={exc}")
        return DateResult(day=day, status="failed", error=str(exc), silver_present=silver_present)

    if not tables:
        return DateResult(day=day, status="skipped", rows=0, dropped=dropped, silver_present=silver_present)

    table = pa.concat_tables(tables)
    archive_key = storage.write_archive(scfg.source_id, day, table)
    storage.write_archive_manifest(scfg.source_id, day, {
        "source_id": scfg.source_id,
        "date": f"{day:%Y-%m-%d}",
        "archive_key": archive_key,
        "source_kind": SOURCE_KIND_BOOTSTRAP,
        "rows": table.num_rows,
        "dropped": dropped,
        "column_issues": column_issues,
        "silver_present": silver_present,
        "loaded_at": datetime.now(tz=_KST).isoformat(),
    })
    logger.info(
        f"stage=bootstrap status=loaded source={scfg.source_id} date={day} "
        f"rows={table.num_rows} dropped={dropped} key={archive_key}"
    )
    return DateResult(
        day=day, status="loaded", rows=table.num_rows, dropped=dropped,
        archive_key=archive_key, silver_present=silver_present,
    )
```

- [ ] **Step 4: 통과를 확인한다**

Run: `cd collector && uv run --frozen pytest tests/test_bootstrap_runner.py -v`
Expected: PASS (12개)

- [ ] **Step 5: 커밋**

```bash
git add collector/bootstrap/runner.py collector/tests/test_bootstrap_runner.py
git commit -m "feature: bootstrap runner 추가

검증 전에 시간대로 그룹핑한다. _window_start의 원천인 stationDt가
bike_station_realtime.yaml의 컬럼이 아니라 validate_batch가 떨어뜨리기 때문이다.
그룹마다 시각이 상수가 되면 검증이 행을 폐기해도 정렬이 깨지지 않는다.

quarantine 대신 column_issues를 manifest에 남긴다. silver_signature는 넣지
않는다 - compaction이 나중에 silver를 발견하면 서명 불일치로 넘겨받는다."
```

---

### Task 6: CLI와 통합 검증

**Files:**
- Create: `collector/bootstrap/__main__.py`
- Test: `collector/tests/test_bootstrap_cli.py`

**Interfaces:**
- Consumes: 앞의 모든 모듈
- Produces:
  - `bootstrap.__main__.parse_args(argv) -> argparse.Namespace`
  - `bootstrap.__main__.resolve_dates(args) -> list[date]`
  - `bootstrap.__main__.exit_code_for(results) -> int`
  - `bootstrap.__main__.main(argv) -> int`

- [ ] **Step 1: 실패하는 테스트를 쓴다**

`collector/tests/test_bootstrap_cli.py`:

```python
"""bootstrap CLI 테스트: 인자 파싱, 날짜 범위, 종료 코드."""

from datetime import date

import pytest

from bootstrap import __main__ as cli
from bootstrap.runner import DateResult


class TestParseArgs:
    def test_source_and_range_are_required(self):
        with pytest.raises(SystemExit):
            cli.parse_args(["--source", "bike_rental_history"])

    def test_parses_range_and_options(self):
        args = cli.parse_args([
            "--source", "bike_rental_history", "--from", "2026-06-01", "--to", "2026-06-03",
            "--csv-dir", "data", "--concurrency", "8", "--force",
        ])

        assert args.source == "bike_rental_history"
        assert getattr(args, "from") == "2026-06-01"
        assert args.csv_dir == "data"
        assert args.concurrency == 8
        assert args.force is True

    def test_default_concurrency_is_four(self):
        """공공 API라 기본을 보수적으로 둔다."""
        args = cli.parse_args(["--source", "x", "--from", "2026-06-01", "--to", "2026-06-01"])

        assert args.concurrency == 4


class TestResolveDates:
    def test_range_is_inclusive(self):
        args = cli.parse_args(["--source", "x", "--from", "2026-06-01", "--to", "2026-06-03"])

        assert cli.resolve_dates(args) == [date(2026, 6, 1), date(2026, 6, 2), date(2026, 6, 3)]

    def test_single_day_range(self):
        args = cli.parse_args(["--source", "x", "--from", "2026-06-01", "--to", "2026-06-01"])

        assert cli.resolve_dates(args) == [date(2026, 6, 1)]

    def test_reversed_range_exits(self):
        args = cli.parse_args(["--source", "x", "--from", "2026-06-05", "--to", "2026-06-01"])

        with pytest.raises(SystemExit):
            cli.resolve_dates(args)


class TestExitCode:
    def test_zero_when_nothing_failed(self):
        results = [DateResult(day=date(2026, 6, 1), status="loaded", rows=3),
                   DateResult(day=date(2026, 6, 2), status="skipped")]

        assert cli.exit_code_for(results) == 0

    def test_nonzero_when_any_failed(self):
        results = [DateResult(day=date(2026, 6, 1), status="loaded", rows=3),
                   DateResult(day=date(2026, 6, 2), status="failed", error="boom")]

        assert cli.exit_code_for(results) != 0

    def test_zero_for_empty_results(self):
        assert cli.exit_code_for([]) == 0
```

- [ ] **Step 2: 실패를 확인한다**

Run: `cd collector && uv run --frozen pytest tests/test_bootstrap_cli.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'bootstrap.__main__'`

- [ ] **Step 3: 구현한다**

`collector/bootstrap/__main__.py`:

```python
"""CLI 진입점으로 과거 데이터를 archive에 한 번 적재한다.

## 실행 방법

    cd collector
    uv run --frozen python -m bootstrap --source bike_rental_history \
        --from 2025-01-01 --to 2025-12-31 --csv-dir ../data
    uv run --frozen python -m bootstrap --source bike_station_realtime \
        --from 2025-01-01 --to 2025-12-31 --concurrency 4

Airflow가 부르지 않는 수동 작업이라 태스크 빌더를 만들지 않는다.

## 재개

이미 archive가 있는 날짜는 건너뛴다. 상태 파일을 두지 않는다 — 날짜 단위로
원자적이라(한 날짜를 다 만든 뒤 쓴다) 중단 시 그 날짜는 아예 안 써지고 다음 실행이
다시 만든다. `--force`로 무시한다.

## 종료 코드

실패한 날짜가 하나라도 있으면 non-zero다. 재개가 archive 존재 기반이라 다시 돌리면
실패한 날짜만 재시도된다.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

import httpx

import config.loader as config_loader
from bootstrap import api_source, csv_source, runner
from bootstrap import config as bootstrap_config
from logging_setup import configure_batch_logging


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """CLI 인자를 파싱한다."""
    parser = argparse.ArgumentParser(prog="python -m bootstrap")
    parser.add_argument("--source", required=True, help="소스 id")
    parser.add_argument("--from", dest="from", required=True, help="시작 날짜 (YYYY-MM-DD)")
    parser.add_argument("--to", required=True, help="끝 날짜, 포함 (YYYY-MM-DD)")
    parser.add_argument("--csv-dir", help="kind=csv일 때 CSV들이 있는 디렉터리")
    parser.add_argument("--concurrency", type=int, default=4, help="kind=history_api의 동시 조회 수")
    parser.add_argument("--force", action="store_true", help="archive가 있어도 다시 쓴다")
    return parser.parse_args(argv)


def resolve_dates(args: argparse.Namespace) -> list[date]:
    """처리할 날짜를 오름차순으로 만든다.

    raises:
        SystemExit: `--from`이 `--to`보다 뒤일 때.
    """
    first, last = date.fromisoformat(getattr(args, "from")), date.fromisoformat(args.to)
    if first > last:
        raise SystemExit(f"--from({first})이 --to({last})보다 뒤다")
    return [first + timedelta(days=n) for n in range((last - first).days + 1)]


def exit_code_for(results: list[runner.DateResult]) -> int:
    """실패한 날짜가 있으면 non-zero."""
    return 1 if any(r.status == "failed" for r in results) else 0


def main(argv: list[str] | None = None) -> int:
    """인자를 파싱해 대상 날짜를 적재하고 결과를 종료 코드로 반환한다."""
    args = parse_args(argv)
    configure_batch_logging(args.source)

    scfg = config_loader.load(args.source)
    bcfg = bootstrap_config.load(args.source)
    days = resolve_dates(args)

    results: list[runner.DateResult] = []
    if bcfg.kind == "csv":
        if not args.csv_dir:
            raise SystemExit("kind=csv 소스에는 --csv-dir가 필요하다")
        by_date = csv_source.read_by_date(bcfg, Path(args.csv_dir), set(days))
        for day in days:
            results.append(runner.load_date(scfg, bcfg, day, by_date.get(day, []), force=args.force))
    else:
        with httpx.Client(timeout=60.0) as client:
            for day in days:
                try:
                    rows = api_source.fetch_by_date(
                        bcfg, day, client=client, concurrency=args.concurrency
                    )
                except api_source.FetchFailed as exc:
                    results.append(runner.DateResult(day=day, status="failed", error=str(exc)))
                    continue
                results.append(runner.load_date(scfg, bcfg, day, rows, force=args.force))

    tally: dict[str, int] = {}
    for result in results:
        tally[result.status] = tally.get(result.status, 0) + 1
    summary = " ".join(f"{status}={count}" for status, count in sorted(tally.items()))
    overlapped = sum(1 for r in results if r.silver_present)
    print(f"source={args.source} dates={len(days)} {summary} silver_overlap={overlapped}")

    return exit_code_for(results)


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: 통과를 확인한다**

Run: `cd collector && uv run --frozen pytest tests/test_bootstrap_cli.py -v`
Expected: PASS (9개)

- [ ] **Step 5: 전체 테스트를 돌린다**

Run: `cd collector && uv run --frozen pytest -q`
Expected: PASS

다른 모듈도 확인한다:

```bash
cd /Users/admin/Code/DE_team2-GangnamguUmBokDong
for m in collector airflow nowcaster loader normalizer; do
  printf "%-12s " "$m"; (cd $m && uv run --frozen pytest -q 2>&1 | tail -1)
done
```
Expected: 전부 PASS

- [ ] **Step 6: 실제 CSV로 손 검증한다**

`data/서울특별시 공공자전거 대여이력 정보_2606 (2).csv`가 있으면 하루치를 적재해 본다. moto가 아니라 실제 MinIO가 필요하므로, 없으면 이 단계는 건너뛰고 다음 스텝으로 간다.

```bash
docker compose -f ops/compose/docker-compose.yml up -d minio
cd collector && uv run --frozen python -m bootstrap \
    --source bike_rental_history --from 2026-06-01 --to 2026-06-01 --csv-dir ../data
```

확인할 것:
- 출력의 `loaded=1`
- archive 행 수가 CSV의 2026-06-01 건수(152,006)에서 `BIKE_ID` 결측분을 뺀 값과 맞는지
- 같은 명령을 다시 돌리면 `skipped=1`이 되는지
- `_source_kind`가 전부 `bootstrap`인지

- [ ] **Step 7: 커밋**

```bash
git add collector/bootstrap/__main__.py collector/tests/test_bootstrap_cli.py
git commit -m "feature: bootstrap CLI 추가

kind에 따라 입력 경로가 갈린다. csv는 파일을 한 번 훑어 날짜별로 나누고,
history_api는 날짜마다 24시간을 병렬 조회한다. API 조회가 재시도 후에도
실패하면 그 날짜만 failed로 남기고 계속한다.

Airflow가 부르지 않는 수동 작업이라 태스크 빌더를 만들지 않는다."
```

---

## Self-Review

**Spec 커버리지**

| spec 항목 | 구현 태스크 |
|---|---|
| `_source_kind` 메타 컬럼 | Task 1 |
| `archive_exists` 재개 판정 | Task 1 |
| bootstrap 전용 설정 파일 | Task 2 |
| 값 매핑표(USR_CLS_CD·SEX_CD) | Task 2 (설정) + Task 3 (적용) |
| CSV 단일 패스·날짜 버킷팅 | Task 3 |
| 정렬 안 된 입력 대응 | Task 3 |
| `YYYYMMDDHH` 10자리 형식 | Task 4 |
| 병렬도 기본 4 | Task 4, Task 6 |
| 실패 시 날짜 건너뛰기 | Task 4 (예외) + Task 6 (수집) |
| 시간대 그룹핑 후 검증 | Task 5 |
| `validate_batch` 재사용 | Task 5 |
| quarantine 없이 집계만 | Task 5 (manifest의 `column_issues`) |
| silver 겹침 경고 + 집계 | Task 5 (`silver_present`) + Task 6 (요약) |
| `silver_signature` 미기록 | Task 5 |
| archive 존재 시 skip, `--force` | Task 5 |
| CLI `--from/--to` | Task 6 |
| compaction 산출물과 스키마 일치 | Task 5 (`test_schema_matches_what_compaction_produces`) |

**범위 밖으로 남긴 것**

- 초단기 실황 입력 — spec대로 `kind`를 늘리는 자리만 열어둔다. `BootstrapConfig.kind`가 `Literal`이라 값 하나와 모듈 하나를 추가하면 된다
- 인구 — nowcaster 현행 유지

**타입 일관성 확인**

- `DateResult`는 Task 5에서 정의하고 Task 6에서 쓴다. 필드명 `status`·`silver_present`·`error`가 양쪽에서 같다
- `BootstrapConfig`는 Task 2에서 정의하고 3·4·5·6에서 쓴다. `kind`·`window`·`column_map`·`value_map`·`service`·`time_format`·`page_size` 이름이 일치한다
- `SOURCE_KIND_BOOTSTRAP`은 Task 1에서 정의하고 Task 5에서 쓴다
- `archive_schema`/`conform`은 이미 구현돼 있고 Task 1에서 스키마만 바뀐다
- `read_by_date`는 Task 3에서 `dict[date, list[dict]]`를 반환하고 Task 6이 `by_date.get(day, [])`로 쓴다
- `fetch_by_date`는 Task 4에서 `list[dict]`를 반환하고 Task 6이 그대로 `load_date`에 넘긴다
