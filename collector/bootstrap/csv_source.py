"""과거 CSV를 읽어 날짜별 Arrow 테이블로 나눈다.

## 왜 한 번만 읽는가

대여이력 월 파일이 733MB / 418만 행이다. 날짜마다 파일을 다시 훑으면 31번 읽게 되어
월당 15~30분이 걸린다. 한 번만 읽고 날짜별로 버킷팅한다.

행 순서가 완전히 정렬돼 있지 않다 — `00:18:46` 행이 `00:30` 이후에 나오는 것을
실측으로 확인했다. 따라서 "날짜가 바뀌면 flush"는 쓸 수 없고 파일 끝까지 읽어야
한 날짜가 끝났다고 확정할 수 있다.

## 메모리

두 가지로 상주 메모리를 억제한다.

1. **파일명으로 범위 밖 월을 거른다.** 실제 파일명은
   `서울특별시 공공자전거 대여이력 정보_2606 (2).csv`처럼 `_YYMM` 조각을 담고 있다.
   이 조각이 요청한 날짜 범위와 겹치지 않으면 파일을 아예 열지 않는다. 디렉터리가
   42개월 25GB라도 하루치를 요청하면 그 달의 파일만 연다.

   파일명에서 YYMM을 뽑을 수 없으면(규칙을 모르는 파일명) **건너뛰지 않고 읽는다** —
   파일명 규칙을 모른다고 데이터를 조용히 빠뜨리는 것이 훨씬 나쁘다. 이런 경우
   로그를 남긴다.

2. **파이썬 dict 대신 Arrow로 쌓는다.** CSV를 100,000행 청크로 읽어, 청크마다
   날짜별로 나누고 각 날짜 몫을 `pa.RecordBatch`로 바꿔 리스트에 append한다. 청크의
   파이썬 문자열 객체는 변환 직후 해제되므로 상주 메모리는 "Arrow 누적분 + 청크
   하나" 정도로 묶인다. 파일을 다 읽은 뒤 날짜별로 `pa.Table.from_batches`로 합친다.

   모든 컬럼은 문자열(`pa.string()`)로 둔다. 타입 캐스팅은 이후 `validate_batch`가
   한다.
"""

from __future__ import annotations

import csv
import logging
import re
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa

from bootstrap.config import BootstrapConfig

logger = logging.getLogger(__name__)

_CHUNK_ROWS = 100_000

# 파일명 안의 `_YYMM` 조각. 예: "..._2606 (2).csv" -> "2606".
# 앞뒤로 숫자가 더 붙지 않은 정확히 4자리만 잡는다("_20260601" 같은 건 걸러진다).
_YYMM_PATTERN = re.compile(r"(?<!\d)_(\d{4})(?!\d)")


def _file_year_month(path: Path) -> tuple[int, int] | None:
    """파일명에서 YYMM을 뽑아 (연, 월)로 반환한다. 못 뽑거나 월이 부적절하면 None."""
    match = _YYMM_PATTERN.search(path.stem)
    if not match:
        return None
    yy, mm = int(match.group(1)[:2]), int(match.group(1)[2:])
    if not 1 <= mm <= 12:
        return None
    return (2000 + yy, mm)


def _file_overlaps_range(path: Path, months: set[tuple[int, int]]) -> bool | None:
    """파일의 (연, 월)이 요청 범위의 달 집합과 겹치는지.

    returns:
        True/False: 파일명에서 YYMM을 뽑아 겹침 여부를 판정했다.
        None: YYMM을 뽑을 수 없어 판정 불가 — 호출부가 안전하게(읽는 쪽으로) 처리해야 한다.
    """
    year_month = _file_year_month(path)
    if year_month is None:
        return None
    return year_month in months


def read_by_date(cfg: BootstrapConfig, csv_dir: Path, days: set[date]) -> dict[date, pa.Table]:
    """디렉터리의 CSV들을 한 번씩 훑어 요청한 날짜별 Arrow 테이블로 나눈다.

    헤더는 `column_map`으로 물리 컬럼명이 되고, `value_map`에 있는 값은 변환된다.
    `na_values`에 해당하는 값은 빈 문자열이 되어 collector 검증 엔진이 결측으로
    판정한다(`_judge_column`이 `raw_value == ""`를 결측으로 본다).

    파일명에서 YYMM을 뽑아 요청 범위와 겹치지 않으면 그 파일은 열지 않는다. YYMM을
    뽑을 수 없는 파일명은 건너뛰지 않고 읽되 로그를 남긴다.

    args:
        cfg: 해당 소스의 bootstrap 설정
        csv_dir: CSV들이 있는 디렉터리. `*.csv`만 읽는다.
        days: 담을 날짜 집합. 여기 없는 날짜의 행은 버린다.
    returns:
        `{날짜: pa.Table}`. 모든 컬럼은 문자열 타입이다. 행이 하나도 없는 날짜는
        키 자체가 없다.
    raises:
        ValueError: 시각 컬럼을 설정된 형식으로 파싱할 수 없을 때.
    """
    months = {(d.year, d.month) for d in days}
    na_values = set(cfg.na_values)
    batches: dict[date, list[pa.RecordBatch]] = defaultdict(list)

    for path in sorted(csv_dir.glob("*.csv")):
        overlaps = _file_overlaps_range(path, months)
        if overlaps is False:
            logger.info(
                f"stage=bootstrap_csv file={path.name} 요청 범위와 겹치지 않아 건너뜀"
            )
            continue
        if overlaps is None:
            logger.warning(
                f"stage=bootstrap_csv file={path.name} 파일명에서 YYMM을 뽑을 수 없어 "
                "전체를 읽는다"
            )
        _read_file_into_batches(path, cfg, days, na_values, batches)

    return {day: pa.Table.from_batches(day_batches) for day, day_batches in batches.items()}


def _read_file_into_batches(
    path: Path,
    cfg: BootstrapConfig,
    days: set[date],
    na_values: set[str],
    batches: dict[date, list[pa.RecordBatch]],
) -> None:
    """CSV 한 파일을 청크 단위로 읽어 `batches`에 날짜별 RecordBatch를 쌓는다."""
    physical_columns = list(cfg.column_map.values())

    with path.open(encoding=cfg.encoding, errors="replace", newline="") as handle:
        chunk: dict[date, dict[str, list[str]]] = {}
        rows_in_chunk = 0

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

            day_columns = chunk.setdefault(day, {name: [] for name in physical_columns})
            for name in physical_columns:
                day_columns[name].append(row[name])

            rows_in_chunk += 1
            if rows_in_chunk >= _CHUNK_ROWS:
                _flush_chunk(chunk, batches)
                chunk = {}
                rows_in_chunk = 0

        if chunk:
            _flush_chunk(chunk, batches)


def _flush_chunk(
    chunk: dict[date, dict[str, list[str]]],
    batches: dict[date, list[pa.RecordBatch]],
) -> None:
    """청크의 날짜별 파이썬 열을 RecordBatch로 바꿔 누적하고 청크를 비운다."""
    for day, columns in chunk.items():
        arrays = {name: pa.array(values, type=pa.string()) for name, values in columns.items()}
        batches[day].append(pa.RecordBatch.from_pydict(arrays))


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
