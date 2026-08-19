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
from bootstrap.station_join import StationMap
from core.wind import wind_components

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


def read_by_date(
    cfg: BootstrapConfig,
    csv_dir: Path,
    days: set[date],
    station_map: StationMap | None = None,
) -> dict[date, pa.Table]:
    """디렉터리의 CSV들을 한 번씩 훑어 요청한 날짜별 Arrow 테이블로 나눈다.

    헤더는 `column_map`으로 물리 컬럼명이 되고, `value_map`에 있는 값은 변환된다.
    `na_values`에 해당하는 값은 빈 문자열이 되어 collector 검증 엔진이 결측으로
    판정한다(`_judge_column`이 `raw_value == ""`를 결측으로 본다).

    파일명에서 YYMM을 뽑아 요청 범위와 겹치지 않으면 그 파일은 열지 않는다. YYMM을
    뽑을 수 없는 파일명은 건너뛰지 않고 읽되 로그를 남긴다.

    args:
        cfg: 해당 소스의 bootstrap 설정
        csv_dir: CSV들이 있는 디렉터리. `cfg.file_pattern`에 맞는 파일만 읽는다.
        days: 담을 날짜 집합. 여기 없는 날짜의 행은 버린다.
        station_map: `join`이 선언된 설정에서 조인에 쓸 매핑표. 선언되지 않았으면 무시된다.
    returns:
        `{날짜: pa.Table}`. 모든 컬럼은 문자열 타입이다. 행이 하나도 없는 날짜는
        키 자체가 없다.
    raises:
        ValueError: 시각 컬럼을 설정된 형식으로 파싱할 수 없을 때, 또는 `join`이
            선언됐는데 `station_map`이 없을 때.
    """
    if cfg.join is not None and station_map is None:
        # 그냥 두면 조인 컬럼이 전부 비고 required인 stationId 결측으로 그 날짜의 행이
        # 통째로 폐기된다. "적재는 성공했는데 archive가 비었다"보다 여기서 끊는 게 낫다.
        raise ValueError(f"join(provider={cfg.join.provider})이 선언됐는데 station_map이 없다")

    months = {(d.year, d.month) for d in days}
    na_values = set(cfg.na_values)
    batches: dict[date, list[pa.RecordBatch]] = defaultdict(list)

    for path in sorted(csv_dir.glob(cfg.file_pattern)):
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
        _read_file_into_batches(path, cfg, days, na_values, batches, station_map)

    return {day: pa.Table.from_batches(day_batches) for day, day_batches in batches.items()}


def _read_file_into_batches(
    path: Path,
    cfg: BootstrapConfig,
    days: set[date],
    na_values: set[str],
    batches: dict[date, list[pa.RecordBatch]],
    station_map: StationMap | None = None,
) -> None:
    """CSV 한 파일을 청크 단위로 읽어 `batches`에 날짜별 RecordBatch를 쌓는다."""
    # 상수·분해 결과도 archive 스키마에 실려야 하므로 물리 컬럼 목록에 함께 넣는다.
    # 분해의 재료가 된 CSV 헤더(`일시` 등)는 여기 없다 — collector 컬럼이 아니다.
    physical_columns = [
        name
        for name in (
            *cfg.column_map.values(),
            *cfg.constants,
            *(target for spec in cfg.derived_time.values() for target in spec.into),
            *cfg.composed_time,
            *(cfg.join.fills if cfg.join else ()),
            *((cfg.derived_wind.u, cfg.derived_wind.v) if cfg.derived_wind else ()),
        )
        # 언더스코어로 시작하는 컬럼은 조인 키(`_station_no`)처럼 행을 만드는 데만 쓰는
        # 임시 값이다. 어차피 `conform()`이 archive 스키마에서 떨어뜨리므로 여기서
        # 빼서 큰 파일에서 헛되이 쌓이지 않게 한다.
        if not name.startswith("_")
    ]

    with path.open(encoding=cfg.encoding, errors="replace", newline="") as handle:
        chunk: dict[date, dict[str, list[str]]] = {}
        rows_in_chunk = 0

        for raw in csv.DictReader(handle):
            row = {
                physical: ("" if raw.get(header) in na_values else (raw.get(header) or ""))
                for header, physical in cfg.column_map.items()
            }
            row.update(cfg.constants)
            # 날짜 버킷팅(_row_date)이 분해·결합 결과를 읽을 수 있어야 하므로 그보다 먼저 한다.
            row.update(_derived_time_values(raw, cfg))
            row.update(_composed_time_values(raw, cfg))
            day = _row_date(row, cfg)
            if day not in days:
                continue
            for column, mapping in cfg.value_map.items():
                if column in row and row[column] in mapping:
                    row[column] = mapping[row[column]]

            # value_map이 풍속·풍향을 고칠 수도 있으므로 파생은 그 뒤에 계산한다.
            row.update(_derived_wind_values(row, cfg))
            # value_map이 조인 키를 고칠 수도 있으므로 조인은 그 뒤에 온다.
            row.update(_join_values(row, cfg, station_map))

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


def _derived_time_values(raw: dict, cfg: BootstrapConfig) -> dict[str, str]:
    """`derived_time` 규칙으로 시각 컬럼을 분해한 물리 컬럼 값을 만든다.

    args:
        raw: CSV 원본 행(헤더 기준). 분해 재료는 매핑 전 헤더로 읽는다.
        cfg: `derived_time`을 담은 설정.
    returns:
        물리 컬럼명 -> 형식이 적용된 문자열.
    raises:
        ValueError: 원본 값을 `parse` 형식으로 읽을 수 없을 때. 조용히 빈 값으로
            넘기면 required 컬럼이 결측이 되어 행 전체가 폐기되므로 여기서 끊는다.
    """
    values: dict[str, str] = {}
    for header, spec in cfg.derived_time.items():
        text = (raw.get(header) or "").strip()
        try:
            parsed = datetime.strptime(text, spec.parse)
        except ValueError as exc:
            raise ValueError(
                f"시각 컬럼 '{header}'을 '{spec.parse}' 형식으로 읽을 수 없다: {text!r}"
            ) from exc
        for target, fmt in spec.into.items():
            values[target] = parsed.strftime(fmt)
    return values


def _composed_time_values(raw: dict, cfg: BootstrapConfig) -> dict[str, str]:
    """`composed_time` 규칙으로 여러 시각 컬럼을 물리 컬럼 하나로 합친다.

    재고 CSV는 시각이 `일시`(`2025-12-01`)와 `시간대`(`0`)로 나뉘어 있다. 값을 공백
    하나로 이어 붙여(`"2025-12-01 0"`) 읽는데, `%H`는 제로패딩이 없는 한 자리 시각도
    받으므로 CSV를 그대로 쓸 수 있다.

    args:
        raw: CSV 원본 행(헤더 기준). 결합 재료는 매핑 전 헤더로 읽는다.
        cfg: `composed_time`을 담은 설정.
    returns:
        물리 컬럼명 -> 형식이 적용된 문자열.
    raises:
        ValueError: 이어 붙인 값을 `parse` 형식으로 읽을 수 없을 때. `derived_time`과
            같은 이유로 여기서 끊는다 — 빈 값으로 넘기면 날짜 버킷팅이 죽거나 행이
            통째로 사라진다.
    """
    values: dict[str, str] = {}
    for target, spec in cfg.composed_time.items():
        text = " ".join((raw.get(header) or "").strip() for header in spec.from_)
        try:
            parsed = datetime.strptime(text, spec.parse)
        except ValueError as exc:
            raise ValueError(
                f"시각 컬럼 {list(spec.from_)}을 합친 '{text}'을 '{spec.parse}' 형식으로 "
                f"읽을 수 없다 (대상 컬럼 '{target}')"
            ) from exc
        values[target] = parsed.strftime(spec.format)
    return values


def _join_values(row: dict, cfg: BootstrapConfig, station_map: StationMap | None) -> dict[str, str]:
    """`join` 규칙으로 매핑표에서 가져온 컬럼 값을 만든다.

    매핑표에 없는 대여소는 `fills` 전부가 빈 문자열이 된다. required인 `stationId`가
    비면 검증 엔진의 `required_missing` 정책이 그 행을 폐기하므로, 여기서 따로
    거르지 않는다 — 폐기 건수는 manifest의 `column_issues`에 남는다.

    args:
        row: 매핑·상수·분해·결합·value_map까지 끝난 행.
        cfg: `join`을 담은 설정.
        station_map: 조인에 쓸 매핑표.
    returns:
        `fills`에 선언된 물리 컬럼명 -> 값. 규칙이 없으면 빈 dict.
    """
    spec = cfg.join
    if spec is None or station_map is None:
        return {}

    info = station_map.lookup(row.get(spec.by.number, ""), row.get(spec.by.name, ""))
    available = {
        "stationId": info.station_id if info else "",
        "rackTotCnt": info.rack_tot_cnt if info else "",
        "shared": info.shared if info else "",
        "stationLatitude": info.latitude if info else "",
        "stationLongitude": info.longitude if info else "",
    }
    return {column: available.get(column, "") for column in spec.fills}


def _derived_wind_values(row: dict, cfg: BootstrapConfig) -> dict[str, str]:
    """`derived_wind` 규칙으로 UUU·VVV를 계산한다.

    풍속·풍향 중 하나라도 결측이면 두 성분을 빈 문자열로 둔다 — 0으로 채우면 "무풍"이
    되어 뜻이 달라진다(실측: 풍속·풍향 결측이 각 31건). 검증 엔진이 빈 문자열을
    결측으로 판정해 `optional_missing` 정책이 걸린다.

    소수 첫째 자리로 찍는 이유는 운영 수집분의 UUU·VVV가 그 자리까지만 오기 때문이다.
    둘째 자리를 붙이면 같은 컬럼에 정밀도가 다른 값이 섞이는데, 입력인 풍속·풍향이
    이미 반올림된 값이라 그 자리는 실제 정밀도가 아니다.

    args:
        row: 매핑·상수·분해·value_map까지 끝난 행.
        cfg: `derived_wind`를 담은 설정.
    returns:
        `{u 컬럼: 값, v 컬럼: 값}`. 규칙이 없으면 빈 dict.
    """
    spec = cfg.derived_wind
    if spec is None:
        return {}

    speed, direction = row.get(spec.speed, ""), row.get(spec.direction, "")
    if not str(speed).strip() or not str(direction).strip():
        return {spec.u: "", spec.v: ""}

    u, v = wind_components(speed, direction)
    return {spec.u: _format_component(u), spec.v: _format_component(v)}


def _format_component(value: float) -> str:
    """성분을 운영 수집분과 같은 소수 첫째 자리 문자열로 찍는다.

    `+ 0.0`으로 음의 0을 없앤다. 북풍(`VEC=0`)이면 동서 성분이 정확히 `-0.0`이 되고,
    절댓값이 0.05보다 작은 음수도 반올림 후 `-0.0`이 된다. 그대로 두면 수치적으로
    같은 값이 `"0.0"`과 `"-0.0"` 두 표기로 archive에 섞이고, parquet의 double로도
    음의 0이 남아 비교·집계에서 혼란을 준다.
    """
    return f"{round(value, 1) + 0.0:.1f}"


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
