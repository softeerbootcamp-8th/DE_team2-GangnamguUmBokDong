"""서울 열린데이터광장 CSV(1회성) -> 아카이브 parquet 정규화.

실제 다운로드한 CSV(`250_LOCAL_RESD_YYYYMMDD.csv`)는 EUC-KR 인코딩에 한글
헤더를 쓰고, 결측/마스킹 값은 `*`로 표기된다. `read_source_csv`가 이를
collector와 동일한 물리 컬럼(YMD, TT, H_DNG_CD, CELL_ID, SPOP, M00~M70,
F00~F70)으로 정규화한다. 2026-04-01~2026-08-11 파일 133개를 전수 비교해
헤더/컬럼 수가 전부 동일함을 확인했다(스키마 변경 없음).
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

_COLUMN_RENAME = {
    "일자": "YMD",
    "시간": "TT",
    "행정동코드": "H_DNG_CD",
    "250M격자": "CELL_ID",
    "생활인구합계": "SPOP",
    "남자 0~9세": "M00",
    "남자 10~14세": "M10",
    "남자 15~19세": "M15",
    "남자 20~24세": "M20",
    "남자 25~29세": "M25",
    "남자 30~34세": "M30",
    "남자 35~39세": "M35",
    "남자 40~44세": "M40",
    "남자 45~49세": "M45",
    "남자 50~54세": "M50",
    "남자 55~59세": "M55",
    "남자 60~64세": "M60",
    "남자 65~69세": "M65",
    "남자 70세 이상": "M70",
    "여자 0~9세": "F00",
    "여자 10~14세": "F10",
    "여자 15~19세": "F15",
    "여자 20~24세": "F20",
    "여자 25~29세": "F25",
    "여자 30~34세": "F30",
    "여자 35~39세": "F35",
    "여자 40~44세": "F40",
    "여자 45~49세": "F45",
    "여자 50~54세": "F50",
    "여자 55~59세": "F55",
    "여자 60~64세": "F60",
    "여자 65~69세": "F65",
    "여자 70세 이상": "F70",
}

_ID_COLUMNS = ("YMD", "TT", "H_DNG_CD", "CELL_ID")


def read_source_csv(path: str | Path) -> pa.Table:
    """서울 열린데이터광장에서 받은 원본 CSV 한 개를 읽어 collector 물리 스키마로 정규화한다."""
    frame = pd.read_csv(path, encoding="euc-kr", dtype=str, na_values=["*"])
    frame = frame.rename(columns=_COLUMN_RENAME)
    for col in frame.columns:
        if col not in _ID_COLUMNS:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return pa.Table.from_pandas(frame, preserve_index=False)


def add_estimation_columns(table: pa.Table) -> pa.Table:
    """실측 데이터임을 나타내는 `is_estimated=False`, `estimation_method="actual"` 컬럼을 추가한다."""
    n = table.num_rows
    table = table.append_column("is_estimated", pa.array([False] * n, type=pa.bool_()))
    table = table.append_column("estimation_method", pa.array(["actual"] * n, type=pa.string()))
    return table


def group_rows_by_date(table: pa.Table) -> dict[date, pa.Table]:
    """`YMD`(YYYYMMDD 문자열) 컬럼 값 기준으로 테이블을 날짜별로 분리한다."""
    if table.num_rows == 0:
        return {}

    ymd_values = table.column("YMD").to_pylist()
    grouped: dict[date, pa.Table] = {}
    for ymd in sorted(set(ymd_values)):
        target_date = datetime.strptime(ymd, "%Y%m%d").replace(tzinfo=UTC).date()
        mask = pc.equal(table.column("YMD"), ymd)
        grouped[target_date] = table.filter(mask)
    return grouped
