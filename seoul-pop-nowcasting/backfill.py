"""서울 열린데이터광장 CSV(1회성) -> 아카이브 parquet 정규화.

CSV 파일은 collector의 `living_population_grid` silver와 동일한 물리 컬럼
(YMD, TT, H_DNG_CD, CELL_ID, SPOP, M00~M70, F00~F70)을 갖는다고 가정한다.
실제 다운로드한 CSV의 컬럼명/인코딩은 반드시 사전에 확인해야 한다.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pyarrow as pa
import pyarrow.compute as pc


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
