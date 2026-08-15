"""후보 주차의 archive 테이블들을 (H_DNG_CD, CELL_ID, TT) 키로 조인해
`estimator.estimate`를 격자·시간대별로 적용한 추정 테이블을 만든다.
"""

from __future__ import annotations

import pandas as pd

import estimator

KEY_COLS = ["H_DNG_CD", "CELL_ID", "TT"]
VALUE_COLS = [
    "SPOP",
    "M00", "M10", "M15", "M20", "M25", "M30", "M35", "M40", "M45", "M50", "M55", "M60", "M65", "M70",
    "F00", "F10", "F15", "F20", "F25", "F30", "F35", "F40", "F45", "F50", "F55", "F60", "F65", "F70",
]


def _indexed(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=VALUE_COLS)
    present_cols = [c for c in VALUE_COLS if c in frame.columns]
    return frame.set_index(KEY_COLS)[present_cols]


def _all_keys(frames: list[pd.DataFrame]) -> list[tuple]:
    keys: set[tuple] = set()
    for frame in frames:
        if not frame.empty:
            keys.update(frame.index.tolist())
    return sorted(keys)


def historical_average(frames: list[pd.DataFrame | None]) -> pd.DataFrame | None:
    """전체 결측 폴백 최후 단계용: 여러 날짜의 archive를 (H_DNG_CD, CELL_ID, TT) 기준으로 평균낸다."""
    present = [f for f in frames if f is not None and not f.empty]
    if not present:
        return None
    combined = pd.concat(present, ignore_index=True)
    value_cols = [c for c in VALUE_COLS if c in combined.columns]
    return combined.groupby(KEY_COLS)[value_cols].mean()


def build_nowcast_table(
    candidate_frames: list[pd.DataFrame | None],
    extended_frames: list[pd.DataFrame | None] = (),
    historical_avg_frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """`candidate_frames`는 [1주전, 2주전, 3주전, 4주전] (해당 주차 archive 전체, 결측/불일치는 None)."""
    indexed_candidates = [_indexed(f) for f in candidate_frames]
    indexed_extended = [_indexed(f) for f in extended_frames]
    indexed_historical = _indexed(historical_avg_frame)

    keys = _all_keys(indexed_candidates + indexed_extended + ([indexed_historical] if historical_avg_frame is not None else []))

    def _lookup(frame: pd.DataFrame, col: str):
        return frame.loc[key, col] if key in frame.index and col in frame.columns else None

    rows = []
    for key in keys:
        row = dict(zip(KEY_COLS, key))

        # SPOP(총 생활인구)이 이 행의 대표 컬럼이다 - 어떤 주차가 유효한지는
        # SPOP 기준으로 한 번만 판정하고, 그 판정(estimation_method)을 행 전체에 적용한다.
        # 나머지 연령/성별 컬럼은 값만 같은 방식으로 계산하되 method는 따로 보고하지 않는다.
        spop_candidates = [_lookup(frame, "SPOP") for frame in indexed_candidates]
        spop_extended = [_lookup(frame, "SPOP") for frame in indexed_extended]
        spop_historical = _lookup(indexed_historical, "SPOP") if historical_avg_frame is not None else None
        _, method = estimator.estimate(spop_candidates, extended=spop_extended, historical_avg=spop_historical)

        for col in VALUE_COLS:
            candidates = [_lookup(frame, col) for frame in indexed_candidates]
            extended = [_lookup(frame, col) for frame in indexed_extended]
            historical_avg = _lookup(indexed_historical, col) if historical_avg_frame is not None else None
            value, _ = estimator.estimate(candidates, extended=extended, historical_avg=historical_avg)
            row[col] = value

        row["is_estimated"] = True
        row["estimation_method"] = method
        rows.append(row)

    return pd.DataFrame(rows)
