"""후보 주차의 archive 테이블들을 (H_DNG_CD, CELL_ID, TT) 키로 조인해
`estimator.estimate`를 격자·시간대별로 적용한 추정 테이블을 만든다.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

KEY_COLS = ["H_DNG_CD", "CELL_ID", "TT"]
VALUE_COLS = [
    "SPOP",
    "M00", "M10", "M15", "M20", "M25", "M30", "M35", "M40", "M45", "M50", "M55", "M60", "M65", "M70",
    "F00", "F10", "F15", "F20", "F25", "F30", "F35", "F40", "F45", "F50", "F55", "F60", "F65", "F70",
]

_WEIGHTS = {1: 0.4, 2: 0.3, 3: 0.2, 4: 0.1}


def historical_average(frames: list[pd.DataFrame | None]) -> pd.DataFrame | None:
    """전체 결측 폴백 최후 단계용: 여러 날짜의 archive를 (H_DNG_CD, CELL_ID, TT) 기준으로 평균낸다."""
    present = [f for f in frames if f is not None and not f.empty]
    if not present:
        return None
    combined = pd.concat(present, ignore_index=True)
    value_cols = [c for c in VALUE_COLS if c in combined.columns]
    return combined.groupby(KEY_COLS)[value_cols].mean()


def _prep(frames: list[pd.DataFrame | None], suffix_prefix: str) -> list[pd.DataFrame]:
    """각 프레임을 KEY_COLS로 인덱싱하고, 컬럼명에 `__{suffix_prefix}{순번}`을 붙인다.

    순번은 1부터 시작하며 "가까운 주차/우선순위"를 뜻한다(가중치 매핑, 폴백 우선순위에 사용).
    """
    prepped = []
    for i, frame in enumerate(frames, start=1):
        if frame is None or frame.empty:
            continue
        present_cols = [c for c in VALUE_COLS if c in frame.columns]
        if not present_cols:
            continue
        prepped.append(frame.set_index(KEY_COLS)[present_cols].add_suffix(f"__{suffix_prefix}{i}"))
    return prepped


def build_nowcast_table(
    candidate_frames: list[pd.DataFrame | None],
    extended_frames: list[pd.DataFrame | None] = (),
    historical_avg_frame: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """`candidate_frames`는 [1주전, 2주전, 3주전, 4주전] (해당 주차 archive 전체, 결측/불일치는 None).

    격자 수 × 시간대만큼 키가 많아질 수 있어(서울 전체 기준 수십만 행), 키별 파이썬 루프
    대신 pandas 벡터화 연산으로 처리한다.
    """
    n_candidates = len(candidate_frames)
    candidate_prepped = _prep(candidate_frames, "c")
    extended_prepped = _prep(extended_frames, "e")
    historical_prepped = _prep([historical_avg_frame] if historical_avg_frame is not None else [], "h")

    all_prepped = candidate_prepped + extended_prepped + historical_prepped
    if not all_prepped:
        return pd.DataFrame(columns=[*KEY_COLS, *VALUE_COLS, "is_estimated", "estimation_method"])

    wide = pd.concat(all_prepped, axis=1, join="outer")

    # SPOP이 대표 컬럼이다 - 어떤 주차가 유효한지는 SPOP 기준으로 한 번만 판정하고,
    # 그 판정(estimation_method)을 행 전체에 적용한다.
    spop_cand_cols = [f"SPOP__c{i}" for i in range(1, n_candidates + 1) if f"SPOP__c{i}" in wide.columns]
    if spop_cand_cols:
        count_valid = wide[spop_cand_cols].notna().sum(axis=1)
    else:
        count_valid = pd.Series(0, index=wide.index)

    method = pd.Series("no_data", index=wide.index, dtype=object)
    method[count_valid == n_candidates] = "weighted_avg"
    method[count_valid == 1] = "single_week_fallback"
    method[(count_valid > 1) & (count_valid < n_candidates)] = "reweighted_avg"
    need_fallback = count_valid == 0

    ext_prefix_cols = [c for c in wide.columns if "__e" in c]
    if need_fallback.any() and ext_prefix_cols:
        spop_ext_cols = [c for c in ext_prefix_cols if c.startswith("SPOP__e")]
        if spop_ext_cols:
            has_ext = wide.loc[need_fallback, spop_ext_cols].notna().any(axis=1)
            method.loc[has_ext[has_ext].index] = "extended_lookback_fallback"
            need_fallback.loc[has_ext[has_ext].index] = False

    if need_fallback.any() and "SPOP__h1" in wide.columns:
        has_hist = wide.loc[need_fallback, "SPOP__h1"].notna()
        method.loc[has_hist[has_hist].index] = "grid_historical_avg"

    result = pd.DataFrame(index=wide.index)
    for col in VALUE_COLS:
        cand_cols = [f"{col}__c{i}" for i in range(1, n_candidates + 1) if f"{col}__c{i}" in wide.columns]
        if cand_cols:
            week_weights = np.array([_WEIGHTS[int(c.rsplit("__c", 1)[1])] for c in cand_cols])
            cand_values = wide[cand_cols]
            valid = cand_values.notna()
            weight_sum = (valid * week_weights).sum(axis=1)
            weighted_sum = (cand_values.fillna(0.0) * week_weights).sum(axis=1)
            value = weighted_sum / weight_sum.where(weight_sum > 0)
        else:
            value = pd.Series(np.nan, index=wide.index)

        ext_cols = [f"{col}__e{i}" for i in range(1, len(extended_frames) + 1) if f"{col}__e{i}" in wide.columns]
        if ext_cols:
            # bfill(axis=1) 후 첫 컬럼 = 이 행에서 컬럼 순서상 가장 먼저 나오는 값 없지 않은 값
            ext_value = wide[ext_cols].bfill(axis=1).iloc[:, 0]
            value = value.where(value.notna(), ext_value)

        hist_col = f"{col}__h1"
        if hist_col in wide.columns:
            value = value.where(value.notna(), wide[hist_col])

        result[col] = value.fillna(0.0)

    result["is_estimated"] = True
    result["estimation_method"] = method
    return result.reset_index()
