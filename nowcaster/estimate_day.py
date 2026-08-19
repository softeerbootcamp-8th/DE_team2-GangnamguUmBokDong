"""과거 아카이브 데이터를 바탕으로 일자별 생활인구 격자 추정 테이블을 생성한다."""

from __future__ import annotations

# pyrefly: ignore [missing-import]
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
    """여러 아카이브 데이터프레임의 격자·시간대별 인구 평균을 계산한다.

    args:
        frames: 과거 아카이브 DataFrame 목록
    returns:
        격자·시간대별 평균 DataFrame (데이터가 없으면 None)
    """
    present = [f for f in frames if f is not None and not f.empty]
    if not present:
        return None
    combined = pd.concat(present, ignore_index=True)
    value_cols = [c for c in VALUE_COLS if c in combined.columns]
    return combined.groupby(KEY_COLS)[value_cols].mean()


def _prep(frames: list[pd.DataFrame | None], suffix_prefix: str) -> list[pd.DataFrame]:
    """데이터프레임의 키를 인덱스로 설정하고 컬럼명에 순번 접미사를 부여한다."""
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
    """후보 주차 및 폴백 데이터들을 결합하여 일자별 생활인구 추정 테이블을 생성한다.

    args:
        candidate_frames: 1~4주 전 후보 DataFrame 목록 (결측은 None)
        extended_frames: 5~8주 전 확장 후보 DataFrame 목록
        historical_avg_frame: 과거 전체 평균 DataFrame
    returns:
        추정치 및 추정 방식 메타데이터가 포함된 DataFrame
    """
    n_candidates = len(candidate_frames)

    # 1. 주차별 데이터프레임의 키를 인덱스로 설정하고 컬럼명 접미사 부여 (__c: 후보, __e: 확장, __h: 과거평균)
    candidate_prepped = _prep(candidate_frames, "c")
    extended_prepped = _prep(extended_frames, "e")
    historical_prepped = _prep([historical_avg_frame] if historical_avg_frame is not None else [], "h")

    # 모든 주차 데이터를 합쳤을 때 아무 데이터도 없다면 빈 결과 테이블을 즉시 반환
    all_prepped = candidate_prepped + extended_prepped + historical_prepped
    if not all_prepped:
        return pd.DataFrame(columns=[*KEY_COLS, *VALUE_COLS, "is_estimated", "estimation_method"])

    # 2. KEY_COLS 인덱스를 기준으로 모든 주차의 컬럼을 가로로 결합 (Wide format 테이블 생성)
    wide = pd.concat(all_prepped, axis=1, join="outer")

    # 3. SPOP(생활인구합계)을 대표 컬럼으로 삼아 행별 유효 주차 수를 계산하고 estimation_method를 결정
    spop_cand_cols = [f"SPOP__c{i}" for i in range(1, n_candidates + 1) if f"SPOP__c{i}" in wide.columns]
    if spop_cand_cols:
        count_valid = wide[spop_cand_cols].notna().sum(axis=1)
    else:
        count_valid = pd.Series(0, index=wide.index)

    # 기본값은 "no_data"로 설정하고 유효 주차 수에 따라 라벨 부여
    method = pd.Series("no_data", index=wide.index, dtype=object)
    method[count_valid == n_candidates] = "weighted_avg"                          # 4개 주차 모두 존재
    method[count_valid == 1] = "single_week_fallback"                              # 1개 주차만 존재
    method[(count_valid > 1) & (count_valid < n_candidates)] = "reweighted_avg"   # 2~3개 주차 존재 (가중치 재조정)
    need_fallback = count_valid == 0                                              # 최근 4주가 모두 결측된 행들

    # 3-1. 1차 폴백: 5~8주 전 확장 데이터(__e)가 하나라도 존재하는 행에 라벨 부여
    ext_prefix_cols = [c for c in wide.columns if "__e" in c]
    if need_fallback.any() and ext_prefix_cols:
        spop_ext_cols = [c for c in ext_prefix_cols if c.startswith("SPOP__e")]
        if spop_ext_cols:
            has_ext = wide.loc[need_fallback, spop_ext_cols].notna().any(axis=1)
            method.loc[has_ext[has_ext].index] = "extended_lookback_fallback"
            need_fallback.loc[has_ext[has_ext].index] = False                     # 확장 데이터로 해결된 행은 폴백 대상에서 제외

    # 3-2. 2차 폴백: 여전히 결측인 행 중 격자의 과거 전체 평균(__h1)이 존재하는 행에 라벨 부여
    if need_fallback.any() and "SPOP__h1" in wide.columns:
        has_hist = wide.loc[need_fallback, "SPOP__h1"].notna()
        method.loc[has_hist[has_hist].index] = "grid_historical_avg"

    # 4. 29개 VALUE_COLS에 대해 가중평균 및 단계별 폴백 수치 연산 수행
    result = pd.DataFrame(index=wide.index)
    for col in VALUE_COLS:
        cand_cols = [f"{col}__c{i}" for i in range(1, n_candidates + 1) if f"{col}__c{i}" in wide.columns]
        if cand_cols:
            # 주차별 가중치(0.4, 0.3, 0.2, 0.1) 매핑 및 결측치를 제외한 가중치 합으로 정규화하여 가중평균 계산
            week_weights = np.array([_WEIGHTS[int(c.rsplit("__c", 1)[1])] for c in cand_cols])
            cand_values = wide[cand_cols]
            valid = cand_values.notna()
            weight_sum = (valid * week_weights).sum(axis=1)
            weighted_sum = (cand_values.fillna(0.0) * week_weights).sum(axis=1)
            value = weighted_sum / weight_sum.where(weight_sum > 0)
        else:
            value = pd.Series(np.nan, index=wide.index)

        # 4-1. 최근 4주가 모두 NaN인 경우: 5~8주 전 확장 데이터 중 가장 가까운 유효 주차 값(bfill)으로 대체
        ext_cols = [f"{col}__e{i}" for i in range(1, len(extended_frames) + 1) if f"{col}__e{i}" in wide.columns]
        if ext_cols:
            ext_value = wide[ext_cols].bfill(axis=1).iloc[:, 0]
            value = value.where(value.notna(), ext_value)

        # 4-2. 확장 데이터도 NaN인 경우: 과거 전체 평균 수치(__h1)로 대체
        hist_col = f"{col}__h1"
        if hist_col in wide.columns:
            value = value.where(value.notna(), wide[hist_col])

        # 4-3. 과거 평균조차 없는 경우: 최종 0.0으로 결측치 채움
        result[col] = value.fillna(0.0)

    # 5. 메타데이터(추정 여부 플래그, 추정 방식) 부여 및 KEY_COLS를 일반 컬럼으로 리셋하여 반환
    result["is_estimated"] = True
    result["estimation_method"] = method
    return result.reset_index()
