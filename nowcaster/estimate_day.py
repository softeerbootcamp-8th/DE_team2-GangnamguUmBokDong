"""과거 아카이브 데이터를 바탕으로 일자별 생활인구 격자 추정 테이블을 생성한다."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import date

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


_RunningSumCount = tuple[pd.DataFrame, pd.DataFrame]


def _accumulate(running: _RunningSumCount | None, frame: pd.DataFrame | None) -> _RunningSumCount | None:
    """하루치 프레임을 누적 합/카운트에 더한다. 프레임이 비었으면 누적을 그대로 반환한다."""
    if frame is None or frame.empty:
        return running
    value_cols = [c for c in VALUE_COLS if c in frame.columns]
    keyed = frame.set_index(KEY_COLS)[value_cols]
    count = keyed.notna().astype("int64")
    values = keyed.fillna(0.0)
    if running is None:
        return values, count
    running_sum, running_count = running
    return running_sum.add(values, fill_value=0.0), running_count.add(count, fill_value=0)


def _finalize(running: _RunningSumCount | None) -> pd.DataFrame | None:
    """누적 합/카운트에서 평균 DataFrame을 만든다. 누적이 없으면 None."""
    if running is None:
        return None
    running_sum, running_count = running
    return running_sum / running_count.where(running_count > 0)


def historical_average_over_dates(
    dates: Iterable[date], read_frame: Callable[[date], pd.DataFrame | None]
) -> pd.DataFrame | None:
    """`historical_average`와 같은 결과를 날짜 하나씩 읽어 누적 합/카운트로 계산한다.

    `historical_average([read_frame(d) for d in dates])`와 동치이지만, 매칭되는
    과거 날짜 전체를 리스트로 한꺼번에 메모리에 올리지 않는다 — 같은 요일/휴일
    패턴에 맞는 날짜가 (인구 격자처럼 하루 수십MB인 소스에서) 수십~수백 개에
    달하면 리스트 컴프리헨션이 그 개수만큼 프레임을 동시에 들고 있어 OOM이 난다
    (2026-08 실측: 이 태스크가 exit 137로 SIGKILL됨). 읽은 프레임은 러닝 합/카운트에
    반영한 직후 버려서, 상주 메모리가 날짜 수와 무관하게 격자 하나치 크기로 고정된다.

    캐시 없이 매번 전체를 다시 읽으므로 archive가 커질수록 이 함수 자체의 실행
    시간은 계속 늘어난다 — 실행 시간까지 archive 크기와 무관하게 만들려면
    `historical_average_cached`를 쓴다.
    """
    running: _RunningSumCount | None = None
    for target in dates:
        running = _accumulate(running, read_frame(target))
    return _finalize(running)


def historical_average_cached(
    pattern: str,
    dates: Iterable[date],
    read_frame: Callable[[date], pd.DataFrame | None],
    load_cache: Callable[[str], tuple[pd.DataFrame | None, pd.DataFrame | None, list[str]]],
    save_cache: Callable[[str, pd.DataFrame, pd.DataFrame, list[str]], None],
) -> pd.DataFrame | None:
    """캐시된 누적 합/카운트에 새로 추가된 날짜만 더해 과거 전체 평균을 유지한다.

    `historical_average_over_dates`는 매번 매칭되는 날짜 전체를 다시 읽는다 —
    archive가 쌓일수록(하루에 1개씩 늘어남) 이 함수의 실행 시간도 계속 늘어난다
    (2026-08 실측: 594일 backfill 직후 한 번 실행에 약 20분, 매칭 날짜 약 420개
    x 대상일 7개 ≈ 2,900회 S3 읽기). 패턴(평일/휴일)별로 누적 합/카운트와 "이미
    반영한 날짜" 목록을 S3에 캐시해두고, 다음 실행부터는 그 목록에 없는 날짜만
    읽어 누적한다 — 정상 운영(하루 1개씩 archive가 늘어나는 상황)에서는 실행마다
    새로 읽는 날짜가 보통 0~1개뿐이라 archive 크기와 무관하게 빨라진다.

    args:
        pattern: 캐시를 구분하는 키(예: "weekday"/"special"). 호출부가 정한다 —
            이 함수는 패턴이 무엇을 의미하는지 모른다.
        dates: 이번 호출에서 유효한, 그 패턴에 매칭되는 전체 날짜 집합(현재
            archive 기준). 캐시에 없는 날짜만 실제로 읽는다.
        read_frame: 날짜 하나의 아카이브를 읽는 함수.
        load_cache: `pattern`으로 (누적 합, 누적 카운트, 반영한 날짜 문자열 목록)을
            읽는 함수. 캐시가 없으면 (None, None, [])를 반환해야 한다.
        save_cache: `pattern`, 갱신된 누적 합/카운트, 갱신된 날짜 문자열 목록을
            받아 저장하는 함수. 새로 반영한 날짜가 하나도 없으면 호출되지 않는다.
    """
    dates = sorted(set(dates))
    cached_sum, cached_count, included = load_cache(pattern)
    included_dates = {date.fromisoformat(d) for d in included}
    new_dates = [d for d in dates if d not in included_dates]

    running: _RunningSumCount | None = (cached_sum, cached_count) if cached_sum is not None else None
    for target in new_dates:
        running = _accumulate(running, read_frame(target))
        # 그 날짜에 데이터가 없어도(read_frame이 None) "확인은 했다"로 기록한다 —
        # 그러지 않으면 데이터 없는 날짜를 실행마다 계속 다시 시도하게 된다.
        included_dates.add(target)

    if new_dates and running is not None:
        save_cache(pattern, running[0], running[1], sorted(d.isoformat() for d in included_dates))

    return _finalize(running)


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
