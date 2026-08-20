"""Authority inference가 legacy champion pointer 없이 pinned model만 쓰는지 검증한다."""

import numpy as np
import pandas as pd
import pytest
from core.gold_publication import canonical_json_bytes

from ml_core import scoring
from ml_core.model_contract import station_dtype_from_payload


class _Booster:
    """LightGBM Booster의 predict 표면만 제공하는 fixture다."""

    def __init__(self, value: float) -> None:
        """모든 행에 반환할 값을 고정한다."""
        self.value = value

    def predict(self, frame: pd.DataFrame) -> np.ndarray:
        """입력 행 수만큼 고정 prediction을 반환한다."""
        return np.full(len(frame), self.value)


def _frame() -> pd.DataFrame:
    """Rental scoring contract의 한 행짜리 feature frame을 만든다."""
    return pd.DataFrame(
        {
            "station_no": [1],
            "capacity": [10],
            "lat": [37.5],
            "lon": [127.0],
            "temp": [20.0],
            "precip": [0.0],
            "pop_total": [100.0],
            "minute": [60],
            "dow": [1],
            "is_holiday": [0],
            "day": [9000],
            "horizon": [1],
            "rental_lag_1h": [2.0],
            "rental_exposure": [0.5],
            "date": ["2026-08-20"],
            "hour": [1],
        }
    )


def _pinned(value: float = 2.0) -> scoring.PinnedScoringModel:
    """Rental/return에 재사용할 in-memory pinned scorer를 만든다."""
    return scoring.PinnedScoringModel(
        boosters={
            "poisson": _Booster(value),
            "q10": _Booster(1.0),
            "q50": _Booster(2.0),
            "q90": _Booster(3.0),
        },
        conformal_correction=0.25,
        station_dtype=pd.CategoricalDtype(categories=[1]),
    )


def test_pinned_context_never_calls_legacy_model_or_category_loaders(monkeypatch):
    """Pointer/archive lazy loader가 모두 실패하도록 해도 pinned bytes만으로 채점한다."""
    monkeypatch.setattr(
        scoring,
        "load_boosters",
        lambda _name: (_ for _ in ()).throw(AssertionError("legacy boosters")),
    )
    monkeypatch.setattr(
        scoring,
        "load_conformal_correction",
        lambda _name: (_ for _ in ()).throw(AssertionError("legacy correction")),
    )
    monkeypatch.setattr(
        scoring,
        "load_station_dtype",
        lambda _name: (_ for _ in ()).throw(AssertionError("legacy categories")),
    )

    with scoring.use_pinned_scoring_models({"rental": _pinned(), "return": _pinned()}):
        result = scoring.predict(
            _frame(),
            "rental",
            exposure_col="rental_exposure",
        )

    assert result.loc[0, "pred_mean"] == 1.0
    assert result.loc[0, "pred_p10"] == 0.75
    assert result.loc[0, "pred_p50"] == 2.0
    assert result.loc[0, "pred_p90"] == 3.25


def test_pinned_scoring_context_rejects_nested_or_incomplete_model_pair():
    """한 run에서 scorer 교체나 rental-only mapping을 허용하지 않는다."""
    with (
        pytest.raises(ValueError, match="rental/return"),
        scoring.use_pinned_scoring_models({"rental": _pinned()}),
    ):
        pass

    with (
        scoring.use_pinned_scoring_models({"rental": _pinned(), "return": _pinned()}),
        pytest.raises(RuntimeError, match="중첩"),
        scoring.use_pinned_scoring_models({"rental": _pinned(), "return": _pinned()}),
    ):
        pass


def test_station_dtype_from_pinned_payload_preserves_category_order():
    """Canonical station_no array 순서를 LightGBM categorical order로 보존한다."""
    dtype = station_dtype_from_payload(canonical_json_bytes([7, 2, 11]))

    assert list(dtype.categories) == [7, 2, 11]
