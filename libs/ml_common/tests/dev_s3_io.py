"""s3_io.py의 S3 왕복(read/write)을 moto로 검증한다."""

import lightgbm as lgb
import numpy as np
import pandas as pd
import pytest

from ml_common import s3_io


def test_get_object_bytes_missing_key_returns_none():
    assert s3_io.get_object_bytes("no/such/key.json") is None


def test_put_then_get_object_bytes_round_trip():
    s3_io.put_object_bytes("some/key.bin", b"hello world")
    assert s3_io.get_object_bytes("some/key.bin") == b"hello world"


def test_read_parquet_missing_key_returns_none():
    assert s3_io.read_parquet("no/such/table.parquet") is None


def test_write_then_read_parquet_round_trip():
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    s3_io.write_parquet(df, "some/table.parquet")

    result = s3_io.read_parquet("some/table.parquet")

    pd.testing.assert_frame_equal(result, df)


def test_read_parquet_with_columns_filter():
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"], "c": [1.5, 2.5]})
    s3_io.write_parquet(df, "some/table.parquet")

    result = s3_io.read_parquet("some/table.parquet", columns=["a", "c"])

    assert list(result.columns) == ["a", "c"]


def test_read_json_missing_key_returns_none():
    assert s3_io.read_json("no/such/file.json") is None


def test_write_then_read_json_round_trip():
    data = {"holidays_2025": ["2025-01-01", "2025-12-25"]}
    s3_io.write_json("some/file.json", data)

    assert s3_io.read_json("some/file.json") == data


def test_list_keys_returns_only_matching_prefix():
    s3_io.put_object_bytes("silver/station/a.parquet", b"1")
    s3_io.put_object_bytes("silver/station/b.parquet", b"2")
    s3_io.put_object_bytes("silver/rental/c.parquet", b"3")

    keys = s3_io.list_keys("silver/station/")

    assert sorted(keys) == ["silver/station/a.parquet", "silver/station/b.parquet"]


def test_list_keys_empty_prefix_returns_empty_list():
    assert s3_io.list_keys("nothing/here/") == []


@pytest.fixture
def tiny_booster() -> lgb.Booster:
    """왕복 검증용 아주 작은 실제 LightGBM 모델(합성 데이터 5행)."""
    X = pd.DataFrame({"x": [1.0, 2.0, 3.0, 4.0, 5.0]})
    y = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    train_set = lgb.Dataset(X, label=y)
    return lgb.train({"objective": "regression", "verbose": -1, "num_leaves": 3}, train_set, num_boost_round=2)


def test_stage_and_upload_then_download_and_load_booster_round_trip(tiny_booster):
    X = pd.DataFrame({"x": [1.0, 2.0, 3.0]})
    expected_pred = tiny_booster.predict(X)

    s3_io.stage_and_upload_booster(tiny_booster, "models/tiny_test.txt")
    loaded = s3_io.download_and_load_booster("models/tiny_test.txt")

    np.testing.assert_allclose(loaded.predict(X), expected_pred)


def test_download_and_load_booster_missing_key_raises_file_not_found():
    with pytest.raises(FileNotFoundError):
        s3_io.download_and_load_booster("models/does_not_exist.txt")
