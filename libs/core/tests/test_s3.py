"""core.s3의 S3 왕복(read/write)을 moto로 검증한다."""

import pandas as pd
import pytest

from core import s3


def test_get_object_bytes_missing_key_returns_none():
    assert s3.get_object_bytes("no/such/key.json") is None


def test_put_then_get_object_bytes_round_trip():
    s3.put_object_bytes("some/key.bin", b"hello world")
    assert s3.get_object_bytes("some/key.bin") == b"hello world"


def test_read_parquet_missing_key_returns_none():
    assert s3.read_parquet("no/such/table.parquet") is None


def test_write_then_read_parquet_round_trip():
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    s3.write_parquet(df, "some/table.parquet")

    result = s3.read_parquet("some/table.parquet")

    pd.testing.assert_frame_equal(result, df)


def test_read_parquet_with_columns_filter():
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"], "c": [1.5, 2.5]})
    s3.write_parquet(df, "some/table.parquet")

    result = s3.read_parquet("some/table.parquet", columns=["a", "c"])

    assert list(result.columns) == ["a", "c"]


def test_read_parquet_reads_spark_style_multi_part_directory():
    """Spark의 df.write.parquet(key)는 key 자체가 아니라 key를 prefix로 삼아
    part-00000-....parquet 여러 개 + _SUCCESS 마커를 쓴다 — read_parquet()가
    정확히 그 key의 단일 GET이 실패하면 prefix로 보고 파트 파일들을 찾아
    이어붙이는지 확인한다."""
    part1 = pd.DataFrame({"a": [1, 2]})
    part2 = pd.DataFrame({"a": [3, 4]})
    s3.put_object_bytes("out/table.parquet/_SUCCESS", b"")
    s3.write_parquet(part1, "out/table.parquet/part-00000.parquet")
    s3.write_parquet(part2, "out/table.parquet/part-00001.parquet")

    result = s3.read_parquet("out/table.parquet")

    assert sorted(result["a"].tolist()) == [1, 2, 3, 4]


def test_read_parquet_as_pandas_false_returns_pyarrow_table():
    df = pd.DataFrame({"a": [1, 2]})
    s3.write_parquet(df, "some/table.parquet")

    result = s3.read_parquet("some/table.parquet", as_pandas=False)

    assert result.to_pandas()["a"].tolist() == [1, 2]


def test_read_parquet_date_range_reads_only_requested_partitions():
    """Spark의 partitionBy("date") 출력(key/date=YYYY-MM-DD/part-*.parquet, 파일
    내용엔 date 컬럼이 없음 — Hive 컨벤션)에서 date_range로 지정한 날짜만 나열/
    다운로드하고, 범위 밖 파티션은 아예 안 건드리는지 확인한다."""
    # Spark처럼 파일 내용엔 "date" 컬럼이 없다 — 파티션 폴더명에만 있음.
    s3.write_parquet(pd.DataFrame({"a": [1, 2]}), "mh/date=2025-11-01/part-00000.parquet")
    s3.write_parquet(pd.DataFrame({"a": [3]}), "mh/date=2025-11-02/part-00000.parquet")
    # 범위 밖 — 읽히면 안 된다.
    s3.write_parquet(pd.DataFrame({"a": [999]}), "mh/date=2025-12-01/part-00000.parquet")

    result = s3.read_parquet("mh", date_range=("2025-11-01", "2025-11-02"))

    assert sorted(result["a"].tolist()) == [1, 2, 3]
    assert sorted(result["date"].unique().tolist()) == ["2025-11-01", "2025-11-02"]


def test_read_parquet_date_range_with_columns_keeps_date_and_requested_order():
    s3.write_parquet(pd.DataFrame({"a": [1], "b": [10]}), "mh2/date=2025-11-01/part-00000.parquet")

    result = s3.read_parquet("mh2", columns=["date", "a"], date_range=("2025-11-01", "2025-11-01"))

    assert list(result.columns) == ["date", "a"]
    assert result["date"].tolist() == ["2025-11-01"]


def test_read_parquet_date_range_missing_partitions_returns_none():
    assert s3.read_parquet("mh3", date_range=("2025-01-01", "2025-01-02")) is None


def test_read_parquet_date_range_as_pandas_false_returns_pyarrow_table():
    s3.write_parquet(pd.DataFrame({"a": [1]}), "mh4/date=2025-11-01/part-00000.parquet")

    result = s3.read_parquet("mh4", as_pandas=False, date_range=("2025-11-01", "2025-11-01"))

    assert result.to_pandas()["a"].tolist() == [1]


def test_read_json_missing_key_returns_none():
    assert s3.read_json("no/such/file.json") is None


def test_write_then_read_json_round_trip():
    data = {"holidays_2025": ["2025-01-01", "2025-12-25"]}
    s3.write_json("some/file.json", data)

    assert s3.read_json("some/file.json") == data


def test_list_keys_returns_only_matching_prefix():
    s3.put_object_bytes("silver/station/a.parquet", b"1")
    s3.put_object_bytes("silver/station/b.parquet", b"2")
    s3.put_object_bytes("silver/rental/c.parquet", b"3")

    keys = s3.list_keys("silver/station/")

    assert sorted(keys) == ["silver/station/a.parquet", "silver/station/b.parquet"]


def test_list_keys_empty_prefix_returns_empty_list():
    assert s3.list_keys("nothing/here/") == []


def test_object_exists():
    assert s3.object_exists("some/key.bin") is False
    s3.put_object_bytes("some/key.bin", b"x")
    assert s3.object_exists("some/key.bin") is True


def test_delete_object():
    s3.put_object_bytes("some/key.bin", b"x")
    s3.delete_object("some/key.bin")
    assert s3.get_object_bytes("some/key.bin") is None


@pytest.mark.parametrize("n", [0, 3])
def test_delete_objects(n):
    keys = [f"batch/{i}.bin" for i in range(n)]
    for key in keys:
        s3.put_object_bytes(key, b"x")

    s3.delete_objects(keys)

    assert all(s3.get_object_bytes(key) is None for key in keys)
