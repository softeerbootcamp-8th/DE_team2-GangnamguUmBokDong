"""학습 archive pair를 5분 추론용 serving release로 수동 게시한다.

Spark가 쓴 ``station_master.parquet/part-*.parquet`` prefix는 하나의 immutable
object가 아니므로 release 입력으로 직접 사용할 수 없다. 이 진입점은 실제로 읽은
모든 part bytes에서 ``station_id``/``station_no`` 1:1 crosswalk를 만들고 canonical
JSON 한 개로 고정한 다음, 검증된 rental/return archive와 station profile을 기존
pair-atomic publication 경계에 전달한다.

이 명령은 모델 선택이나 자동 승격 정책을 수행하지 않는다. 운영자가 검토한 exact
archive prefix 둘과 station dependency를 명시하는 수동 maintenance 명령이다.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from urllib.parse import urlsplit

import pyarrow as pa
from core import s3 as s3_io
from core.gold_publication import canonical_json_bytes, sha256_hex
from core.model_snapshot import (
    StationCrosswalk,
    StationCrosswalkEntry,
    build_station_crosswalk,
)
from ml_core.serving_release import (
    ServingReleasePointer,
    publish_release_artifact,
)

from .promotion import prepare_and_promote_serving_release_pair

_SOURCE_FINGERPRINT_SCHEMA_VERSION = "ml-station-crosswalk-source-v1"


@dataclass(frozen=True, slots=True)
class StationCrosswalkBuild:
    """Multipart station master에서 만든 canonical crosswalk와 source 증거다."""

    crosswalk: StationCrosswalk
    source_fingerprint_sha256: str
    source_object_count: int
    source_row_count: int


def build_station_crosswalk_from_spark_source(source_key: str) -> StationCrosswalkBuild:
    """Spark station master object/prefix를 읽어 canonical crosswalk를 만든다.

    args:
        source_key: 단일 Parquet object 또는 Spark multipart prefix의 bucket-relative key
    returns:
        canonical crosswalk와 실제 읽은 part bytes 기반 source fingerprint
    raises:
        ValueError: source key가 잘못됐거나 station mapping이 1:1이 아닐 때
        FileNotFoundError: source 아래 읽을 Parquet object가 없을 때
    """
    _require_source_key(source_key)
    with s3_io.capture_object_reads() as capture:
        table = s3_io.read_parquet(
            source_key,
            columns=["station_id", "station_no"],
            as_pandas=False,
        )
    if table is None:
        raise FileNotFoundError(f"station master Parquet이 없습니다: {source_key}")
    if not isinstance(table, pa.Table):
        raise TypeError("station master reader는 pyarrow.Table을 반환해야 합니다.")

    captured = capture.objects
    if not captured:
        raise RuntimeError(
            "station master를 읽었지만 exact source object bytes가 capture되지 않았습니다."
        )
    source_document = {
        "objects": [
            {
                "byte_sha256": sha256_hex(obj.payload),
                "key": obj.key,
                "size_bytes": len(obj.payload),
            }
            for obj in captured
        ],
        "schema_version": _SOURCE_FINGERPRINT_SCHEMA_VERSION,
        "source_key": source_key,
    }
    source_fingerprint = sha256_hex(canonical_json_bytes(source_document))

    station_ids = table.column("station_id").to_pylist()
    station_nos = table.column("station_no").to_pylist()
    entries: list[StationCrosswalkEntry] = []
    for index, (station_id, station_no) in enumerate(
        zip(station_ids, station_nos, strict=True)
    ):
        if type(station_id) is not str or type(station_no) is not int:
            raise ValueError(
                "station master mapping은 non-null string/int여야 합니다: "
                f"row={index}, station_id={station_id!r}, station_no={station_no!r}"
            )
        entries.append(
            StationCrosswalkEntry(station_no=station_no, sta_id=station_id)
        )
    crosswalk = build_station_crosswalk(entries)
    return StationCrosswalkBuild(
        crosswalk=crosswalk,
        source_fingerprint_sha256=source_fingerprint,
        source_object_count=len(captured),
        source_row_count=table.num_rows,
    )


def publish_pair_release(
    *,
    rental_archive_prefix: str,
    return_archive_prefix: str,
    station_profile_source_key: str,
    station_master_source_key: str,
    allow_contract_change: bool = False,
) -> dict:
    """명시된 학습·station 산출물을 하나의 원자적 serving release로 게시한다.

    Station master multipart bytes는 먼저 canonical crosswalk JSON으로 변환해
    content-addressed object로 고정한다. 이후 검증이나 pointer CAS가 실패하면 기존
    ``serving-release/current.json``은 기존 core publication 계약에 따라 유지된다.

    returns:
        운영자가 release generation과 source identity를 확인할 JSON 호환 결과
    """
    build = build_station_crosswalk_from_spark_source(station_master_source_key)
    crosswalk_ref = publish_release_artifact(
        build.crosswalk.canonical_bytes,
        role="station_master_source",
        extension="json",
    )
    crosswalk_key = _bucket_key_from_s3_uri(crosswalk_ref.uri)
    pointer = prepare_and_promote_serving_release_pair(
        rental_archive_prefix=rental_archive_prefix,
        return_archive_prefix=return_archive_prefix,
        station_profile_source_key=station_profile_source_key,
        station_master_source_key=crosswalk_key,
        allow_contract_change=allow_contract_change,
    )
    return _publication_result(pointer, build, crosswalk_ref.uri)


def _publication_result(
    pointer: ServingReleasePointer,
    build: StationCrosswalkBuild,
    crosswalk_uri: str,
) -> dict:
    """Typed pointer와 crosswalk build 증거를 JSON 호환 결과로 변환한다."""
    return {
        "generation": pointer.generation,
        "release_manifest_byte_sha256": pointer.release_manifest_byte_sha256,
        "release_manifest_uri": pointer.release_manifest_uri,
        "station_crosswalk_byte_sha256": build.crosswalk.sha256,
        "station_crosswalk_source_fingerprint_sha256": (
            build.source_fingerprint_sha256
        ),
        "station_crosswalk_source_object_count": build.source_object_count,
        "station_crosswalk_source_row_count": build.source_row_count,
        "station_crosswalk_uri": crosswalk_uri,
    }


def _require_source_key(value: str) -> str:
    """Station master source가 명시적인 bucket-relative Parquet key/prefix인지 검증한다."""
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or value.startswith("/")
        or "//" in value
        or not value.endswith(".parquet")
    ):
        raise ValueError(
            "station master source는 bucket-relative .parquet object/prefix여야 합니다."
        )
    return value


def _bucket_key_from_s3_uri(uri: str) -> str:
    """Content-addressed S3 URI에서 현재 bucket 내부 object key를 반환한다."""
    parsed = urlsplit(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.startswith("/"):
        raise ValueError(f"content-addressed S3 URI가 아닙니다: {uri}")
    key = parsed.path.removeprefix("/")
    if not key:
        raise ValueError(f"S3 URI에 object key가 없습니다: {uri}")
    return key


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """수동 pair release publication CLI 인자를 파싱한다."""
    parser = argparse.ArgumentParser(
        description="학습 archive pair를 serving-release/current.json으로 원자 게시합니다."
    )
    parser.add_argument("--rental-archive-prefix", required=True)
    parser.add_argument("--return-archive-prefix", required=True)
    parser.add_argument("--station-profile-key", required=True)
    parser.add_argument("--station-master-key", required=True)
    parser.add_argument(
        "--allow-contract-change",
        action="store_true",
        help="승인된 maintenance migration에서만 기존 serving contract 변경을 허용합니다.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> dict:
    """CLI 입력을 검증해 pair release를 게시하고 generation 결과를 출력한다."""
    args = _parse_args(argv)
    result = publish_pair_release(
        rental_archive_prefix=args.rental_archive_prefix,
        return_archive_prefix=args.return_archive_prefix,
        station_profile_source_key=args.station_profile_key,
        station_master_source_key=args.station_master_key,
        allow_contract_change=args.allow_contract_change,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return result


if __name__ == "__main__":
    main()
