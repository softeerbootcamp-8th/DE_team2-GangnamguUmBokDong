"""대형 ZIP 선택 staging을 작은 합성 ZIP으로 검증한다."""

from __future__ import annotations

import stat
import unicodedata
import zipfile
from datetime import date
from pathlib import Path

import pytest

from bootstrap import zip_stage
from bootstrap.zip_stage import Selection, StageError


def _entries() -> dict[str, bytes]:
    """두 달 경계와 범위 밖 파일을 포함한 정상 합성 중앙 목록을 만든다."""
    return {
        "bundle/raw/서울특별시 공공자전거 대여이력 정보_2501 (2).csv": b"rent-jan",
        "bundle/raw/서울특별시 공공자전거 대여이력 정보_2502.csv": b"rent-feb",
        "bundle/raw/서울특별시 공공자전거 대여이력 정보_2503.csv": b"rent-mar",
        "bundle/raw_station/data_2501.csv": b"station-jan",
        "bundle/raw_station/data_2502.csv": b"station-feb",
        "bundle/raw_station/data_2503.csv": b"station-mar",
        "bundle/raw_forecast/OBS_ASOS_TIM_108_2025.csv": b"weather",
        "bundle/raw_people/250m/resd/250_LOCAL_RESD_202501/250_LOCAL_RESD_20250131.csv": (
            b"population-jan"
        ),
        "bundle/raw_people/250m/resd/250_LOCAL_RESD_202502/250_LOCAL_RESD_20250201.csv": (
            b"population-feb"
        ),
        # 실제 아카이브에도 short_for 아래 잘못 복사된 RESD가 있다. 경로가 다른 이
        # 파일은 거주인구 후보로 세지 않아야 한다.
        "bundle/raw_people/250m/short_for/250_LOCAL_RESD_20250131.csv": b"wrong-copy",
    }


def _write_zip(path: Path, entries: dict[str, bytes]) -> Path:
    """주어진 멤버만 가진 작은 합성 ZIP을 작성한다."""
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, body in entries.items():
            archive.writestr(name, body)
    return path


def _stage(
    zip_path: Path,
    output_root: Path,
    *,
    dry_run: bool = False,
    force: bool = False,
) -> zip_stage.StageSummary:
    """두 날짜 경계 범위를 기본 출력 디렉터리로 staging한다."""
    return zip_stage.stage(
        zip_path,
        output_root / "bootstrap",
        output_root / "population",
        date(2025, 1, 31),
        date(2025, 2, 1),
        dry_run=dry_run,
        force=force,
    )


def test_selects_exact_months_and_dates_with_bootstrap_names(tmp_path):
    """겹치는 달·날짜만 골라 각 소비자가 기대하는 파일명으로 출력한다."""
    zip_path = _write_zip(tmp_path / "small.zip", _entries())
    output_root = tmp_path / "out"

    summary = _stage(zip_path, output_root)

    assert summary.file_count == 7
    assert summary.uncompressed_bytes == sum(
        len(value)
        for key, value in _entries().items()
        if "2503" not in key and "short_for" not in key
    )
    assert (
        output_root / "bootstrap/서울특별시 공공자전거 대여이력 정보_2501 (2).csv"
    ).read_bytes() == b"rent-jan"
    assert (
        output_root / "bootstrap/서울특별시 공공자전거 대여이력 정보_2502.csv"
    ).read_bytes() == b"rent-feb"
    assert (
        output_root / "bootstrap/대여소별 공공자전거 대여가능 수량_2501.csv"
    ).read_bytes() == b"station-jan"
    assert (
        output_root / "bootstrap/대여소별 공공자전거 대여가능 수량_2502.csv"
    ).read_bytes() == b"station-feb"
    assert (
        output_root / "bootstrap/weather_realtime_2025.csv"
    ).read_bytes() == b"weather"
    assert (
        output_root / "population/250_LOCAL_RESD_20250131.csv"
    ).read_bytes() == b"population-jan"
    assert (
        output_root / "population/250_LOCAL_RESD_20250201.csv"
    ).read_bytes() == b"population-feb"
    assert not (
        output_root / "bootstrap/서울특별시 공공자전거 대여이력 정보_2503.csv"
    ).exists()


def test_nfd_utf8_name_is_normalized_to_nfc_before_matching(tmp_path):
    """macOS식 NFD 한글 이름도 NFC 패턴과 일치하고 NFC 이름으로 출력한다."""
    entries = _entries()
    rental_name = "bundle/raw/서울특별시 공공자전거 대여이력 정보_2501 (2).csv"
    entries[unicodedata.normalize("NFD", rental_name)] = entries.pop(rental_name)
    zip_path = _write_zip(tmp_path / "nfd.zip", entries)
    output_root = tmp_path / "out"

    _stage(zip_path, output_root)

    destination = (
        output_root / "bootstrap/서울특별시 공공자전거 대여이력 정보_2501 (2).csv"
    )
    assert unicodedata.is_normalized("NFC", destination.name)
    assert destination.read_bytes() == b"rent-jan"


def test_no_utf8_flag_recovers_utf8_bytes_and_preserves_malformed_cp437():
    """플래그 누락 UTF-8은 복구하고 UTF-8이 아닌 정상 CP437 이름은 보존한다."""
    nfd_name = unicodedata.normalize("NFD", "한글.csv")
    mojibake = nfd_name.encode("utf-8").decode("cp437")
    recoverable = zipfile.ZipInfo(mojibake)
    recoverable.flag_bits = 0

    malformed_utf8 = zipfile.ZipInfo("valid-cp437-ÿ.csv")
    malformed_utf8.flag_bits = 0

    assert zip_stage._normalize_member_name(recoverable) == "한글.csv"
    assert zip_stage._normalize_member_name(malformed_utf8) == "valid-cp437-ÿ.csv"


@pytest.mark.parametrize(
    "missing_fragment", ["data_2502.csv", "250_LOCAL_RESD_20250201.csv"]
)
def test_missing_logical_member_fails_closed(tmp_path, missing_fragment):
    """요청한 논리 파일 하나라도 없으면 부분 결과를 만들지 않는다."""
    entries = {
        key: value for key, value in _entries().items() if missing_fragment not in key
    }
    zip_path = _write_zip(tmp_path / "missing.zip", entries)
    output_root = tmp_path / "out"

    with pytest.raises(StageError, match="matches=0"):
        _stage(zip_path, output_root)

    assert not output_root.exists()


def test_duplicate_logical_member_fails_closed(tmp_path):
    """같은 달 대여 파일이 둘이면 임의로 하나를 고르지 않는다."""
    entries = _entries()
    entries["another/raw/서울특별시 공공자전거 대여이력 정보_2501.csv"] = b"duplicate"
    zip_path = _write_zip(tmp_path / "duplicate.zip", entries)

    with pytest.raises(StageError, match=r"rental:2501.*matches=2"):
        _stage(zip_path, tmp_path / "out")


def test_dry_run_reads_metadata_without_creating_output(tmp_path, capsys):
    """dry-run은 파일 수와 크기를 계산하되 출력 디렉터리도 만들지 않는다."""
    zip_path = _write_zip(tmp_path / "small.zip", _entries())
    output_root = tmp_path / "out"

    exit_code = zip_stage.main(
        [
            "--zip",
            str(zip_path),
            "--bootstrap-dir",
            str(output_root / "bootstrap"),
            "--population-dir",
            str(output_root / "population"),
            "--from",
            "2025-01-31",
            "--to",
            "2025-02-01",
            "--dry-run",
        ]
    )

    assert exit_code == 0
    assert not output_root.exists()
    output = capsys.readouterr().out
    assert "mode=dry-run" in output
    assert "files=7" in output
    assert "uncompressed_bytes=" in output
    assert "compressed_bytes=" in output


def test_existing_output_requires_force_and_force_replaces_atomically(tmp_path):
    """기본은 덮어쓰지 않고 force에서만 기존 파일을 교체한다."""
    zip_path = _write_zip(tmp_path / "small.zip", _entries())
    output_root = tmp_path / "out"
    destination = (
        output_root / "bootstrap/서울특별시 공공자전거 대여이력 정보_2501 (2).csv"
    )
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"keep-me")

    with pytest.raises(StageError, match="--force"):
        _stage(zip_path, output_root)
    assert destination.read_bytes() == b"keep-me"

    _stage(zip_path, output_root, force=True)
    assert destination.read_bytes() == b"rent-jan"


def test_copy_failure_cleans_temporaries_and_leaves_no_partial_outputs(
    tmp_path, monkeypatch
):
    """모든 복사가 끝나기 전 실패하면 임시 파일과 부분 최종 파일을 남기지 않는다."""
    zip_path = _write_zip(tmp_path / "small.zip", _entries())
    output_root = tmp_path / "out"
    original_copy = zip_stage._copy_entry
    calls = 0

    def fail_second_copy(archive, selection, temporary_path):
        """첫 멤버만 복사하고 두 번째 복사 전에 합성 실패를 낸다."""
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("synthetic copy failure")
        original_copy(archive, selection, temporary_path)

    monkeypatch.setattr(zip_stage, "_copy_entry", fail_second_copy)

    with pytest.raises(OSError, match="synthetic"):
        _stage(zip_path, output_root)

    assert not list(output_root.rglob("*.csv"))
    assert not list(output_root.rglob("*.part"))


@pytest.mark.parametrize(
    "unsafe_name", ["../escape.csv", "/absolute.csv", "dir\\escape.csv"]
)
def test_unsafe_member_path_fails_even_when_unrelated(tmp_path, unsafe_name):
    """선택 대상이 아닌 멤버도 경로 이동 형태면 ZIP 전체를 거부한다."""
    entries = _entries()
    entries[unsafe_name] = b"unsafe"
    zip_path = _write_zip(tmp_path / "unsafe.zip", entries)

    with pytest.raises(StageError):
        _stage(zip_path, tmp_path / "out", dry_run=True)


def test_symlink_like_member_fails_closed(tmp_path):
    """Unix 외부 속성이 심볼릭 링크인 멤버를 추출하지 않는다."""
    zip_path = _write_zip(tmp_path / "symlink.zip", _entries())
    link = zipfile.ZipInfo("unrelated/link")
    link.create_system = 3
    link.external_attr = (stat.S_IFLNK | 0o777) << 16
    with zipfile.ZipFile(zip_path, "a") as archive:
        archive.writestr(link, "target")

    with pytest.raises(StageError, match="심볼릭 링크"):
        _stage(zip_path, tmp_path / "out", dry_run=True)


def test_output_collision_fails_closed(tmp_path):
    """서로 다른 논리 입력이 같은 출력 경로를 가리키면 거부한다."""
    info = zipfile.ZipInfo("raw/a.csv")
    member = zip_stage.Member(
        info=info, normalized_name=info.filename, parts=("raw", "a.csv")
    )
    destination = tmp_path / "same.csv"
    selections = [
        Selection("first", member, destination),
        Selection("second", member, destination),
    ]

    with pytest.raises(StageError, match="출력 경로가 충돌"):
        zip_stage._validate_destinations(selections)


def test_cross_year_range_is_rejected_without_writes(tmp_path):
    """연도별 ASOS 출력 하나로 표현할 수 없는 범위는 명시적으로 거부한다."""
    zip_path = _write_zip(tmp_path / "small.zip", _entries())

    with pytest.raises(StageError, match="같은 연도"):
        zip_stage.stage(
            zip_path,
            tmp_path / "bootstrap",
            tmp_path / "population",
            date(2025, 12, 31),
            date(2026, 1, 1),
            dry_run=True,
        )

    assert not (tmp_path / "bootstrap").exists()
