"""유니코드 정규화 형태와 무관하게 파일명을 매칭하는 glob."""

from __future__ import annotations

import fnmatch
import unicodedata
from pathlib import Path


def glob_normalized(csv_dir: Path, pattern: str) -> list[Path]:
    """유니코드 정규화 형태와 무관하게 파일명을 매칭해 정렬된 목록으로 반환한다.

    macOS(APFS/HFS+)는 파일명을 NFD로 저장하는데, 이 코드베이스의 `file_pattern`은
    소스 파일에 NFC로 적혀 있다. `Path.glob()`은 바이트 단위로 비교하므로 한글
    패턴이 macOS에서 조용히 0건 매칭되고(에러 없음), archive가 빈 채로 "성공"한다
    (실측: bike_rental_history 1개월 요청이 empty=31로 끝남). 양쪽을 NFC로 맞춘
    뒤 fnmatch로 비교해 이 문제를 없앤다.
    """
    normalized_pattern = unicodedata.normalize("NFC", pattern)
    return sorted(
        path
        for path in csv_dir.iterdir()
        if path.is_file()
        and fnmatch.fnmatch(unicodedata.normalize("NFC", path.name), normalized_pattern)
    )
