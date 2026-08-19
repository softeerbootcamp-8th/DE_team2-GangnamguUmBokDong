"""구 Gold 스키마용 로컬 시드 실행을 명시적으로 차단한다."""

import sys

DISABLED_MESSAGE = (
    "[gold-postgis] apps/api/seed_gold.py는 #129 Gold PostGIS 계약과 호환되지 않아 비활성화되었습니다. "
    "loader/gold_cli.py의 검증된 seed publisher 경로를 사용하세요."
)


def main() -> int:
    """비활성화 안내를 출력하고 실패 종료 코드를 반환한다."""
    print(DISABLED_MESSAGE, file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
