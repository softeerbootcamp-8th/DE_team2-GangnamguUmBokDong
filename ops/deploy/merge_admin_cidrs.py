"""admin_cidrs.auto.tfvars에 CIDR을 더하거나 뺀다.

SSM이 이 계정에서 전면 거부되어 SSH가 유일한 접속 수단이고, 22번은
`admin_cidrs`에 있는 CIDR에만 열린다. 이 목록을 덮어쓰면 팀원 한 명이
`make allow-my-ip`를 실행할 때마다 나머지 전원이 끊기므로, 항상 기존 목록을
읽어 합집합/차집합으로 다시 쓴다.

tfvars는 terraform이 읽는 파일이라 사람이 손으로 고치기보다 이 스크립트로
바꾸는 편이 안전하다 — 중복 CIDR이나 잘못된 형식이 들어가면 apply가 늦게
실패한다.
"""

from __future__ import annotations

import ipaddress
import json
import re
import sys
from pathlib import Path

TFVARS = Path(__file__).resolve().parents[2] / "terraform" / "admin_cidrs.auto.tfvars"
_LIST = re.compile(r"admin_cidrs\s*=\s*\[(?P<body>.*?)\]", re.DOTALL)


def _existing() -> list[str]:
    """현재 tfvars의 CIDR 목록을 읽는다. 파일이 없으면 빈 목록이다."""
    if not TFVARS.exists():
        return []
    match = _LIST.search(TFVARS.read_text(encoding="utf-8"))
    if match is None:
        return []
    return [
        item.strip().strip('"')
        for item in match.group("body").split(",")
        if item.strip().strip('"')
    ]


def _validated(value: str) -> str:
    """CIDR 형식을 검증해 canonical 문자열로 돌려준다."""
    try:
        network = ipaddress.ip_network(value, strict=False)
    except ValueError as exc:
        raise SystemExit(f"CIDR 형식이 아닙니다: {value} ({exc})") from exc
    return str(network)


def main(argv: list[str]) -> int:
    """CIDR 하나를 더하거나(-기본) 뺀 뒤 tfvars를 다시 쓴다."""
    remove = "--remove" in argv
    targets = [item for item in argv if not item.startswith("--")]
    if len(targets) != 1:
        print("사용법: merge_admin_cidrs.py [--remove] <CIDR>", file=sys.stderr)
        return 2
    target = _validated(targets[0])
    current = [_validated(item) for item in _existing()]

    if remove:
        updated = [item for item in current if item != target]
        if len(updated) == len(current):
            print(f"admin_cidrs에 없던 항목입니다: {target}", file=sys.stderr)
    else:
        updated = current if target in current else [*current, target]

    ordered = sorted(set(updated), key=lambda item: ipaddress.ip_network(item))
    body = ", ".join(json.dumps(item) for item in ordered)
    TFVARS.write_text(f"admin_cidrs = [{body}]\n", encoding="utf-8")
    print(f"admin_cidrs = [{body}]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
