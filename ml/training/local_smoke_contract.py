"""로컬 학습 smoke의 모델 artifact와 pointer 불변을 검증한다."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys

import lightgbm as lgb
from core.s3 import get_object_bytes, list_keys
from ml_core.paths import MODELS_PREFIX, archive_models_prefix

_TARGETS = ("rental", "return")
_BOOSTERS = ("poisson", "q10", "q50", "q90")
_JSON_ARTIFACTS = ("station_categories", "conformal_correction", "metrics", "profile")


def pointer_snapshot() -> dict[str, str]:
    """현재 champion pointer key별 SHA-256을 결정적 사전으로 반환한다."""
    prefix = f"{MODELS_PREFIX}/champion/"
    snapshot: dict[str, str] = {}
    for key in sorted(list_keys(prefix)):
        payload = get_object_bytes(key)
        if payload is not None:
            snapshot[key] = hashlib.sha256(payload).hexdigest()
    return snapshot


def _required_keys(models_prefix: str) -> tuple[str, ...]:
    """대여·반납 smoke archive가 가져야 할 16개 artifact key를 반환한다."""
    return tuple(
        [
            f"{models_prefix}/{target}_{suffix}.txt"
            for target in _TARGETS
            for suffix in _BOOSTERS
        ]
        + [
            f"{models_prefix}/{target}_{kind}.json"
            for target in _TARGETS
            for kind in _JSON_ARTIFACTS
        ]
    )


def verify(archive_date: str, before: dict[str, str]) -> dict[str, object]:
    """모델 archive 전체를 읽고 champion pointer가 바뀌지 않았는지 확인한다."""
    models_prefix = archive_models_prefix(archive_date, "builtin-default")
    payloads: dict[str, bytes] = {}
    for key in _required_keys(models_prefix):
        payload = get_object_bytes(key)
        if not payload:
            raise ValueError(f"필수 smoke artifact가 없습니다: {key}")
        payloads[key] = payload

    for target in _TARGETS:
        for suffix in _BOOSTERS:
            key = f"{models_prefix}/{target}_{suffix}.txt"
            lgb.Booster(model_str=payloads[key].decode("utf-8"))
        for kind in _JSON_ARTIFACTS:
            key = f"{models_prefix}/{target}_{kind}.json"
            document = json.loads(payloads[key])
            if kind == "station_categories" and not document:
                raise ValueError(f"station category가 비었습니다: {key}")
            if kind == "metrics" and document.get("model_name") != target:
                raise ValueError(f"metrics model_name이 다릅니다: {key}")

    after = pointer_snapshot()
    if after != before:
        raise ValueError(f"champion pointer가 변경됐습니다: before={before}, after={after}")
    return {
        "artifact_count": len(payloads),
        "models_prefix": models_prefix,
        "pointer_count": len(after),
        "status": "success",
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """CLI 인자를 파싱한다."""
    parser = argparse.ArgumentParser(description="로컬 학습 smoke 계약을 검증한다.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("snapshot-pointers")
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--archive-date", required=True)
    verify_parser.add_argument("--pointer-snapshot", required=True, type=json.loads)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Pointer snapshot 또는 최종 artifact 검증을 실행한다."""
    args = parse_args(argv)
    try:
        if args.command == "snapshot-pointers":
            result: object = pointer_snapshot()
        else:
            result = verify(args.archive_date, args.pointer_snapshot)
    except (UnicodeDecodeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
