"""하이퍼파라미터 프로필을 로컬 JSON 파일에서 읽어 S3(+MLflow 이력)에 올린다.

프로필은 이제 저장소에 커밋된 로컬 파일이 아니라 S3에 산다(`libs/ml_core/
common_config.py`/`profile_registry.py` docstring 참고) — 이 스크립트는 operator가
새 프로필을 만들거나 기존 값을 바꿀 때 쓰는 유일한 진입점이다. 로컬 JSON 파일 자체는
그냥 이 스크립트에 넘길 입력일 뿐, 실행 후엔 아무 의미가 없다(git에 커밋할 필요 없음).

사용:
    uv run python -m ml_core.scripts.push_profile default path/to/profile.json
    uv run python -m ml_core.scripts.push_profile challenger-45min path/to/challenger.json
"""

import argparse
import json
from pathlib import Path

from ml_core.profile_registry import push_profile


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name", help="프로필 이름 (ML_PROFILE 환경변수와 매칭)")
    parser.add_argument("json_path", type=Path, help="프로필 내용이 담긴 로컬 JSON 파일 경로")
    args = parser.parse_args()

    profile = json.loads(args.json_path.read_text(encoding="utf-8"))
    push_profile(args.name, profile)
    print(f"프로필 '{args.name}'을 S3(profiles/{args.name}.json)와 MLflow에 올렸습니다.")


if __name__ == "__main__":
    main()
