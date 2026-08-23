#!/usr/bin/env bash
# NGINX_BASIC_AUTH_USER/NGINX_BASIC_AUTH_PASSWORD로 .htpasswd를 생성(또는 갱신)한다.
#
# 파일 자체는 git에 절대 커밋하지 않는다(.gitignore 참고) — 비밀번호 해시라도
# 저장소 히스토리에 남으면 오프라인 크래킹 대상이 된다. 로컬 개발/운영 모두
# 이 스크립트로 그때그때 생성한다.
set -Eeuo pipefail

readonly USER="${NGINX_BASIC_AUTH_USER:?NGINX_BASIC_AUTH_USER가 필요합니다}"
readonly PASSWORD="${NGINX_BASIC_AUTH_PASSWORD:?NGINX_BASIC_AUTH_PASSWORD가 필요합니다}"
readonly OUT="${1:-$(dirname "$0")/.htpasswd}"

# -B: bcrypt. -n: 파일 대신 stdout에 써서 여기서 리다이렉트한다(-c는 항상
# 새로 만들며 기존 계정을 지우므로 여러 계정을 관리할 땐 append로 다룬다).
htpasswd -Bbn "${USER}" "${PASSWORD}" > "${OUT}"
chmod 600 "${OUT}"
echo "[nginx] ${OUT} 생성 완료 (계정: ${USER})"
echo "[nginx] 계정을 더 추가하려면: htpasswd -B '${OUT}' <추가할 아이디>"
echo "[nginx] 계정을 지우려면:     htpasswd -D '${OUT}' <지울 아이디>"
