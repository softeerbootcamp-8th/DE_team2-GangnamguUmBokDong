#!/usr/bin/env bash
# 호스트별로 추가할 로컬 Compose 파일 인자를 출력한다.
set -Eeuo pipefail

host_os="${COMPOSE_HOST_OS_OVERRIDE:-$(uname -s)}"
host_arch="${COMPOSE_HOST_ARCH_OVERRIDE:-$(uname -m)}"

if [[ "${host_os}" == "Darwin" && "${host_arch}" == "arm64" ]]; then
    printf '%s\n' '-f ops/compose/docker-compose.apple-silicon.yml'
fi
