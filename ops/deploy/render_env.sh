#!/usr/bin/env bash
# S3의 설정 객체를 내려받아 /opt/app/.env를 만든다.
#
# 시크릿을 SSM Parameter Store나 Secrets Manager에 두지 않는 이유는 두 서비스가 모두
# 계정 정책상 거부이기 때문이다(2026-08-21 정찰). S3 + SSE-KMS로 대체하고, 접근은
# 인스턴스 역할로 통제한다.
#
# 객체가 둘로 나뉘어 있다:
#   config/prod.env     Terraform이 생성 (DB 연결·생성된 시크릿·버킷 등)
#   config/secrets.env  사람이 직접 업로드 (SEOUL_OPENAPI_KEY, KMA_APIHUB_KEY)
# API 키를 분리한 이유는 Terraform 코드·tfvars·tfstate 어디에도 남기지 않기 위해서다.
set -Eeuo pipefail

readonly S3_BUCKET="${S3_BUCKET:?[render-env] S3_BUCKET이 필요합니다}"
readonly CONFIG_KEY="${CONFIG_KEY:-config/prod.env}"
readonly SECRETS_KEY="${SECRETS_KEY:-config/secrets.env}"
readonly ENV_PATH="${ENV_PATH:-/opt/app/.env}"

# .env에 들어가면 안 되는 키.
#
# AWS_ACCESS_KEY_ID/SECRET/SESSION_TOKEN: boto3는 환경변수 자격증명이 있으면
#   credential chain에서 **EC2 instance profile을 아예 조회하지 않는다**. 하나라도
#   섞이면 S3 접근이 전부 403이 되는데, 증상이 "왜 IAM Role이 안 먹지"로 보여서
#   원인 파악에 몇 시간이 든다.
# S3_ENDPOINT_URL: MinIO 주소가 남아 있으면 core.s3가 실제 S3 대신 그쪽으로 간다.
#   운영에서는 비어 있어야 하며(`or None` 처리), 아예 넣지 않는다.
readonly DENYLIST='^(AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN|S3_ENDPOINT_URL)='

TMP_ENV="$(mktemp)"
readonly TMP_ENV
trap 'rm -f "${TMP_ENV}"' EXIT

fetch() {
    local key="$1" required="$2"
    if aws s3 cp "s3://${S3_BUCKET}/${key}" - 2>/dev/null; then
        return 0
    fi
    if [[ "${required}" == "required" ]]; then
        echo "[render-env] 필수 객체를 읽을 수 없습니다: s3://${S3_BUCKET}/${key}" >&2
        echo "[render-env] terraform apply가 끝났는지, 인스턴스 역할에 kms:Decrypt가 있는지 확인하세요." >&2
        exit 66
    fi
    echo "[render-env] 선택 객체 없음(건너뜀): s3://${S3_BUCKET}/${key}" >&2
    return 0
}

{
    fetch "${CONFIG_KEY}" required
    echo
    fetch "${SECRETS_KEY}" optional
} > "${TMP_ENV}"

# denylist에 걸린 줄이 있으면 조용히 지우지 않고 알린다 — 누가 왜 넣었는지 알아야 한다.
if denied="$(grep -E "${DENYLIST}" "${TMP_ENV}" | cut -d= -f1)"; [[ -n "${denied}" ]]; then
    echo "[render-env] 아래 키는 instance profile 인증을 깨뜨리므로 제외합니다:" >&2
    while IFS= read -r denied_key; do
        echo "[render-env]   - ${denied_key}" >&2
    done <<< "${denied}"
fi

install -d -m 700 "$(dirname "${ENV_PATH}")"
umask 077
grep -Ev "${DENYLIST}" "${TMP_ENV}" > "${ENV_PATH}"
chmod 600 "${ENV_PATH}"

# 숫자를 포함해야 한다 — S3_BUCKET 같은 키를 빠뜨리면 개수가 틀리게 보고된다.
KEY_COUNT="$(grep -cE '^[A-Z0-9_]+=' "${ENV_PATH}" || true)"
readonly KEY_COUNT
echo "[render-env] ${ENV_PATH} 생성 완료 (키 ${KEY_COUNT}개, 0600)"

# 운영에 반드시 있어야 하는 키가 빠지면 컨테이너가 기동 중에야 실패한다. 여기서 미리 잡는다.
missing=()
for required_key in DATABASE_URL AIRFLOW__DATABASE__SQL_ALCHEMY_CONN MLFLOW_BACKEND_STORE_URI \
                    AIRFLOW_JWT_SECRET S3_BUCKET SEOUL_OPENAPI_KEY KMA_APIHUB_KEY; do
    grep -qE "^${required_key}=." "${ENV_PATH}" || missing+=("${required_key}")
done

if (( ${#missing[@]} > 0 )); then
    echo "[render-env] 값이 비었거나 없는 키: ${missing[*]}" >&2
    echo "[render-env] SEOUL_OPENAPI_KEY/KMA_APIHUB_KEY는 사람이 올려야 합니다:" >&2
    echo "[render-env]   aws s3 cp secrets.env s3://${S3_BUCKET}/${SECRETS_KEY} --sse aws:kms" >&2
    exit 78
fi

echo "[render-env] 필수 키 확인 완료."
