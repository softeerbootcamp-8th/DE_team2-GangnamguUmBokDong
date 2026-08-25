#!/usr/bin/env bash
# EMR classic 클러스터의 모든 노드에서 실행되는 bootstrap action.
#
#   aws emr create-cluster --bootstrap-actions \
#     Path=s3://<bucket>/emr/bootstrap.sh,Args=[<bucket>]
#
# 하는 일 두 가지:
#   1. Python 3.11 + 서드파티 의존성 설치
#   2. 레포 내부 패키지(core, ml_core, feature_engine, training)를 S3에서 내려 /opt/gng에 풀기
#
# EMR Serverless의 venv-pack 방식 대신 이걸 쓰는 이유: 노드 위에서 직접 pip install하므로
# 플랫폼 정합을 신경 쓸 필요가 없다(Serverless는 amazonlinux:2023에서 미리 빌드해야 하고
# 어긋나면 invalid ELF header로 죽는다). classic을 택한 실질적 이점 중 하나다.
#
# ⚠️ Python 3.11이 필요한 이유:
#   ml/feature_engine/spark/run_pipeline.py와 build_merged_table.py가 `str | None`
#   문법을 `from __future__ import annotations` 없이 쓴다 → 3.10 미만에서는 **import
#   자체가 실패**한다. EMR 7.x 기본 Python은 3.9일 수 있어 명시적으로 설치한다.
#
# 이 스크립트와 짝을 이루는 spark-submit 설정(Makefile emr 타겟이 넘긴다):
#   --conf spark.pyspark.python=/usr/bin/python3.11
#   --conf spark.pyspark.driver.python=/usr/bin/python3.11
#   --conf spark.executorEnv.PYTHONPATH=/opt/gng
#   --conf spark.yarn.appMasterEnv.PYTHONPATH=/opt/gng
set -Eeuo pipefail

readonly S3_BUCKET="${1:?[emr-bootstrap] 첫 번째 인자로 S3 버킷 이름이 필요합니다}"
readonly PYFILES_KEY="${2:-emr/pyfiles.tar.gz}"
readonly TARGET_DIR="/opt/gng"

echo "[emr-bootstrap] Python 3.11 설치"
sudo dnf install -y python3.11 python3.11-pip

echo "[emr-bootstrap] 서드파티 의존성 설치"
# 목록은 ml/feature_engine/pyproject.toml + ml/training/pyproject.toml 기준이다.
# requirements.txt에는 pyproj가 빠져 있어(이중 관리 drift) 그쪽을 따르면 런타임에
# ImportError가 난다. boto3는 watermark.py가 Spark가 아니라 plain boto3로 워터마크
# JSON을 읽기 때문에 필요하다. lightgbm/scikit-learn/mlflow-skinny는 training
# 패키지(월간 재학습 evaluation·YARN distributed-shell 학습)가 이 노드에서 직접
# 실행되면서 추가됐다.
sudo python3.11 -m pip install --quiet --upgrade pip
sudo python3.11 -m pip install --quiet \
    pandas \
    pyarrow \
    boto3 \
    holidays \
    pyproj \
    numpy \
    lightgbm \
    scikit-learn \
    "mlflow-skinny>=2.14"

echo "[emr-bootstrap] 레포 패키지 내려받기: s3://${S3_BUCKET}/${PYFILES_KEY}"
# pip 설치가 아니라 PYTHONPATH에 얹는 방식이다. libs/ml_core와 ml/feature_engine이
# `[tool.uv] package = false`인 평탄 레이아웃이라 그대로 복사하는 편이 단순하다.
sudo mkdir -p "${TARGET_DIR}"
aws s3 cp "s3://${S3_BUCKET}/${PYFILES_KEY}" /tmp/pyfiles.tar.gz
sudo tar -xzf /tmp/pyfiles.tar.gz -C "${TARGET_DIR}"
rm -f /tmp/pyfiles.tar.gz

echo "[emr-bootstrap] 설치 확인"
# import까지 확인해서 3.10+ 문법과 의존성 누락을 여기서 잡는다 — 잡이 시작된 뒤
# executor에서 터지면 로그를 뒤지느라 시간이 든다.
sudo env PYTHONPATH="${TARGET_DIR}" python3.11 -c "
import sys
assert sys.version_info >= (3, 11), sys.version
import pandas, pyarrow, boto3, holidays, pyproj, lightgbm, sklearn, mlflow  # noqa: F401
from core import s3  # noqa: F401
from ml_core import common_config, silver_schema  # noqa: F401
from training import config as training_config  # noqa: F401
print('[emr-bootstrap] python', sys.version.split()[0], '+ core/ml_core/training import OK')
"

echo "[emr-bootstrap] 완료."
