#!/usr/bin/env bash
# 실제 과거 ZIP을 현행 Archive·Spark·LightGBM 경로로 관통하는
# 로컬 학습 smoke다.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

: "${SEOUL_OPENAPI_KEY:?SEOUL_OPENAPI_KEY가 .env 또는 환경에 필요합니다.}"

SMOKE_PROJECT="${TRAINING_SMOKE_COMPOSE_PROJECT:-local-training-smoke}"
SMOKE_BUCKET="${TRAINING_SMOKE_BUCKET:-local-training-smoke}"
SMOKE_MINIO_PORT="${TRAINING_SMOKE_MINIO_PORT:-39000}"
SMOKE_MINIO_CONSOLE_PORT="${TRAINING_SMOKE_MINIO_CONSOLE_PORT:-39001}"
SMOKE_POSTGRES_PORT="${TRAINING_SMOKE_POSTGRES_PORT:-35433}"
SMOKE_MLFLOW_PORT="${TRAINING_SMOKE_MLFLOW_PORT:-35000}"
SMOKE_STAGE_ROOT="${TRAINING_SMOKE_STAGE_ROOT:-$ROOT_DIR/data/local-training-smoke}"
SMOKE_ARCHIVE="${TRAINING_SMOKE_ARCHIVE:-$ROOT_DIR/data/아카이브.zip}"
SMOKE_ARCHIVE_DATE="${TRAINING_SMOKE_ARCHIVE_DATE:-$(date -u '+%Y-%m-%d-local-smoke-%H%M%S')}"

SOURCE_START="2025-11-01"
SOURCE_END="2025-11-26"
TRAIN_START="2025-11-02"
TRAIN_END="2025-11-19"

export LOCAL_TRAINING_SMOKE_ALLOW_WRITE=1
export S3_ENDPOINT_URL="http://localhost:${SMOKE_MINIO_PORT}"
export S3_BUCKET="$SMOKE_BUCKET"
export AWS_ACCESS_KEY_ID="${MINIO_ROOT_USER:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${MINIO_ROOT_PASSWORD:-minioadmin}"
export MLFLOW_TRACKING_URI="http://localhost:${SMOKE_MLFLOW_PORT}"
export TRAIN_WINDOW_START="$TRAIN_START"
export TRAIN_WINDOW_END="$TRAIN_END"
export INCREMENTAL_LOOKBACK_HOURS=24
export HORIZON_COUNT=2
export MAX_TRAIN_HORIZON=2
export TRAIN_ANCHOR_TICK_MINUTES=60
export LGB_NUM_BOOST_ROUND=20
export LGB_EARLY_STOPPING_ROUNDS=5
export LGB_NUM_THREADS="${LGB_NUM_THREADS:-4}"
export MODEL_ARCHIVE_DATE="$SMOKE_ARCHIVE_DATE"
export SPARK_MASTER="${SPARK_MASTER:-local[4]}"
export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-6g}"
export SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-4}"

if [[ ! -f "$SMOKE_ARCHIVE" ]]; then
  echo "[training-smoke] Archive ZIP이 없습니다: $SMOKE_ARCHIVE" >&2
  exit 1
fi

COMPOSE=(docker compose -p "$SMOKE_PROJECT" -f ops/compose/docker-compose.yml)
export MINIO_API_PORT="$SMOKE_MINIO_PORT"
export MINIO_CONSOLE_PORT="$SMOKE_MINIO_CONSOLE_PORT"
export POSTGRES_PORT="$SMOKE_POSTGRES_PORT"
export MLFLOW_PORT="$SMOKE_MLFLOW_PORT"

echo "[training-smoke] 격리 MinIO 기동: project=$SMOKE_PROJECT bucket=$SMOKE_BUCKET"
"${COMPOSE[@]}" up -d minio
docker run --rm --network "${SMOKE_PROJECT}_default" --entrypoint sh minio/mc -c \
  "mc alias set local http://minio:9000 '$AWS_ACCESS_KEY_ID' '$AWS_SECRET_ACCESS_KEY' >/dev/null && mc mb --ignore-existing local/'$SMOKE_BUCKET'"
"${COMPOSE[@]}" up -d postgres mlflow

for _ in $(seq 1 60); do
  if curl --fail --silent "$MLFLOW_TRACKING_URI/health" >/dev/null; then
    break
  fi
  sleep 1
done
curl --fail --silent --show-error "$MLFLOW_TRACKING_URI/health" >/dev/null

echo "[training-smoke] 프로젝트 환경 동기화"
for project in collector nowcaster normalizer ml/feature_engine ml/training; do
  uv sync --project "$project" --frozen
done

BOOTSTRAP_DIR="$SMOKE_STAGE_ROOT/bootstrap"
POPULATION_DIR="$SMOKE_STAGE_ROOT/population"
LOG_DIR="$SMOKE_STAGE_ROOT/logs"
mkdir -p "$LOG_DIR"

population_count=0
if [[ -d "$POPULATION_DIR" ]]; then
  population_count="$(find "$POPULATION_DIR" -maxdepth 1 -name '250_LOCAL_RESD_*.csv' -type f | wc -l)"
fi
if [[ ! -f "$BOOTSTRAP_DIR/weather_realtime_2025.csv" || "$population_count" -ne 26 ]]; then
  echo "[training-smoke] ZIP에서 필요한 26일만 선택 staging"
  (
    cd collector
    uv run --frozen python -m bootstrap.zip_stage \
      --zip "$SMOKE_ARCHIVE" \
      --bootstrap-dir "$BOOTSTRAP_DIR" \
      --population-dir "$POPULATION_DIR" \
      --from "$SOURCE_START" \
      --to "$SOURCE_END" \
      --force
  )
else
  echo "[training-smoke] 기존 선택 staging 재사용"
fi

echo "[training-smoke] 실제 원천을 일별 Archive로 적재"
for source in bike_rental_history bike_station_realtime weather_ultra_short_live; do
  (
    cd collector
    uv run --frozen python -m bootstrap \
      --source "$source" \
      --from "$SOURCE_START" \
      --to "$SOURCE_END" \
      --csv-dir "$BOOTSTRAP_DIR"
  ) | tee "$LOG_DIR/bootstrap-${source}.log"
done
(
  cd nowcaster
  uv run --frozen python main.py backfill-archive --csv-dir "$POPULATION_DIR"
) | tee "$LOG_DIR/bootstrap-living-population.log"

echo "[training-smoke] 실제 Archive에서 current station dimension 생성"
(
  cd normalizer
  uv run --frozen python local_training_master.py --source-date "$TRAIN_START"
)

POINTER_BEFORE="$(
  cd ml
  ./training/.venv/bin/python -m training.local_smoke_contract snapshot-pointers
)"

echo "[training-smoke] Spark feature table 생성"
(
  cd ml
  ./feature_engine/.venv/bin/python -m feature_engine.spark.run_pipeline
  ./feature_engine/.venv/bin/python -m feature_engine.spark.build_multi_horizon_features
) | tee "$LOG_DIR/feature-engine.log"

echo "[training-smoke] rental·return prototype 학습"
export TRAIN_PROGRESS_LOG_PATH="$LOG_DIR/rental-training-progress.log"
(
  cd ml
  ./training/.venv/bin/python -m training.train_rental_model
) | tee "$LOG_DIR/rental-training.log"
export TRAIN_PROGRESS_LOG_PATH="$LOG_DIR/return-training-progress.log"
(
  cd ml
  ./training/.venv/bin/python -m training.train_return_model
) | tee "$LOG_DIR/return-training.log"

echo "[training-smoke] artifact bundle과 champion pointer 불변 검증"
(
  cd ml
  ./training/.venv/bin/python -m training.local_smoke_contract verify \
    --archive-date "$SMOKE_ARCHIVE_DATE" \
    --pointer-snapshot "$POINTER_BEFORE"
)

echo "[training-smoke] SUCCESS"
echo "[training-smoke] model archive: models/archive/dt=$SMOKE_ARCHIVE_DATE/builtin-default"
echo "[training-smoke] MLflow: $MLFLOW_TRACKING_URI"
