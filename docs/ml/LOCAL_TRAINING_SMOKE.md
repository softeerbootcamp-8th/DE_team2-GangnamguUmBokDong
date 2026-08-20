# 실제 과거 자료 기반 로컬 학습 smoke

`data/아카이브.zip`의 실제 2025년 자료를 현행 Archive → Spark feature →
LightGBM 경로로 관통해 rental·return 프로토타입 모델을 만드는 개발 검증이다.
모델 품질 개선이나 운영 승격이 목적이 아니며 champion/serving pointer를 변경하지 않는다.

## 실행

`.env`에 `SEOUL_OPENAPI_KEY`가 있어야 한다. 저장소 루트에서 다음 한 명령을 실행한다.

```bash
make training-smoke
```

스크립트는 별도 Compose project `local-training-smoke`와 다음 기본 포트를 사용한다.

- MinIO API/Console: `39000`/`39001`
- Postgres: `35433`
- MLflow: `35000`

포트가 겹치면 `TRAINING_SMOKE_MINIO_PORT`, `TRAINING_SMOKE_MINIO_CONSOLE_PORT`,
`TRAINING_SMOKE_POSTGRES_PORT`, `TRAINING_SMOKE_MLFLOW_PORT`로 바꿀 수 있다.

## 검증 범위

- ZIP staging 원천: 2025-11-01~2025-11-26
- 실제 학습 window: 2025-11-02~2025-11-19
- train: embargo와 평가일을 제외한 9일
- valid: 11일·13일
- test: 17일·19일
- 모델 grid: 20분
- 학습 anchor: 60분
- horizon: 1~2
- LightGBM boosting: 최대 20회
- rental·return 각각 Poisson, P10, P50, P90 모델 생성

운영 기본값인 35일 lookback과 horizon 12는 로컬 smoke에 과도하므로 이 실행에만
lookback 24시간, horizon 2를 사용한다. 적용값은 각 모델의 `*_profile.json`에
기록된다. 학습 명령에는 `--promote-if-no-champion`을 전달하지 않는다.

## 산출물과 확인

모델은 다음 격리 archive에 저장된다.

```text
models/archive/dt=<실행별 고유 ID>/builtin-default/
```

대여·반납별 booster 4개와 `station_categories`, `conformal_correction`, `metrics`,
`profile` JSON까지 총 16개 artifact를 다시 읽어 검증한다. 실행 전후 champion
pointer의 key와 SHA-256도 비교해 변경이 있으면 실패한다.

MLflow는 `http://localhost:35000`에서 학습 파라미터·지표·artifact를 확인할 수 있다.
실행 로그와 peak RSS 기록은 Git에서 제외되는 `data/local-training-smoke/logs/`에 남는다.

## 데이터 사용 방식

ZIP 전체를 풀지 않는다. 필요한 월별 대여·재고·ASOS 파일과 26개의 일별 생활인구
파일만 `data/local-training-smoke/`로 스트리밍한다. `station_master_enriched`는
가짜 ID를 만들지 않고 실제 `bike_station_realtime` Archive의 `stationId`·좌표와
실제 생활인구 CELL 격자를 결합해 만든다.
