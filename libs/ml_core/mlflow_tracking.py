"""MLflow tracking 공통 설정.

feature_engine/training/inference 세 인스턴스가 서로 다른 서버(EMR, 로컬, 어디든)에서
돌아도 전부 같은 tracking server(ops/compose의 mlflow 서비스)에 기록하도록, `S3_ENDPOINT_URL`/
`DATABASE_URL`과 동일한 패턴으로 `MLFLOW_TRACKING_URI` 환경변수 하나만 맞추면 되게 한다.

그 서버는 `--serve-artifacts`로 띄워서 아티팩트 업로드/다운로드를 서버가 대신
중계한다 — 그래서 클라이언트(이 함수를 호출하는 쪽)는 S3 자격증명을 몰라도 된다.
"""

import os

import mlflow

MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")


def configure(experiment_name: str) -> None:
    """tracking URI를 맞추고 실험(experiment)을 선택한다.

    실제 run 시작/종료(`mlflow.start_run()`/`mlflow.end_run()`)와 로깅
    (`log_params`/`log_metrics`)은 호출부 책임이다 — 여기는 "어느 서버, 어느
    실험으로 기록할지"만 결정한다.
    """
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(experiment_name)
