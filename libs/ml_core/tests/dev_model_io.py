"""stage_and_upload_booster()가 항상 S3에 업로드하고, log_to_mlflow=True일 때만
같은 임시 파일을 mlflow.log_artifact()로도 남기는지 검증한다.

lgb.Booster를 실제로 학습시키는 대신 `save_model(path)`만 구현한 가짜 객체를
쓴다 — 이 함수는 그 메서드 하나만 호출하므로 duck-typing으로 충분하다.
"""

from core import s3 as s3_io

from ml_core import model_io


class _FakeBooster:
    def save_model(self, path: str) -> None:
        with open(path, "w") as f:
            f.write("fake booster bytes")


def test_always_uploads_to_s3():
    model_io.stage_and_upload_booster(_FakeBooster(), "models/test/booster.txt")

    assert s3_io.get_object_bytes("models/test/booster.txt") == b"fake booster bytes"


def test_logs_to_mlflow_only_when_requested(monkeypatch):
    calls = []
    monkeypatch.setattr(model_io.mlflow, "log_artifact", lambda path, artifact_path=None: calls.append(artifact_path))

    model_io.stage_and_upload_booster(_FakeBooster(), "models/test/booster2.txt", log_to_mlflow=True)

    assert calls == ["models"]


def test_skips_mlflow_by_default(monkeypatch):
    calls = []
    monkeypatch.setattr(model_io.mlflow, "log_artifact", lambda path, artifact_path=None: calls.append(artifact_path))

    model_io.stage_and_upload_booster(_FakeBooster(), "models/test/booster3.txt")

    assert not calls
