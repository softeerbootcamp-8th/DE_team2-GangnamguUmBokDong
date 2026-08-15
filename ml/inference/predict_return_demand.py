"""반납 수요 추론 CLI (exposure 미적용). 사용법은 predict_rental_demand.py와 동일."""

from . import config
from .predict_common import run_predict_cli

OUTPUT_PARQUET = f"{config.PROCESSED_V2_PREFIX}/predictions_return_test.parquet"


def main():
    """CLI 인자를 파싱해 반납 수요를 예측한다.

    returns:
        pd.DataFrame: run_predict_cli()의 결과
    """
    return run_predict_cli("return", "return_count", None, OUTPUT_PARQUET)


if __name__ == "__main__":
    main()
