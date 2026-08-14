"""대여 수요 추론 CLI.

사용 예:
  ./.venv/bin/python -m inference.predict_rental_demand
      # 기본값: 2025-12(테스트 기간) 전체 정류소 예측

  ./.venv/bin/python -m inference.predict_rental_demand --station-id ST-1234 --start-date 2025-06-01 --end-date 2025-06-07
      # 특정 정류소, 특정 기간만 (2025년 범위 내에서)

  ./.venv/bin/python -m inference.predict_rental_demand --station-id ST-1234 --start-date 2025-06-01 --end-date 2025-06-01 --hour 8
      # 특정 정류소의 특정 시각 하나만

  ./.venv/bin/python -m inference.predict_rental_demand --station-ids ST-1234,ST-5678,ST-9012 --start-date 2025-06-01 --end-date 2025-06-01 --hour 8
      # 정류소 여러 개를 한 번에 배치 예측 (쉼표로 구분, --station-id와 동시 사용 불가)

station_id/기간이 42행 이하로 좁혀지면 표로 바로 출력하고, 그보다 크면 요약 지표만
찍고 전체 결과는 parquet로 저장한다.
"""

from . import config
from .predict_common import run_predict_cli

OUTPUT_PARQUET = config.PROCESSED_V2_DIR / "predictions_rental_test.parquet"


def main():
    """CLI 인자를 파싱해 대여 수요를 예측한다.

    returns:
        pd.DataFrame: run_predict_cli()의 결과
    """
    return run_predict_cli("rental", "rental_count", "rental_exposure", OUTPUT_PARQUET)


if __name__ == "__main__":
    main()
