# 대형 아카이브 ZIP 선택 준비

> **구현됨:** 아래 절차는 `collector/bootstrap/zip_stage.py`와 해당 테스트에 구현되어 있다. 코드 확인일: 2026-08-24.

`data/아카이브.zip` 같은 대형 파일은 통째로 풀지 않는다. 아래 CLI는 ZIP 중앙
디렉터리의 이름·크기만 읽어 요청 기간에 필요한 파일만 고르고, 작은 임시 파일로
스트리밍한 뒤 최종 이름으로 원자적으로 교체한다.

```bash
cd collector
uv run --frozen python -m bootstrap.zip_stage \
  --zip ../data/아카이브.zip \
  --bootstrap-dir ../data/bootstrap-2025-sample \
  --population-dir ../data/population-2025-sample \
  --from 2025-01-01 \
  --to 2025-01-20 \
  --dry-run
```

`--dry-run`은 CSV 본문이나 행을 읽지 않으며 선택 파일 수와 압축 전후 합계 크기만
출력한다. 결과를 확인한 뒤 `--dry-run`을 빼면 실제로 준비한다. 기존 파일이 있으면
기본적으로 중단하며, 의도적으로 교체할 때만 `--force`를 붙인다.

선택 범위는 다음과 같다.

- 범위와 겹치는 달의 `raw/서울특별시 공공자전거 대여이력 정보_YYMM.csv`.
- 범위와 겹치는 달의 `raw_station/data_YYMM.csv`. 출력할 때
  `대여소별 공공자전거 대여가능 수량_YYMM.csv`로 바꾼다.
- `raw_forecast/OBS_ASOS_TIM_*.csv` 정확히 하나. 출력 이름은
  `weather_realtime_YYYY.csv`다.
- 범위에 속한 날짜마다 `raw_people/250m/resd/.../250_LOCAL_RESD_YYYYMMDD.csv`
  정확히 하나. 파일명은 그대로 보존한다.

각 논리 파일이 없거나 둘 이상이면 조용히 임의 파일을 고르지 않고 실패한다. 경로
이동(`..`), 절대 경로, 역슬래시, 심볼릭 링크·특수 파일, 출력 경로 충돌도 실패한다.
macOS ZIP의 UTF-8 플래그 누락/NFD 이름은 UTF-8 복구 후 NFC로 정규화한다.

## ASOS 기간 확인의 한계

ASOS 원본 하나가 요청 기간 전체를 포함하는지는 ZIP 중앙 디렉터리만으로 확정할 수
없다. 이 CLI는 파일이 정확히 하나인지와 안전성만 확인하고 본문을 전수 스캔하지
않는다. 실제 행의 날짜 제한과 범위 밖 행 제거는 뒤의
`weather_ultra_short_live` bootstrap 날짜 필터가 맡는다. 단일 출력 파일 이름이
연도 기준이므로 현재 CLI는 `--from`과 `--to`가 같은 연도인 범위만 허용한다.
