"""CLI 진입점 — 인자 파싱 후 pipeline을 호출한다.

구현 예정: docs/collector/implementation-issues.md #8
설계 근거: docs/collector/implementation-plan.md 9절 (실행 인터페이스)

## 실행 방법

wheel 빌드 없이 실행한다. 하위 디렉토리는 `sys.path[0] == collector/`라 그대로
import된다.

    cd collector
    uv run python main.py --source bike_station_realtime \
        --window-start 2026-08-12T14:10:00Z [--force]

Airflow는 소스별 태스크에서 `data_interval_start`를 `--window-start`로 넘긴다.

## 구현할 것

- 인자 — `--source`(필수) · `--window-start`(필수, ISO8601 Z) · `--force`
- `window_end`는 config의 `schedule.interval`로 계산한다. **collector 자체는 스케줄을
  모른다.**
- 처리 순서 — 인자 파싱 → 로깅 초기화 → config 로드 → pipeline 실행 → 종료 코드 반환.
  로깅 초기화가 pipeline보다 **먼저**여야 고정 필드가 모든 로그에 붙는다.
- 종료 코드 — `SUCCEEDED` · `PARTIAL` · `EMPTY` · `SKIPPED`는 0,
  `FAILED`는 non-zero. Airflow 태스크 실패로 이어져야 한다. `PARTIAL`을 0으로 두는
  것은 `max_drop_ratio` 이내를 정상으로 본다는 설계 결정이다.

## 미결정

- `--window-start`가 주기 경계에 맞는지 검사할지(5분 주기에 `14:12`가 들어온 경우).
  경계 정렬 검증을 넣을지는 #8에서 정한다.

## 주의

- 스택 트레이스를 그대로 뱉지 않는다. 실패는 manifest에 남기고 정리된 메시지와 종료
  코드로 전달한다.
- 인증키를 인자로 받지 않는다. 키는 환경변수(`SEOUL_OPENAPI_KEY` ·
  `KMA_APIHUB_KEY`)에서만 읽는다.
"""
