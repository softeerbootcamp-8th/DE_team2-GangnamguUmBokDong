"""DAG run의 논리 시각(logical_date)을 KST 문자열로 바꾸는 단일 소스.

collector/storage.py와 loader/s3_reader.py는 window_start 객체가 들고 있는
시·분을 UTC로 정규화하지 않고 그대로 `%H`/`%HHMM`으로 S3 키에 찍는다. 같은 DAG
실행 안에서 서로 다른 오프셋(KST vs UTC)으로 표현된 같은 시각이 서로 다른 태스크에
전달되면, 그 태스크들이 서로 다른 S3 키를 계산해 조용히 어긋난다. 이를 막기 위해
모든 태스크 빌더는 이 파일의 상수만 가져다 쓰고, 각자 시간대 변환을 다시 하지
않는다 — KST 오프셋(+09:00)만 파이프라인 전체에서 사용한다.

수동 trigger의 logical_date는 19:33처럼 5분 경계가 아닐 수 있다. 모든
컴포넌트가 같은 Silver key를 사용하고 inference의 5분 그리드 계약을
만족하도록 공통 기준 시각을 5분 단위로 내림한다.
"""

_KST_RUN_TS = '(dag_run.logical_date or dag_run.start_date).astimezone(macros.dateutil.tz.gettz("Asia/Seoul"))'
_KST_WINDOW_TS = f"{_KST_RUN_TS}.replace(minute=({_KST_RUN_TS}.minute // 5) * 5, second=0, microsecond=0)"

KST_WINDOW_START = "{{ " + _KST_WINDOW_TS + ".isoformat() }}"
KST_DATE = "{{ " + _KST_WINDOW_TS + '.strftime("%Y-%m-%d") }}'
KST_HOUR = "{{ " + _KST_WINDOW_TS + ".hour }}"
KST_MINUTE = "{{ " + _KST_WINDOW_TS + ".minute }}"


def kst_date_days_ago(days: int) -> str:
    """공통 기준 시각에서 `days`일 전의 KST 날짜 템플릿을 반환한다."""
    if days <= 0:
        raise ValueError(f"days는 양수여야 한다: {days}")
    shifted = f"({_KST_WINDOW_TS} - macros.timedelta(days={days}))"
    return "{{ " + f'{shifted}.strftime("%Y-%m-%d")' + " }}"


def kst_day_hour_replay_days_ago(days: int, hour: int) -> str:
    """`days`일 전 `hour`시의 마지막 5분 윈도우를 KST 템플릿으로 반환한다.

    과거 시각을 다시 호출하므로 같은 시간대의 어느 5분 윈도우든 API 응답은 같다.
    H:55를 사용하면 기존 마지막 스냅샷을 덮어쓰면서 결과가 대상 날짜의 파티션에
    남는다. H+1:00을 쓰면 23시 결과가 다음 날짜 파티션으로 넘어가므로 피한다.
    """
    if days <= 0:
        raise ValueError(f"days는 양수여야 한다: {days}")
    if not 0 <= hour <= 23:
        raise ValueError(f"hour는 0~23이어야 한다: {hour}")
    window_start = (
        f"({_KST_WINDOW_TS}.replace(hour={hour}, minute=55, second=0, microsecond=0) "
        f"- macros.timedelta(days={days}))"
    )
    return "{{ " + f"{window_start}.isoformat()" + " }}"


def kst_window_start_shifted(hours: int) -> str:
    """공통 기준 시각을 `hours`시간 앞으로 당긴 `--window-start` 템플릿.

    과거 윈도우를 다시 수집할 때 쓴다. 위 상수들과 같은 `_KST_WINDOW_TS`(5분 내림한
    KST 시각)에서 출발하므로 시간대 변환이 두 갈래로 갈리지 않는다.

    시간을 통째로 당기는 것이 중요하다. collector가 `path_suffix`를 계산할 때 쓰는
    `window_last`(= window_start - 1초)도 같이 당겨지므로 그 시간대를 조회하게 되고,
    silver도 그 시간대의 `dt`/`hh` 파티션에 쓰인다 — 데이터의 시각과 파티션이
    어긋나지 않는다.

    args:
        hours: 몇 시간 앞으로 당길지. 양수여야 한다.
    returns:
        `--window-start`에 그대로 넣을 Jinja 템플릿 문자열.
    """
    if hours <= 0:
        raise ValueError(f"hours는 양수여야 한다: {hours}")
    shifted = f"({_KST_WINDOW_TS} - macros.timedelta(hours={hours}))"
    return "{{ " + f"{shifted}.isoformat()" + " }}"
