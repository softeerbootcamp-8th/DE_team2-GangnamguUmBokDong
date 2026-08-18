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
