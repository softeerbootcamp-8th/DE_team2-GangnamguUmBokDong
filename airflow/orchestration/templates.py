"""DAG run의 논리 시각(logical_date)을 KST 문자열로 바꾸는 단일 소스.

collector/storage.py와 loader/s3_reader.py는 window_start 객체가 들고 있는
시·분을 UTC로 정규화하지 않고 그대로 `%H`/`%HHMM`으로 S3 키에 찍는다. 같은 DAG
실행 안에서 서로 다른 오프셋(KST vs UTC)으로 표현된 같은 시각이 서로 다른 태스크에
전달되면, 그 태스크들이 서로 다른 S3 키를 계산해 조용히 어긋난다. 이를 막기 위해
모든 태스크 빌더는 이 파일의 상수만 가져다 쓰고, 각자 시간대 변환을 다시 하지
않는다 — KST 오프셋(+09:00)만 파이프라인 전체에서 사용한다.

4개 DAG 모두 CronTriggerTimetable(시간 기반) 스케줄이라 logical_date가 항상
채워진다.
"""

KST_WINDOW_START = '{{ (dag_run.logical_date or dag_run.start_date).astimezone(macros.dateutil.tz.gettz("Asia/Seoul")).isoformat() }}'
KST_DATE = '{{ (dag_run.logical_date or dag_run.start_date).astimezone(macros.dateutil.tz.gettz("Asia/Seoul")).strftime("%Y-%m-%d") }}'
KST_HOUR = '{{ (dag_run.logical_date or dag_run.start_date).astimezone(macros.dateutil.tz.gettz("Asia/Seoul")).hour }}'
KST_MINUTE = '{{ (dag_run.logical_date or dag_run.start_date).astimezone(macros.dateutil.tz.gettz("Asia/Seoul")).minute }}'
