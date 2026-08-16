# ML → collector 확인/요청 사항

이번 S3/MinIO 데이터 파이프라인 전환(`feature_engineering`/`training`/`inference`가
로컬 `ml/data/processed_v2/*.parquet` 대신 collector가 Silver로 쌓는 S3 데이터를
직접 읽도록 바꾸는 작업)을 진행하며 `collector` 쪽 확정이 필요한 사항을 정리한다.
`collector` 모듈(어댑터/파이프라인/CLI)은 이번 작업에서 손대지 않았다 — 전부 이
문서로만 남긴다.

## 배경 — 왜 `dev/seed_s3_from_local.py`를 기준으로 개발했는가

`collector`의 실제 수집 어댑터(`collector/adapters/`, `collector/pipeline.py` 등)는
아직 docstring만 있고 실제로 동작하는 코드가 없다 — 소스 YAML config
(`collector/sources/*.yaml`)도 존재하지 않는다. 반면 `dev/seed_s3_from_local.py`는
`ml/data/processed_v2/*.parquet`(기존 로컬 정제 데이터)를 Silver 파티션 구조로
변환해 실제로 MinIO에 넣어주는, 이 저장소에서 유일하게 "실행 가능한" Silver 데이터
생성기다. 그래서 이번 ML 쪽 S3 읽기 로직(`libs/ml_common/silver_schema.py`)은 이
스크립트가 실제로 만드는 스키마를 1차 기준으로 삼았고, `docs/collector/DataSchema.md`
/ `docs/collector/implementation-plan.md`(계획 문서)와 다른 부분은 아래에 따로 남긴다.

## 1. `rent_sta_id`/`rtn_sta_id`가 raw 숫자인지 `"ST-"` 접두 문자열인지

`DataSchema.md`의 `rental` 절(259~276행)은 이 두 컬럼에 대해 "유효한 station 식별
규칙을 만족하지 않음. **물리 FK 적용 여부는 과거 폐쇄 대여소 확인 후 결정**"이라고
적혀 있어, 형식이 공식적으로 아직 미정이다. `dev/seed_s3_from_local.py`는 5자리
zero-pad 숫자 문자열(예: `"02183"`)을 그대로 채워 넣는다.

- **지금 ML이 하는 일**: `station_master`의 `station_no`와 매칭할 때
  `normalize_station_no()`(`libs/ml_common/trip_events.py`)로 숫자만 추출해
  비교한다 — raw 숫자든 `"ST-"` 접두든, 앞에 0이 있든 없든 안전하게 매칭된다.
- **요청**: 실제 수집이 시작되면 어느 형식으로 나가는지 확정해 알려주면
  `normalize_station_no()`가 계속 정상 동작하는지, 아니면 매칭 규칙을 더
  단순화해도 되는지 판단할 수 있다.

## 2. `rental`의 실제 수집 주기 — 계획 문서는 5분, 시딩 스크립트는 1시간

`implementation-plan.md`의 소스 목록(26~33행)에는 "따릉이 대여이력 정보"가 **5분**
주기로 명시돼 있는데, `dev/seed_s3_from_local.py`는 1시간 단위(`hh=HH/HH00.parquet`
1개 파일에 그 시간 전체 트립)로 시뮬레이션한다.

- **지금 ML이 하는 일**: `libs/ml_common/silver_schema.hourly_keys()`가 `rental`도
  weather_forecast/population과 똑같이 "정시 파일 1개" 규칙으로 키를 만든다
  (`ml-integration-requests.md`를 쓰게 된 계기 그 자체).
- **실제로 5분 단위로 나가기 시작하면**: `hourly_keys()`가 아니라
  `bike_realtime_tick_keys()`처럼 5분 tick 전용 키 생성 함수가 필요해지고,
  `inference/predict_single.py`의 `_get_raw_rental_trips()`/
  `_fetch_recent_rental_trips()`가 그 함수를 쓰도록 바꿔야 한다(로직 자체는 트립
  단위 원본을 그대로 합치는 것이라 거의 안 바뀜, 키 생성 부분만 교체).
- **요청**: 실제 수집 주기가 5분으로 확정되면 미리 알려주면 이 부분만 좁게
  고칠 수 있다.

## 3. `weather_forecast`/`living_population_per_population_grid`의 정확한 `source_id`

`implementation-plan.md`의 7개 소스 표(26~33행)에는 "기상청 초단기 실황·예보",
"서울 생활인구(250m)" 같은 한글 설명만 있고, `bike_station_realtime`처럼 실제
`source_id` 문자열(YAML의 `source_id:` 필드, 예시는 210행 `bike_station_realtime`
하나뿐)이 이 둘에 대해서는 계획 문서에 나와 있지 않다.

- **지금 ML이 쓰는 값**: `libs/ml_common/silver_schema.py`의
  `WEATHER_SOURCE_ID = "weather_forecast"`, `POPULATION_SOURCE_ID =
  "living_population_per_population_grid"` — 둘 다 `dev/seed_s3_from_local.py`가
  실제로 쓰는 값을 그대로 가져온 것이다.
- **요청**: 실제 소스 YAML의 `source_id`가 이 두 값과 다르게 확정되면 알려달라 —
  `silver_schema.py` 상수 2줄만 바꾸면 되므로 영향 범위는 작다.

## 4. Silver 읽기 헬퍼 — `read_silver()` 대응 함수가 `collector/storage.py`에 없음

`collector/storage.py`는 `write_silver()`(및 bronze/quarantine 관련 쓰기 함수)는
있지만, 그 반대인 "silver 파티션을 읽어오는" 함수는 없다 — collector 입장에서는
쓰기 전용이라 당연할 수 있지만, ML은 정확히 같은 키 규칙으로 다시 읽어야 해서
`libs/ml_common/silver_schema.py` + `libs/ml_common/s3_io.py`에 **키 생성 규칙을
독립적으로 복제**해 자체 구현했다(`collector`를 import하지 않음 — 서로 다른
인스턴스에 독립 배포되는 모듈이라 의존 관계를 만들지 않는 게 이번 설계 원칙).

- **문제**: 키 규칙(`silver/{source_id}/dt=.../hh=.../HHMM.parquet`)이 두 곳에
  중복돼 있어, `collector` 쪽에서 이 규칙이 바뀌면 ML도 같이 고쳐야 하는데 그
  변경을 ML이 자동으로 알 방법이 없다.
- **요청**: `collector` 쪽에서 이 키 규칙을 바꿀 계획이 있다면 미리 공유해달라.
  장기적으로는 `collector`가 `read_silver(source_id, window_start) -> bytes | None`
  같은 공개 헬퍼를 제공하고 ML이 그걸 그대로 쓰는 편이 중복을 없앨 수 있는데,
  이건 두 모듈 간 의존성을 새로 만드는 결정이라 이번 phase 범위 밖으로 두고
  여기서는 필요성만 남긴다.

## 5. Silver 불변성 — backfill 재작성과 `revision`을 ML이 신경써야 하는가

`implementation-plan.md`(739~754행)에 따르면 **silver는 불변이 아니다** — 백필로
같은 window가 덮어써지고 `manifest.revision`이 올라간다("하류는 `revision`을 보고
멱등 재처리한다"). 그런데 ML의 실시간 조회 함수들
(`_get_recent_weather()`/`_get_recent_bike_status()`/`_get_recent_population()`,
`inference/predict_single.py`)은 매번 그 시각의 silver parquet을 그냥 읽기만 하고
`_manifest/{source_id}/dt=.../hh=.../HHMM.json`의 `revision`/`stage`는 전혀 보지
않는다.

- **지금 괜찮은 이유(추정)**: 추론은 "방금 지난 시각"을 짧은 수명의 프로세스가
  한 번 읽고 끝나는 구조라, 그 시점에 이미 `stage=completed`로 확정된 최신
  silver를 읽을 가능성이 높다 — 학습(`feature_engineering`, 1년치 배치)과 달리
  "이미 백필로 여러 번 바뀐 과거 데이터를 다시 집계"하는 경로가 아니다.
- **확인하고 싶은 것**: 그래도 실시간 추론이 매우 최근(예: 방금 지난 5분) 시각을
  읽을 때, 그 window가 아직 `PARTIAL`이거나 재시도 중이어서 완결되지 않은
  상태로 남아있을 가능성이 있는지, 있다면 ML이 `manifest.stage`/`status`를
  확인하고 넘어가야 하는지 확인 요청 — 만약 필요하다면 `_get_recent_*` 함수들에
  manifest 확인 스텝을 추가해야 한다(현재는 안 함).
