# Gold 스키마는 de-project ERD를 기준으로 PostGIS 공간 모델을 사용한다

## 상태

제안

## 배경

현재 Gold는 PostgreSQL을 사용하지만 PostGIS extension과 공간 타입이 없다. 대여소와
행사 장소는 위도·경도를 각각 저장하고, 인근 행사 거리는 API의 Python Haversine
함수로 계산하며, 자치구 판정은 Loader의 Shapely에 의존한다. 날씨 격자 좌표와
자치구명도 사실 테이블마다 반복된다.

이 구조는 공간 연산을 데이터베이스에 맡기기 위해 PostgreSQL을 선택한 이유와 맞지
않고, 팀이 `de-project/docs/프로젝트 관리/ERD.md`와
`de-project/docs/data_dictionary.md`에서 합의한 마스터·관계·표준 단어도 충분히
반영하지 못한다.

운영 RDS는 아직 배포하지 않았다. 따라서 임시 스키마의 변경 이력을 운영
마이그레이션으로 누적하지 않고, 목표 구조를 새로운 운영 베이스라인으로 만든다.
기존 로컬 볼륨은 개발 데이터이므로 목표 구조 적용 시 재생성할 수 있다.

## 결정

### 공간 데이터

- PostgreSQL 16과 호환되는 PostGIS 이미지를 고정하고 `postgis` extension을 멱등
  활성화한다.
- 영속 좌표의 기준은 EPSG:4326 `geometry`다.
- 점은 `geometry(Point, 4326)`, 행정구역은 실제 원천의 다중 면을 보존할 수 있도록
  `geometry(MultiPolygon, 4326)`을 사용한다.
- `lat`/`lon`은 별도 컬럼으로 저장하지 않는다. API 호환 응답에서만
  `ST_Y(point)`, `ST_X(point)`로 파생한다.
- 반경 검색은 Point를 `geography`로 변환한 `ST_DWithin`을 사용해 미터 단위로
  계산한다. 필요한 경우 같은 표현식에 GiST 인덱스를 둔다.
- 좌표 순서는 X=경도, Y=위도로 고정한다.

### 도메인 구조

- de-project의 `gu_master`, `dong_master`, `weather_grid`, `station`, `event_spot`,
  `event` 관계를 Gold의 기본 뼈대로 사용한다.
- 대여소는 행정동과 날씨 격자를 FK로 참조하고, 구명과 격자 번호를 반복 저장하지
  않는다.
- 행사 정보와 행사 장소를 분리한다. 여러 행사가 같은 장소를 공유할 수 있다.
- 현재 서비스에 필요한 실황 날씨는 `weather_observation`, 예보 날씨는
  `weather_forecast`로 분리해 유지한다.
- 서비스 전용 모델인 `station_demand_forecast`, `station_urgency`,
  `rebalance_route`, `rebalance_route_stop`을 유지한다.
- 코드에만 존재하던 11개 배차 센터는 `dispatch_center` 공간 마스터로 승격한다.
- 현재 Gold 소비자가 없는 생활인구·대여이력·공통코드 테이블은 이번 베이스라인에
  넣지 않는다. Gold 소비 요구가 생길 때 de-project ERD를 기준으로 추가한다.

### 시간과 이름

- DB 일시 타입은 `TIMESTAMPTZ`, 저장·비교 기준은 UTC, 화면 표시는 KST로 한다.
- 날짜는 `_dt`, 일시는 `_dttm`으로 끝낸다. `_at`, `_date`, `_time`은 사용하지 않는다.
- de-project 표준 접미사 `_id`, `_nm`, `_no`, `_cnt`, `_rate`, `_lv`, `_cd`,
  `_min`, `_dst`, `_point`, `_polygon`을 사용한다.
- 테이블명은 de-project와 같이 단수형으로 통일한다.
- `base_dttm`은 레코드가 표현하는 관측·계산·발표의 기준 일시로 사용한다.
  예측 대상 시각은 `predicted_dttm` 또는 `forecast_dttm`으로 구분한다.
- 외부 식별자를 보존해야 하는 `sta_id`, `event_id`, `event_spot_id`는 `TEXT`를
  유지한다. de-project 초안의 `station.sta_id INTEGER`는 현재 원천 계약과 맞지 않아
  따르지 않는다.

### 재고 보존 정책

- 운영 Gold의 `station_stock`은 `sta_id`당 최신 1건만 보관한다.
- 관측 이력의 영구 원본은 S3 Silver가 담당한다.
- #108/PR #138이 병합되면 해당 정책을 그대로 목표 스키마에 적용한다.

## 결과

- 공간 데이터의 단일 진실 공급원이 PostGIS 컬럼으로 통일된다.
- 자치구·행정동·격자·배차 센터 관계를 FK와 공간 검증으로 관리할 수 있다.
- 인근 행사 및 공간 귀속 로직을 Python에서 SQL로 옮길 수 있다.
- DB, Loader, API의 이름이 같은 데이터 사전을 사용한다.
- 전환 시 DB뿐 아니라 Loader 설정, Airflow 태스크 인자, API 쿼리, 테스트와 seed를
  함께 바꿔야 한다.

목표 관계와 컬럼은 `docs/gold/target-erd.md`, 표준 단어와 컬럼 의미는
`docs/gold/data-dictionary.md`를 따른다.

