# Collector source 계약 점검

> 상태: 현재 코드 기준 요약<br>
> 최초 API 실측: 2026-08-19<br>
> 코드 재확인: 2026-08-25

이 문서는 외부 API 응답이 Collector를 거쳐 어떤 source 계약으로 저장되는지 설명한다. 정확한 column, type, range, enum과 정책의 최종 기준은 `collector/sources/*.yaml`이다.

## Source 목록

YAML의 `schedule.interval`은 source 자체의 기대 주기를 설명하는 metadata다. 실제 운영 호출 시각은 Airflow DAG와 `airflow/config/schedules.py`가 결정한다.

| Source ID | 외부 service | 실제 운영 주기 | 핵심 처리 |
| --- | --- | --- | --- |
| `bike_rental_history` | 서울 `tbCycleRentData` | 5분 | 해당 시간대 누적 대여·반납 이력, 1시간 replay와 D-6 보강 |
| `bike_station_realtime` | 서울 `bikeList` | 5분 | 빈 페이지까지 탐색, `stationId` 자연키 중복 검증 |
| `population_realtime` | 서울 `citydata_ppltn` | 5분 | POI001~121 병렬 조회, 중첩 예측 12개를 scalar column으로 flatten |
| `weather_ultra_short_live` | 기상청 `getUltraSrtNcst` | 10분 경계 | 34개 격자 병렬 조회, category pivot |
| `weather_ultra_short_forecast` | 기상청 `getUltraSrtFcst` | 10분 경계 | 34개 격자 병렬 조회, category pivot, 강수 범주 변환 |
| `weather_short_term_forecast` | 기상청 `getVilageFcst` | 3시간 경계 | 격자별 다중 page 수집, category pivot, 강수 범주 변환 |
| `living_population_grid` | 서울 `Se250MSpopLocalResd` | 매일 03:00 | 250m 격자 생활인구, `*` 마스킹을 결측으로 처리 |
| `cultural_event` | 서울 `culturalEventInfo` | 매일 03:00 | 문화행사 기간·장소·좌표 검증 |
| `performance_event` | 서울 `stadiumScheduleInfo` | 매일 03:00 | 체육시설 행사 정보 수집 |
| `bike_station_master` | 서울 `bikeStationMaster` | 매일 03:04 | 대여소 ID·주소·좌표 수집 후 별도 enrichment |

날씨 source는 독립 날씨 DAG가 아니라 단일 `realtime_tick`의 source별 freshness gate
뒤에서 필요할 때 실행된다. 실제 기준은 `airflow/dags/realtime_tick.py`다.

## 공통 처리 계약

```text
source YAML load
  → adapter가 API 응답을 part 단위로 fetch
  → part별 Bronze 즉시 저장
  → 수집 완결도 gate
  → adapter normalize
  → column 검증과 policy 적용
  → drop ratio gate
  → immutable Silver·quarantine 저장
  → 진단 manifest
  → authoritative source snapshot manifest
```

어댑터는 API 구조를 평탄화하거나 category를 pivot하지만 값의 type 판단은 하지 않는다. 검증 엔진이 YAML의 `types` 선언 순서대로 casting하고 첫 성공값을 채택한다.

Column은 다음 순서로 판정한다.

1. `None` 또는 빈 문자열이면 `MISSING`
2. 선언 type으로 casting할 수 없으면 `TYPE_ERROR`
3. `range` 또는 `enum`을 벗어나면 `OUTLIER`
4. source·column policy에 따라 유지, null 교체, 보정, 행 폐기 또는 batch 실패

현재 모든 source의 기본 정책은 다음과 같다.

| 상황 | 기본 동작 |
| --- | --- |
| required missing | 행 폐기 |
| required outlier | 행 폐기 |
| optional missing | null 유지 |
| optional outlier/type error | null로 교체 |

일부 column은 YAML의 `on_missing`, `on_outlier`로 기본 정책을 재정의할 수 있다. 실제 적용값은 source YAML을 확인한다.

## 두 개의 품질 Gate

| Gate | 기준 | 초과 시 |
| --- | --- | --- |
| 수집 완결도 | 계획 part·예상 row 대비 누락 비율 | `FAILED/fetch_error`, Silver 미게시 |
| 행 품질 | fetch한 전체 row 대비 폐기 row 비율 | `FAILED/quality_gate`, Silver 미게시 |

`max_missing_ratio`는 column null 비율이 아니라 **수집하지 못한 part 또는 row의 비율**이다. Column별 missing/type/outlier는 manifest의 `column_issues`에 집계되고 `max_drop_ratio`에 영향을 줄 수 있다.

행이 0개일 때 `allow_empty=true`인 문화·공연행사만 정상 `EMPTY`가 될 수 있다. 나머지는 `quality_gate` 실패다.

품질 gate를 통과했지만 일부 part나 row가 빠지면 진단 Silver와 `PARTIAL` manifest를
남길 수 있지만 authoritative source snapshot으로 게시하지 않는다. 생활인구와 두 행사
source는 `max_missing_ratio=0`이라 part 누락을 `PARTIAL`로 허용하지 않는다.

## 서울 열린데이터광장 Adapter

### 오류 분류

| 응답 | 분류 | 동작 |
| --- | --- | --- |
| `INFO-000` | 성공 | 정상 처리 |
| `INFO-200` | 정상 빈 결과 | 빈 part로 처리 |
| `INFO-100` | 치명적 인증 오류 | 같은 실행의 추가 round를 중단하고 실패 |
| `ERROR-5xx`·network 오류 | 일시적 오류 | round 재시도 대상 |
| 그 외 요청 오류 | 영구 오류 | 해당 part 실패 |

API key가 URL path에 들어가므로 예외와 로그에서는 `***`로 가린다.

### Source별 예외 처리

- `bikeList`의 `list_total_count`는 전체 건수가 아니므로 `probe_until_empty`로 빈 page까지 조회한다. 최대 10 page를 넘기면 조용히 truncate하지 않고 실패한다.
- `tbCycleRentData`는 날짜와 시간을 path에 함께 줘야 한다. 5분 tick이 속한 시간의 누적 결과를 받아 Archive compaction에서 중복을 제거한다.
- `citydata_ppltn`은 단일 paged table이 아니라 POI별 endpoint다. POI001~121을 조회하고 `FCST_PPLTN`을 예측시각 순서로 최대 12개 slot에 펼친다.
- 병렬 조회는 source YAML에 `concurrency`가 선언된 source만 사용한다.

## 기상청 Adapter

기상청 세 source는 현재 station과 연결된 동일한 34개 격자를 사용한다. 격자 하나가 하나의 fetch part다.

- 발표 주기에 맞는 가장 최근 base time으로 logical time을 내린다.
- 격자별 category row를 하나의 wide row로 pivot한다.
- `totalCount`가 1,000건을 넘으면 모든 page를 받아 하나의 part로 합친다.
- 한 page라도 실패하면 그 격자 전체를 실패 처리하여 다음 round에서 처음부터 다시 받는다.
- 초단기예보 `RN1`과 단기예보 `PCP`의 범주형 강수 표현은 공통 `precip` caster로 mm 실수로 바꾼다.
- 초단기실황 `RN1`은 이미 숫자이므로 일반 float로 처리한다.

본문 `resultCode=00`만 성공이다. 미발표·연결·quota 계열은 일시 오류, 인증·권한 계열은 치명적 오류로 분류한다.

## 재개와 즉시 복구

Bronze가 저장된 뒤 Silver 쓰기나 품질 검증이 실패하면 다음 실행은 외부 API를 다시
호출하지 않고 같은 Bronze를 재사용한다. 저장 실패 때문에 이미 확보한 원본을 잃지
않기 위한 계약이다.

`fetch_error`는 source별 `fetch.retry_mode`로 복구한다.

- 서울 current/mutable source의 기본값 `refetch_all`: transient round와 같은-window
  Airflow retry 모두 기존 부분본을 버리고 전체를 다시 받는다.
- 기상청 3종의 `retry_missing`: logical window가 발표 시각과 34개 grid key를 고정하므로
  성공 grid를 유지하고 누락 grid만 다시 받는다.

생활인구·행사·대여소 마스터 같은 일일 current-only source의 자동 fetch retry는 같은
KST 날짜에만 허용한다. 다음 날 과거 task를 clear해도 현재 응답을 옛 logical window에
쓰지 않는다. Airflow는 retry 횟수·간격을, Collector는 전체/누락 재수집을 담당한다.

현재 운영 source에는 delayed `backfill` 설정이 없고 범용 Backfill DAG도 없다.
`--backfill`, `_retry_queue`와 manifest 필드는 기존 코드·저장 포맷 파싱 호환을 위해
남아 있지만 기존 marker는 발견·소비되지 않는 정리 대상이다.

`--force`는 기존 Bronze부터 지우고 전체를 다시 받는 명령이며 `--backfill`과 동시에 사용할 수 없다.

## Archive 대상

현재 daily compaction 대상은 다음 세 source다.

- `bike_rental_history`
- `bike_station_realtime`
- `weather_ultra_short_live`

예보 source는 사후 재현 가치가 낮아 compaction하지 않는다. 행사와 일별 생활인구는 하루 1개 window라 작은 파일을 다시 묶을 필요가 없다.

Archive는 source YAML에서 만든 고정 schema와 `_row_status`, `_window_start`, `_source_kind`를 가진다. 원본 Silver는 삭제하지 않는다.

## 현재 알려진 데이터 해석 경계

- 생활인구의 `*`는 type error가 아니라 비식별 마스킹에 의한 결측이다.
- 실시간 인구의 forecast slot 번호는 “n시간 후”가 아니다. 반드시 각 `FCST_n_TIME` 값으로 target time을 해석한다.
- 따릉이 대여이력은 반납 완료 뒤 API에 나타나므로 최초 5분 수집만으로 완전하지 않다. 1시간 replay와 D-6 재수집으로 보강한다.
- `bike_station_realtime`에서는 현재 자전거 수가 거치대 수보다 클 수 있다. 따릉이는 거치대 밖 주차가 가능하므로 두 column의 단순 대소 관계를 품질 오류로 두지 않는다.
- 선언되지 않은 raw field는 Silver에 저장되지 않는다. 새 소비자가 field를 필요로 하면 source YAML과 downstream 계약을 함께 변경해야 한다.

## 변경 시 검증

Source YAML이나 adapter를 바꿀 때 최소 다음 테스트를 실행한다.

```bash
uv run --project collector --frozen pytest \
  collector/tests/test_source_configs.py \
  collector/tests/test_api_contracts.py \
  collector/tests/test_seoul_openapi.py \
  collector/tests/test_kma_apihub.py \
  collector/tests/test_pipeline.py -q
```

외부 API의 실시간 응답은 unit test만으로 보장할 수 없다. service 이름, raw field와 pagination 범위를 바꿀 때는 유효한 key를 사용한 별도 실측도 필요하다.

## 코드 기준 위치

- source 계약: `collector/sources/*.yaml`
- 설정 검증: `collector/config/schema.py`, `collector/config/loader.py`
- API adapter: `collector/adapters/seoul_openapi.py`, `collector/adapters/kma_apihub.py`
- 수집·품질 gate·재개: `collector/pipeline.py`
- column·row 검증: `collector/validation/`
- manifest와 authority: `collector/manifest.py`
- Archive: `collector/compaction.py`
