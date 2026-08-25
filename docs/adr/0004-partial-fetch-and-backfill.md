# ADR-0004: 누락 조각은 분류해 재시도하고 허용 범위 안에서 부분 처리한다

- 상태: 수정됨 (2026-08-25)
- 결정일: 2026-08-13
- 작성자: Data Engineering 2팀
- 대체 대상: ADR-0003의 순번 key와 첫 실패 즉시 중단 결정
- 대체한 ADR: 없음

> **2026-08-25 수정:** 아래 1·2절의 오류 분류와 품질 gate는 유지한다. 운영 source는
> delayed backfill marker를 만들지 않는다. 같은 실행의 transient 다음 round는 기존
> 성공 조각을 유지하고 누락 조각만 재시도하며, 같은-window Airflow 재실행은 manifest가
> 가리키는 Bronze를 재사용한다. 관련 CLI와 manifest 필드는 파싱 호환용 legacy다.

## 배경

여러 조각 중 하나가 일시적으로 실패했을 때 window 전체를 즉시 중단하면 이미 받은 실시간 원본까지 활용하지 못한다. 반면 받은 조각만 조용히 성공으로 처리하면 데이터 누락을 감지할 수 없고, 과거 시점을 조회할 수 없는 snapshot source에 나중 데이터를 채우면 서로 다른 시점이 섞인다.

누락은 재호출로 회복할 수 있지만 검증에서 폐기된 행은 정책이나 원천 품질의 문제다. 두 손실을 같은 기준으로 판정하면 재시도 가능한 실패와 설정을 수정해야 하는 실패를 구분할 수 없다.

## 결정

### 1. 실패 종류에 따라 최대 3라운드로 수집한다

- `TRANSIENT`: timeout, HTTP 429와 5xx는 15초·30초 간격으로 다음 라운드에서 재시도한다.
- `PERMANENT`: HTTP 400과 404는 해당 조각만 누락으로 확정한다.
- `FATAL`: HTTP 401과 403 같은 인증 오류는 남은 호출과 라운드를 즉시 중단한다.

`TRANSIENT` 다음 round는 source 종류와 관계없이 앞 round의 성공 조각을 유지하고
누락 조각만 다시 요청한다.

각 window의 전체 fetch에는 명시한 `fetch.budget`을 적용하고, 없으면 수집 주기의 절반과 30분 중 작은 값을 사용한다. Adapter가 전체 요청 목록을 아는 경우 `planned_parts`를 제공해 budget 전에 시작하지 못한 요청도 누락으로 기록한다.

### 2. 수집 누락과 검증 폐기를 독립적으로 판정한다

`max_missing_ratio`는 fetch 단계의 누락을, `max_drop_ratio`는 validation 단계의 폐기를 판정한다. 누락 비율이 허용 범위 안이면 성공 조각을 정규화·검증해 Silver를 만들고 `PARTIAL`로 기록하며, 초과하면 Silver 없이 `FAILED/fetch_error`로 끝낸다.

검증 폐기 비율의 분모는 실제 수집 행 수다. 폐기 비율이 임계치를 넘으면 Quarantine은 남기되 Silver는 쓰지 않고 `FAILED/quality_gate`로 끝낸다. 최종 `completeness`와 누락 key는 진단 manifest에 기록한다.

`PARTIAL`의 프로세스 종료 코드는 0이므로 Airflow는 downstream task를 스케줄한다.
그러나 PARTIAL Silver는 source authority가 아니며, 실제 데이터 사용은 소비자가
명시적으로 허용한 경우에만 가능하다. 현재 source별 처리는 다음과 같다.

- `population_realtime`: Normalizer가 검증된 exact PARTIAL을 보정 입력으로 허용한다.
- `living_population_grid`: Nowcaster는 PARTIAL을 actual Archive로 승격하지 않지만 기존
  Archive를 이용한 `D-3..D+3` 추정은 계속한다.
- 문화·공연행사: 기존 `publication_state`와 그 content-addressed publication manifest가
  일치하면 Gold 행과 state를 변경하지 않는다. 유지할 state가 없거나 manifest가
  일치하지 않으면 실패한다.
- 그 밖의 authority 기반 소비자는 별도 허용 정책이 없으면 PARTIAL을 입력으로 쓰지 않는다.

### 3. backfill은 시간 일관성을 지킬 수 있는 source에서만 수행한다 (대체됨)

source config의 `backfill.enabled`와 `max_age`로 허용 여부와 기간을 제한한다. 불완전한 실행은 `_retry_queue/{source_id}/{window_start}.json` marker로 찾되, 실제 대상 여부는 manifest를 다시 확인한다.

Backfill은 기존 Bronze를 유지하고 누락 key만 요청한 뒤 window 전체를 다시 처리한다. Silver는 checksum을 포함한 새로운 immutable object로 기록하며, 완전한 `SUCCEEDED` 또는 확인된 `EMPTY` 결과만 source authority manifest의 다음 revision으로 게시한다. 동일 내용 재실행은 기존 authority revision을 재사용한다.

`max_age`가 지나면 marker를 제거하고 진단 manifest의 `backfill_status`를 `expired`로 남긴다. 현재 시점만 반환하는 snapshot source는 backfill을 활성화하지 않는다.

## 근거

- 라운드 재시도는 일시적인 API 장애를 회복하면서 확정 오류의 불필요한 재호출을 막는다.
- 두 품질 게이트를 분리하면 운영자가 API 재시도와 validation 정책 수정을 구분할 수 있다.
- 요청 parameter 기반 key는 특정 조각만 안전하게 보완하고 병렬 호출에도 동일한 identity를 유지한다.
- marker는 실패한 window만 빠르게 찾게 하고 manifest를 진실 공급원으로 유지한다.
- immutable Silver와 authority revision은 backfill correction이 기존에 게시된 정상 결과를 제자리 덮어쓰지 않게 한다.

## 결과

허용된 일부 누락은 명시적인 `PARTIAL` 결과로 보존되지만 authority로 게시되지 않는다.
따라서 완전한 correction이 만들어질 때만 source authority가 바뀐다. 소비자는 명시적
PARTIAL 보정, 이전 publication 유지 또는 입력 부재 실패 중 source에 맞는 정책을
선택한다. API 장애 시 실행 시간이 늘 수 있으므로 fetch budget과 Airflow execution
timeout을 함께 관리해야 한다.

Snapshot source처럼 과거 시각을 재조회할 수 없는 데이터는 불완전 상태가 최종값으로 남을 수 있다. 이는 다른 시점의 데이터를 섞는 것보다 안전한 선택이다.

## 관련 자료

- `collector/adapters/base.py`
- `collector/config/schema.py`
- `collector/pipeline.py`
- `collector/manifest.py`
- `collector/storage.py`
- `collector/tests/test_adapters_base.py`
- `collector/tests/test_pipeline.py`
- `collector/tests/test_manifest.py`
- [ADR-0003](0003-bronze-streaming-and-scaling-boundaries.md)
