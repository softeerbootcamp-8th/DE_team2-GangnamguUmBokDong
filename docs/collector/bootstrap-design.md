# Collector 과거 데이터 초기 적재

> 상태: 구현됨<br>
> 코드 확인일: 2026-08-24

Bootstrap은 운영 Collector가 수집을 시작하기 전의 과거 데이터를 날짜별 Archive로 적재하는 수동 작업이다. Airflow DAG가 아니며 `python -m bootstrap` CLI로 필요한 기간에 한 번 실행한다.

## 왜 별도 경로인가

운영 수집과 초기 적재는 입력 형태와 수명이 다르다.

| 구분 | 운영 Collector | Bootstrap |
| --- | --- | --- |
| 입력 | 서울시·기상청 API | 월별 CSV 또는 과거 조회 API |
| 실행 | Airflow schedule | 운영자가 기간을 지정해 수동 실행 |
| 중간 계층 | Bronze → Silver → Archive compaction | 검증 후 Archive 직접 기록 |
| 시간 해상도 | source schedule의 5분·10분 등 | 과거 원본이 제공하는 주로 1시간 |
| 설정 | `collector/sources/*.yaml` | 운영 설정 + `bootstrap/mappings/*.yaml` |

Bootstrap도 운영 source YAML의 column, type, range와 policy를 그대로 사용한다. 별도 mapping은 CSV header 변환, 값 매핑, 시각 조합, 파생값과 join처럼 과거 입력을 운영 물리 schema에 맞추는 규칙만 가진다.

## 현재 지원 source

mapping 파일이 존재하는 다음 3개 source만 실행할 수 있다.

| Source | 입력 | 핵심 변환 | 알려진 한계 |
| --- | --- | --- | --- |
| `bike_rental_history` | cp949 월별 대여이력 CSV | 한글 header 변환, 사용자·성별 코드 통일 | CSV vintage에 따라 `자전거구분`이 없으면 null |
| `bike_station_realtime` | cp949 시간대별 재고 CSV | 날짜+시간 조합, 대여소 번호를 station ID·좌표 등에 join | `rackTotCnt`, `shared`는 매핑 생성 시점 snapshot 값 |
| `weather_ultra_short_live` | ASOS 서울 108번 시간자료 CSV | 시각 분해, UUU·VVV 계산, 무강수 RN1=0 | 서울 1개 지점·1시간 자료이며 PTY는 null |

현재 mapping에는 `history_api` source가 없다. 코드에는 과거 조회 API 입력기가 구현되어 있지만, 실제 활성 mapping 세 개는 모두 `kind: csv`다.

## 처리 흐름

```text
source YAML + bootstrap mapping
                 ↓
CSV 선택·chunk read 또는 과거 API 조회
                 ↓
header/value/time 변환 + 필요한 join
                 ↓
요청 날짜 밖 행 제거 → 시간 window별 그룹
                 ↓
운영 validation engine으로 검증
                 ↓
고정 Archive schema로 conform + 선택적 dedup
                 ↓
archive/<source_id>/dt=YYYY-MM-DD.parquet
_archive_manifest/<source_id>/dt=YYYY-MM-DD.json
```

시간 window 그룹은 검증 전에 만든다. 검증 엔진은 source YAML에 선언되지 않은 임시 시각 컬럼을 제거하므로, 먼저 그룹을 만들지 않으면 재고 snapshot의 시각을 보존할 수 없다.

Archive에는 source column 외에 다음 메타 column을 붙인다.

| Column | 의미 |
| --- | --- |
| `_row_status` | validation 결과 |
| `_window_start` | 해당 과거 관측이 속한 KST 시간 window |
| `_source_kind` | Bootstrap 행은 `bootstrap`, 운영 compaction 행은 `collector` |

Bootstrap은 quarantine 객체를 만들지 않는다. 날짜별 manifest에 `dropped`, `out_of_range`, `column_issues`와 station join 통계를 남긴다.

## 실행 방법

저장소 루트에서 실행한다.

```bash
cd collector

uv run --frozen python -m bootstrap \
  --source bike_rental_history \
  --from 2025-01-01 \
  --to 2025-12-31 \
  --csv-dir ../data \
  --csv-batch-by-month
```

재고와 날씨도 동일한 CLI를 사용한다.

```bash
uv run --frozen python -m bootstrap \
  --source bike_station_realtime \
  --from 2025-12-01 \
  --to 2025-12-31 \
  --csv-dir ../data

uv run --frozen python -m bootstrap \
  --source weather_ultra_short_live \
  --from 2025-01-01 \
  --to 2025-12-31 \
  --csv-dir ../data
```

`bike_station_realtime`은 대여소 매핑을 보강하기 위해 같은 `--csv-dir`의 대여이력 CSV도 사용한다. 두 종류의 파일을 함께 준비해야 매핑 coverage가 높아진다.

## 주요 옵션

| 옵션 | 동작 |
| --- | --- |
| `--from`, `--to` | 양 끝을 포함하는 처리 날짜 범위 |
| `--csv-dir` | CSV source의 입력 디렉터리. 없거나 디렉터리가 아니면 즉시 실패 |
| `--csv-batch-by-month` | 한 해의 날짜별 Arrow 결과를 동시에 보관하지 않고 월별로 해제 |
| `--materialize-empty-archive` | 확인된 0행 날짜도 schema가 있는 빈 Archive와 manifest로 기록 |
| `--force` | 기존 Archive가 있어도 다시 기록 |
| `--concurrency` | `history_api` 시간대 병렬 조회 수. 기본 4 |

대형 ZIP에서 필요한 CSV만 준비할 때는 [대형 아카이브 ZIP 선택 준비](./archive-zip-staging.md)를 따른다.

## 재개와 실패 계약

- 날짜별 Archive가 이미 있으면 `skipped`로 처리한다.
- 한 날짜는 완성된 뒤 기록하므로 별도 checkpoint 파일을 두지 않는다.
- 실패한 날짜가 하나라도 있으면 CLI 종료 코드는 non-zero다.
- 재실행하면 성공한 날짜는 건너뛰고 미완료 날짜만 다시 시도한다.
- `--force`는 기존 결과를 의도적으로 교체할 때만 사용한다.
- 입력 행이 없거나 검증 결과가 전부 탈락하면 기본 상태는 `empty`다. 빈 Archive가 필요한 소비자가 있을 때만 `--materialize-empty-archive`를 사용한다.
- `history_api`는 날짜 하나의 24시간 중 하나라도 실패하면 부분 결과를 쓰지 않는다. 연속 5일 실패하면 남은 범위를 중단한다.

## Compaction과의 충돌

같은 날짜에 운영 Silver가 존재하면 Bootstrap은 경고와 `silver_present=true`를 남기지만 쓰기를 막지는 않는다. 이후 daily compaction이 실행되면 운영 source authority를 기준으로 Archive를 다시 만들 수 있다.

따라서 원칙적으로 Bootstrap 범위는 운영 수집 시작일 이전으로 제한한다. 운영 기간과 겹치는 날짜를 교체해야 한다면 어떤 결과가 최종 authority인지 확인한 뒤 실행한다.

## 데이터 해석 시 주의점

### 과거 재고

원본 CSV의 `거치대수량`은 거치대 용량이 아니라 대여 가능한 자전거 수다. 이를 `parkingBikeTotCnt`로 사용한다. CSV에 없는 station ID, 실제 거치대 수, 좌표와 `shared`는 실행 시점의 API·대여이력으로 만든 매핑에서 채운다.

따라서 `_source_kind=bootstrap`인 과거 재고의 `rackTotCnt`와 `shared`를 당시 실제 값으로 해석하면 안 된다. 출처 통계는 Archive manifest의 `station_map`에 기록된다.

### 과거 날씨

운영 초단기실황은 여러 격자의 10분 자료지만 Bootstrap ASOS는 서울 관측소 한 곳의 1시간 자료다. 임의로 34개 격자나 10분 간격으로 복제하지 않고 원본 해상도를 보존한다. 격자별 과거 관측이 필요한 소비자에게는 적합하지 않다.

### 중복 제거

Bootstrap의 dedup은 설정에서 활성화된 경우 `_window_start`까지 포함한 완전 동일 행만 합친다. 서로 다른 시각에 값이 같다는 이유로 시계열 행을 제거하지 않는다. 현재 활성 CSV mapping은 모두 `dedup: false`이거나 기본값 false다.

## 적재 전 확인

1. 입력 파일명이 mapping의 `file_pattern`과 일치하는지 확인한다.
2. ZIP을 사용할 경우 먼저 `zip_stage --dry-run`으로 선택 파일과 크기를 검토한다.
3. 짧은 날짜 범위로 실행하여 row·drop·out-of-range·station-map 통계를 확인한다.
4. 생성된 Archive schema와 `_source_kind`를 확인한다.
5. 운영 Silver와 날짜가 겹치지 않는지 확인한다.
6. 전체 범위는 대용량 CSV일 경우 `--csv-batch-by-month`로 실행한다.

## 코드와 테스트 근거

- CLI와 재개·종료 코드: `collector/bootstrap/__main__.py`
- mapping schema: `collector/bootstrap/config.py`
- CSV 처리: `collector/bootstrap/csv_source.py`
- 과거 API 처리: `collector/bootstrap/api_source.py`
- station join: `collector/bootstrap/station_join.py`
- 날짜별 검증·Archive 게시: `collector/bootstrap/runner.py`
- Archive schema와 source kind: `collector/compaction.py`
- source mapping: `collector/bootstrap/mappings/*.yaml`
- 계약 테스트: `collector/tests/test_bootstrap_*.py`
