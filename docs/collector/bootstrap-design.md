# collector 초기 로드(bootstrap) 설계

작성 2026-08-18. 대상 독자는 이 기능을 구현하거나 실행할 사람이다.

## 왜 필요한가

collector는 배포 시점부터 수집을 시작한다. 그 이전 기간은 archive에 아무것도 없다.
분석과 학습은 과거 데이터를 필요로 하므로, 외부 원본(과거 CSV·과거 조회 API)에서
한 번 채워 넣어야 한다.

이 문서는 그 **1회성 부트스트랩**을 다룬다. 매일 도는 compaction(`silver → archive`)과는
출력 경로만 공유하고 입력·검증·실행 주기가 모두 다르다.

두 경로의 소유 구역은 시간으로 갈린다 — bootstrap은 수집 개시 이전, compaction은 최근
며칠이다. 겹치는 날짜가 생기면 경고를 남긴다(아래 "재개와 충돌" 참고).

## 범위

| 소스 | 입력 | 이번 범위 |
|---|---|---|
| `bike_rental_history` | 과거 CSV (`서울특별시 공공자전거 대여이력 정보_YYMM.csv`) | **포함** |
| `bike_station_realtime` | `bikeListHist` 과거 조회 API | **포함** |
| `weather_ultra_short_live` | 기상청 격자 텍스트 | **제외** — 입력 방식 플러그 지점만 열어둔다 |
| `living_population_grid` | 과거 CSV | **제외** — nowcaster가 이미 수행 중 |

### 실황을 미루는 이유

샘플 파일(`청운효자동_강수형태_202607_202607.csv`)이 **1변수 × 1격자 × 1개월** 구조다.
collector는 격자 25개 × 변수 5개(`T1H`·`REH`·`WSD`·`RN1`·`PTY`)를 쓰므로 한 달치를
재구성하려면 **파일 125개**를 격자·시각으로 조인해야 한다. 해상도도 1시간이라 10분
수집분과 밀도가 6배 다르다.

게다가 형식이 헤더 있는 CSV가 아니다. 첫 줄이 메타데이터다.

```
 format: day,hour,value location:60_127 Start : 20260701
 1, 0000, 0.000000
```

여기서 격자(`60_127`)와 시작일(`20260701`)을 파싱해야 하고, `day`는 월중 일자,
`hour`는 `HHMM` 문자열이다. 일반 CSV 리더로는 못 읽으므로 전용 파서가 필요하다.
실제 확보 가능한 파일 형태를 보고 만드는 게 맞다. 다만 `location:60_127`이
`weather_ultra_short_live.yaml`의 격자 목록에 실제로 있어(`- [60, 127]`) 축은 맞는다.

### 인구를 제외하는 이유 — 스키마 소유권 문제

`nowcaster/backfill.py:68-73`이 archive 행에 두 컬럼을 붙인다.

```python
is_estimated = False
estimation_method = "actual"
```

nowcaster가 실측과 추정을 구분하는 수단이다(추정에는 `is_estimated=True`,
`estimate_day.py:118-119`).

그런데 `collector/sources/living_population_grid.yaml`에는 이 두 컬럼이 없다. collector의
`archive_schema()`는 yaml `columns`에서 스키마를 만들므로, bootstrap이 이 소스를 처리하면
`conform()`이 두 컬럼을 떨어뜨린다. 같은 `archive/living_population_grid/` 안에서 날짜마다
스키마가 달라진다 — compaction 설계에서 공들여 없앤 문제가 되살아난다.

yaml에 두 컬럼을 추가하는 것은 답이 아니다. `columns`는 **silver 스키마**를 정의하므로,
collector가 API 응답에 없는 컬럼을 기대하게 된다.

즉 인구 이관은 파일을 옮기는 일이 아니라 **archive 스키마의 소유권을 정리하는** 별도
작업이다. nowcaster의 구현이 동작 중이므로 그대로 둔다.

## 확정된 설계 결정

| # | 항목 | 결정 |
|---|---|---|
| 1 | 대여이력 입력 | CSV |
| 2 | 적재 계층 | `archive/`에 직접 |
| 3 | 재고 입력 | `bikeListHist` API |
| 4 | 출처 구분 | `_source_kind` 메타 컬럼 추가 |
| 5 | 검증 | collector `validate_batch()` 재사용, quarantine 없이 집계만 |
| 6 | 값 체계 | CSV 값을 API 코드 체계로 변환 |
| 7 | 재개 | archive 존재 여부로 판단, `--force`로 무시 |
| 8 | silver 겹침 | 경고 + 계속 (건수 집계) |
| 9 | 설정 위치 | bootstrap 전용 파일 (운영 yaml과 분리) |
| 10 | API 병렬도 | `--concurrency`, 기본 4. 실패 시 그 날짜 건너뛰기 |

### 결정 2 — archive에 직접 쓰는 이유

CSV는 collector 수집 파이프라인을 태울 수 없다(어댑터는 네트워크에서 받는 계약이라
로컬 파일을 모른다). 따라서 출력 위치를 직접 정해야 한다.

archive는 "하루치를 통째로 읽는" 계층이고 초기 로드가 채우려는 것이 정확히 그 용도의
과거 데이터다. silver는 "수집 윈도우 단위"라는 의미가 있는데, CSV에서 만들어낸 가짜
윈도우를 끼워 넣으면 그 의미가 흐려진다 — `_window_start`에 무엇을 넣을지부터 애매하다.

silver에 쓰면 기존 소비자가 과거를 읽을 수 있다는 이점이 있으나 실효가 제한적이다.
ml은 lookback이 168시간이라 과거 3년을 silver로 읽을 일이 없고, loader에는 대여이력
테이블이 아예 없다(`loader/config.py`의 `TABLE_SPECS`).

### 결정 3 — 재고를 API로 하는 이유

재고 CSV(`data_2512.csv`)는 컬럼이 `일시·대여소번호·대여소명·시간대·거치대수량`
5개뿐이라 collector의 7개 컬럼을 못 채운다.

| collector 컬럼 | CSV | 메울 방법 |
|---|---|---|
| `stationId` (**required**) | `00102` 형식 | 마스터 API 조인 필수 |
| `stationName` (required) | 있음 | — |
| `parkingBikeTotCnt` | `거치대수량` | — |
| `rackTotCnt` | 없음 | 마스터 (현재 시점 값) |
| `stationLatitude/Longitude` | 없음 | 마스터 (현재 시점 값) |
| `shared` | 없음 | **못 채움** |

CSV를 쓰면서도 마스터 API를 불러야 하고, 그렇게 채운 `rackTotCnt`·좌표는 과거 시점
값이 아니라 현재 값이라 오히려 틀린 정보가 된다.

`bikeListHist`는 collector 컬럼 7개와 1:1로 일치하고 `stationId`도 `ST-` 형식 그대로다.
각 시각의 실제 값을 준다.

### 결정 4 — `_source_kind`가 필요한 이유

`_window_start`의 의미가 출처마다 다르다.

| 출처 | 뜻 | 해상도 |
|---|---|---|
| compaction (수집분) | **언제 수집했는지** | 5분 / 10분 |
| bootstrap (대여이력 CSV) | **언제 일어났는지** (`RENT_DT` 내림) | 시간 |
| bootstrap (재고 API) | **언제 관측했는지** (`stationDt`) | 시간 |

특히 재고는 행에 다른 시각 컬럼이 없어 `_window_start`가 유일한 시각인데, 과거는 시간
단위·현재는 5분 단위가 된다. 구분 수단이 없으면 archive를 읽는 사람이 밀도 차이의
이유를 알 수 없다.

나중에 붙이려면 이미 쌓인 파일을 전부 다시 써야 하므로 처음부터 넣는다.
`nowcaster`도 같은 발상으로 `is_estimated`를 쓴다.

### 결정 6 — 값 매핑표 (실측 확정)

같은 날 API 1,000건과 CSV를 `(자전거번호, 대여일시)`로 조인해 1,000/1,000 일치를
확인하고 뽑은 대응이다.

| API `USR_CLS_CD` | CSV `이용자종류` |
|---|---|
| `USR_001` | 내국인 |
| `USR_002` | **외국인** |
| `USR_003` | **비회원** |

빈도만 보고 추정하면 `USR_002`/`USR_003`이 뒤집힌다 — CSV에서는 비회원이 외국인보다
흔한데 코드 순서는 반대다. 반드시 이 표를 쓴다.

`SEX_CD`는 CSV에 소문자 `m`/`f`가 섞여 있다(50만 건 중 68건). `M`/`F`로 정규화한다.

`BIKE_SE_CD`는 양쪽 다 한글(`일반자전거`/`새싹자전거`)이라 변환하지 않는다. 다만 2601
이후 CSV에는 컬럼 자체가 없어 null이 된다.

세 컬럼 모두 collector yaml에 `enum` 제약이 없어 변환하지 않아도 검증은 통과한다. 즉
순전히 archive를 어떤 모습으로 둘 것인가의 문제다.

## 구조

```
collector/bootstrap/
  __main__.py      # CLI 진입점
  config.py        # bootstrap 매핑 설정 스키마 + 로더
  csv_source.py    # kind: csv        — 파일 → 행
  api_source.py    # kind: history_api — 과거 조회 API → 행
  runner.py        # 공통: 검증 → 날짜별 archive 적재
  mappings/
    bike_rental_history.yaml
    bike_station_realtime.yaml
```

`main.py`(수집)·`compact.py`(압축)가 단일 모듈인 것과 달리 패키지로 둔다. 관심사가
넷이고, 실황을 붙일 때 `kma_grid_source.py`를 추가하고 `kind`를 하나 늘리면 되는 구조가
필요하다.

실행은 `uv run --frozen python -m bootstrap ...`이다. Airflow가 부르지 않는 수동
작업이므로 태스크 빌더를 만들지 않는다.

재사용하는 것:

- `collector/config/loader.py` — 소스의 `columns`·`policies`를 얻어 검증에 넘긴다
- `collector/validation/engine.py:validate_batch` — 타입 캐스팅과 정책 적용
- `collector/compaction.py:archive_schema` / `conform` — archive 스키마 강제
- `collector/storage.py:write_archive` / `list_silver_objects` / `read_archive_manifest`
- `libs/core/src/core/layout.py:archive_key`

## 처리 흐름

```
python -m bootstrap --source X --from A --to B [--csv-dir D] [--concurrency 4] [--force]

  bootstrap 매핑 설정 로드   (collector/bootstrap/mappings/{source}.yaml)
  collector 소스 설정 로드   (config.loader.load)

  날짜별:
    archive 이미 있으면          → skip                (--force면 진행)
    silver 있으면                → WARNING + 계속       (건수 집계)
    kind에 따라 원시 행 생성
    행을 "그 기록이 속한 시간대"로 그룹핑
    그룹마다: validate_batch(rows, config, ctx)
              _window_start(그룹의 시각) + _source_kind="bootstrap" 주입
    날짜치 concat → conform(archive_schema) → write_archive
    _archive_manifest 기록
```

### 왜 시간대로 먼저 그룹핑하는가

`_window_start`의 원천이 `RENT_DT`(대여이력)와 `stationDt`(재고)인데, `stationDt`는
`bike_station_realtime.yaml`의 컬럼이 아니다. `_process_columns`가 `config.columns`만
순회하므로(`validation/engine.py:170`) `validate_batch`가 이 값을 떨어뜨린다.

검증 전에 시간대로 그룹을 나눠두면 그룹마다 시각이 상수가 되어, 검증이 행을 폐기해도
정렬이 깨지지 않는다. 그룹 처리 후 상수를 컬럼으로 붙이면 된다.

`RunContext`는 그룹의 시각으로 채운다(`window_start`=그룹 시각, `window_end`=+1시간).
정책 함수가 `ctx`를 참조할 수 있어 비울 수 없다.

## archive 스키마 변경

이미 구현된 compaction 코드를 함께 수정한다.

```python
# collector/compaction.py
_META_FIELDS = [
    ("_row_status", pa.string()),
    ("_window_start", pa.string()),
    ("_source_kind", pa.string()),   # 신규
]
```

compaction은 `"collector"`, bootstrap은 `"bootstrap"`을 채운다. 컬럼 목록을 검사하는
기존 테스트가 함께 바뀐다.

## 설정 형식

```yaml
# mappings/bike_rental_history.yaml
kind: csv
encoding: cp949
na_values: []          # 이 vintage는 결측을 빈 문자열로 표기한다 (아래 참고)
column_map:
  자전거번호: BIKE_ID
  대여일시: RENT_DT
  대여 대여소번호: RENT_ID
  대여 대여소명: RENT_NM
  대여거치대: RENT_HOLD
  반납일시: RTN_DT
  반납대여소번호: RTN_ID
  반납대여소명: RTN_NM
  반납거치대: RTN_HOLD
  이용시간(분): USE_MIN
  이용거리(M): USE_DST
  생년: BIRTH_YEAR
  성별: SEX_CD
  이용자종류: USR_CLS_CD
  대여대여소ID: RENT_STATION_ID
  반납대여소ID: RETURN_STATION_ID
value_map:
  USR_CLS_CD: {내국인: USR_001, 외국인: USR_002, 비회원: USR_003}
  SEX_CD: {m: M, f: F}
window: {from_column: RENT_DT, format: "%Y-%m-%d %H:%M:%S"}
```

```yaml
# mappings/bike_station_realtime.yaml
kind: history_api
service: bikeListHist
time_format: "%Y%m%d%H"
page_size: 1000
window: {from_column: stationDt, format: "%Y%m%d%H"}
```

재고는 응답 필드명이 collector 컬럼과 같아 `column_map`이 없다.

`time_format`이 10자리인 것이 중요하다. 8자리(`20260817`)를 주면 API가 **에러 없이
무시하고 최신 스냅샷을 반환한다**. 조용히 틀린 데이터가 들어오므로 형식을 설정으로
고정하고 테스트로 못 박는다.

채우지 못하는 collector 컬럼은 검증 정책에 따라 null이 된다 — 대여이력의
`START_INDEX`·`END_INDEX`·`RNUM`은 API 페이지네이션 메타라 CSV에 없고 archive에도
의미가 없다.

### 결측 표기와 예상 폐기량

이 vintage(2606)에는 `\N` 표기가 없다. 결측이 전부 **빈 문자열**이고, collector의
`_judge_column`이 `raw_value is None or raw_value == ""`를 결측으로 판정하므로
(`validation/engine.py`) `na_values` 없이도 그대로 처리된다. 다른 vintage가 `\N`을 쓸
가능성이 있어 설정 항목은 남기되 기본은 비운다.

앞 20만 행 표본의 빈 값 분포다.

| 컬럼 | 빈 값 | 처리 |
|---|---|---|
| `성별` (`SEX_CD`) | 48,883 | optional → `keep_null` |
| `생년` (`BIRTH_YEAR`) | 2,851 | optional → `keep_null` |
| `반납대여소번호`·`반납대여소명`·`반납대여소ID` | 각 928 | optional → `keep_null` |
| `반납거치대` (`RTN_HOLD`) | 1,031 | optional → `keep_null` |
| **`자전거번호` (`BIKE_ID`)** | **28** | **required → `drop_row`** |

`BIKE_ID`만 `required: true`이고 정책이 `required_missing: drop_row`라 그 행은 폐기된다.
20만 행에 28건이므로 약 0.014%다. quarantine을 쓰지 않으므로 이 수치는
`_archive_manifest`의 `dropped`와 `column_issues`로만 남는다 — 로드 후 이 값이 표본
비율과 크게 다르면 입력을 의심해야 한다.

## 대용량 CSV 처리

대여이력 월 파일이 733MB / 418만 행이다.

날짜마다 파일을 다시 읽으면 31번 훑게 되어 월당 15~30분이 걸린다. **한 번만 읽고
날짜별로 버킷팅**한다.

행 순서가 완전히 정렬돼 있지 않다 — `00:18:46` 행이 `00:30` 이후에 나오는 것을
실측으로 확인했다. 따라서 파일 끝까지 읽어야 한 날짜가 끝났다고 확정할 수 있고,
"날짜가 바뀌면 flush"는 쓸 수 없다.

dict 리스트로 들면 4GB를 넘긴다. 청크 단위로 읽어 Arrow 배치로 쌓아 월 파일 하나 수준의
메모리로 억제한다. 그래도 부족하면 날짜별 임시 parquet으로 흘린다.

## 재개와 충돌

**재개** — 이미 archive가 있는 날짜는 건너뛴다. `--force`로 무시한다. 상태 파일을 두지
않는다. 날짜 단위로 원자적이라(한 날짜를 다 만든 뒤 쓴다) 중단 시 그 날짜는 아예 안
써지고 다음 실행이 다시 만든다.

**silver 겹침** — 그 날짜에 silver가 있으면 compaction의 구역이다. bootstrap이 쓰면 다음
04:30 배치가 silver 기준으로 덮어써서 결과가 조용히 사라진다. 반대 순서면 CSV로 채운 더
온전한 데이터가 sparse한 silver로 대체된다.

거부하지 않고 **경고 후 계속**한다. 다만 로그 한 줄은 대량 적재에서 묻히므로 실행 결과
요약에 건수를 남긴다. 판정은 `storage.list_silver_objects()`로 LIST 한 번이다.

**실패** — API 호출이 재시도 후에도 안 되면 그 날짜를 건너뛰고 계속한다. 재개가 archive
존재 기반이라 다음 실행이 자동으로 다시 시도한다. 6시간짜리 작업이 네트워크 오류 한
번으로 날아가지 않는다. 실패한 날짜가 있으면 종료 코드는 non-zero다.

## 결과 기록

quarantine을 쓰지 않으므로 폐기 행 원본은 남지 않는다. 대신 `validate_batch`가 반환하는
`column_issues`·`policy_actions`를 살려 `_archive_manifest`에 남긴다. 어느 컬럼에서 몇
건이 왜 빠졌는지는 알 수 있다.

```json
{
  "source_id": "bike_rental_history",
  "date": "2025-03-14",
  "archive_key": "archive/bike_rental_history/dt=2025-03-14.parquet",
  "source_kind": "bootstrap",
  "rows": 148203,
  "dropped": 12,
  "column_issues": {"SEX_CD": {"missing": 37201, "outlier": 0, "type_error": 0}},
  "silver_present": false,
  "loaded_at": "2026-08-19T02:11:03+09:00"
}
```

`silver_signature`는 넣지 않는다. compaction이 나중에 그 날짜에서 silver를 발견하면
서명 불일치로 판단해 정상적으로 넘겨받는다(`compaction.compact_date`의 skip 조건이
`previous.get("silver_signature") == signature`이므로 None이면 진행한다).

## 비용

**재고 API** — 시간당 6,400행 / 7페이지, 호출당 약 0.9초.

| 범위 | 호출 수 | 순차 | 4병렬 | 8병렬 |
|---|---|---|---|---|
| 1일 | 168 | 2.5분 | 40초 | 20초 |
| 1년 | 61,320 | 15시간 | 4시간 | 2시간 |
| 3년 (2023-08~) | 184,000 | 46시간 | 12시간 | 6시간 |

기본 병렬도를 4로 둔다. 서울 열린데이터광장은 공공 서비스이므로 기본을 보수적으로 잡고
필요할 때 명시적으로 올린다.

**대여이력 CSV** — 월 파일 하나에 733MB. 읽기는 단일 패스라 월당 수 분이다. 42개월치를
받으면 약 25GB를 수동으로 내려받아야 한다.

## 테스트

**csv_source** — 인코딩(cp949), 컬럼 매핑, 값 매핑, `na_values`, 날짜 파싱, 청크 경계,
순서가 흐트러진 입력에서 날짜 버킷이 온전한지

**api_source** — `YYYYMMDDHH` 10자리 형식(8자리를 주면 다른 데이터가 온다는 것을 못
박는다), 페이지네이션, 실패 재시도 후 건너뛰기, 병렬도 반영

**runner** — archive 존재 시 skip, `--force`가 무시, silver 겹침 경고와 집계,
`validate_batch` 재사용, `_window_start`/`_source_kind` 주입, **compaction 산출물과 스키마가
동일**한지

**CLI** — 인자 파싱, `--from`이 `--to`보다 뒤일 때, 종료 코드

**통합** (moto) — 실제 대여이력 CSV 일부로 원본 건수와 archive 건수가 일치하는지,
compaction이 만든 archive와 bootstrap이 만든 archive를 이어 읽을 수 있는지

## 구현 중 확정된 사항

구현과 리뷰를 거치며 이 문서 작성 시점에 없던 결정이 셋 추가됐다.

### CSV는 파일명으로 범위를 걸러 필요한 파일만 연다

「대용량 CSV 처리」절이 청크 읽기와 Arrow 누적만 정했는데, 그것만으로는 부족했다.
`read_by_date`가 디렉터리의 CSV를 **전부** 열기 때문에, 하루치를 넣으려 해도 42개월
25GB를 다 훑는다. 범위를 나눠 여러 번 돌리는 회피책이 오히려 그 비용을 배로 만든다.

파일명에서 `_(\d{4})` 패턴으로 YYMM을 뽑아, 요청 범위와 겹치지 않는 달은 열지 않는다.
YYMM을 못 뽑는 파일명은 **건너뛰지 않고 읽는다** — 파일명 규칙을 모른다고 데이터를
조용히 빠뜨리는 쪽이 훨씬 나쁘다. 그 경우 로그를 남긴다.

Arrow 누적과 합쳐 실측한 결과, 한 달 4,182,797행이 **1.11GB**(이전 방식 추정 5.5GB),
48.9초다.

### 재고는 같은 시각 안의 동일 행만 합친다

「참고」절이 관측만 하고 결정하지 않았던 항목이다. `bikeListHist`가 시간당 스테이션별
2행을 주고 그중 2,508쌍은 값이 완전히 같다. 그대로 두면 bootstrap 구간에서만 행이 두
배가 되어, `_source_kind`를 보지 않는 집계가 과거를 두 배로 센다.

`BootstrapConfig.dedup`(기본 false)을 두고 `bike_station_realtime`에만 켠다.

**`compaction.dedup()`을 재사용하지 않는다.** 그 함수는 `_window_start`를 **제외한**
전체 컬럼으로 묶는데, compaction에서는 옳지만(윈도우 중복은 같은 기록의 반복 수집)
bootstrap 재고에 쓰면 09시와 10시 재고가 우연히 같을 때 두 시각이 한 행으로 합쳐져
시계열이 파괴된다. bootstrap은 `_window_start`를 **포함한** 중복 제거를 쓴다 — 같은
시각의 동일 행만 합쳐지고, 시각이 다르면 값이 같아도 각각 남는다.

### 실행 안전장치 둘

- **입력을 못 읽었는데 성공으로 끝나는 것을 막는다.** `--csv-dir`가 없는 경로면 즉시
  실패하고, CSV를 하나도 못 찾으면 경고한다. `DateResult.status`를 `skipped`(이미
  적재됨)와 `empty`(행 없음·전량 폐기)로 갈라, 요약만 보고 "정상 재개"와 "입력을 하나도
  못 읽음"을 구별할 수 있게 한다.
- **지속 실패 시 중단한다.** 인증키 오류나 쿼터 소진이면 날짜마다 실패하면서 계속
  진행해 3년 범위에서 26,000회를 헛돈다. 연속 N일 실패하면 남은 날짜를 처리하지 않는다.
  재시도에 지수 백오프를 넣는다.

## 미결정

**초기 로드 기간** — 재고는 2023-08까지 가능하나 3년치가 4병렬로 12시간이다. 대여이력은
확보하는 CSV 파일 수가 결정한다. 실행 시점에 `--from/--to`로 정한다.

**실황 입력 형식** — 실제 확보 가능한 파일을 보고 정한다. `kind`를 하나 늘리는 자리는
열어둔다.

## 적재 전에 남은 것

구현은 끝났으나 실제 적재를 걸기 전에 아래가 필요하다.

**`BIKE_SE_CD` 매핑** — 현재 `column_map`에 없다. 2606 vintage는 헤더가 16개라 컬럼
자체가 없어 무해하지만, 초기 로드가 겨냥하는 **2601 이전은 17개**라 값이 통째로 버려진다.
그 시기 파일 하나로 헤더명을 확인해 한 줄 추가해야 한다. 모르고 적재하면 전량 재적재다.

**실데이터 검증** — 테스트는 전부 합성 데이터와 mock이다. 「테스트」절이 요구한 통합
검증 두 가지(실 CSV 건수 대조, compaction↔bootstrap 이어 읽기)가 아직 없다. MinIO를
띄워 하루치 시험 적재로 원본 건수·`dropped` 비율을 확인한 뒤 대량 적재를 건다.

**API 경로의 날짜 필터** — CSV 경로는 `_row_date`로 대상 날짜 밖 행을 거르는데 API
경로에는 대응물이 없다. `bikeListHist`가 경계에서 직전 시간대를 섞어 주면 `dt=` 파티션
안에 다른 날짜의 `_window_start`가 들어간다.

## 참고 — 실측으로 확인한 사실

- `tbCycleRentData`는 `/{날짜}/{시(HH)}`를 받아 그 한 시간치를 준다. 2016년까지 조회된다.
  2026-06-01 08시 13,282건이 CSV와 정확히 일치한다
- `bikeListHist`는 `/{YYYYMMDDHH}`를 받는다. 2023-08부터 조회된다. 8자리를 주면 무시되고
  최신 스냅샷이 온다. 시간당 6,400행 / 3,198개소로, **스테이션당 2행**이다(2,508쌍은 동일,
  690쌍은 `parkingBikeTotCnt`·`shared`만 다름 — 시간 내 두 관측으로 보인다).
  `shared`는 독립 값이 아니라 거치율 = `round(parking/rack*100)`이다
- 대여소 마스터가 필요해지면 `tbCycleStationInfo`가 있다(3,241개소, `RENT_ID`/`RENT_NO`/
  `HOLD_NUM`/좌표)
- 대여이력 CSV는 cp949. `(자전거번호, 대여일시)`가 유일하지 않다 — 15만 건에서 17건이
  중복이고, 같은 대여인데 `이용시간`·`이용거리`만 미세하게 다른 원본 자체의 기록이다.
  따라서 이 조합을 유일키로 쓰면 안 된다
- 행 순서가 `대여일시` 기준으로 완전히 정렬돼 있지 않다. `00:18:46` 행이 `00:30` 이후에
  나오는 것을 확인했다
- API 수집 시점 값과 월말 확정 CSV를 1,000건 전 필드 비교했을 때 불일치는 대여소 개명
  7건뿐이다. 발행 후 값이 바뀌지 않는다
