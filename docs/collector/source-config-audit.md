# Collector 소스 설정 점검 (raw ↔ silver ↔ 비즈니스 로직)

collector의 10개 소스 YAML을 **실제 API 응답**과 대조해, 선언이 원본과 맞는지 · 각 컬럼이
어떻게 처리되는지 · 전용 파싱이나 값 매핑이 필요한지를 정리한다.

- 점검 일자: 2026-08-19 (KST) — 같은 날 **2차 재점검**을 수행해 결과를 반영했다.
  1차 지적 사항의 수정 반영 여부를 실데이터로 재확인하고, 컬럼 선언이 아니라
  **수집 범위(페이지네이션·순회 상한)** 를 새로 들여다봤다. 그 결과 1차에서 놓친
  🔴 2건(5-18 · 5-19)이 나왔다. 1차는 "raw 키 ↔ 선언 차집합"만 봤기 때문에,
  **차집합이 없어도 행 자체가 안 들어오는** 경우를 볼 수 없었다.
- 점검 대상: `collector/sources/*.yaml` 10개 전부
- 대조 방법: `SEOUL_OPENAPI_KEY` · `KMA_APIHUB_KEY`로 각 엔드포인트를 직접 호출해 raw 응답을
  받고, `yaml`의 `columns` 선언 집합과 raw 키 집합을 차집합으로 비교한 뒤, `validation/engine.py`의
  판정 규칙을 손으로 적용했다. 소스별로 1,000행(대여이력 검증 시 16,896행) 표본을 썼다.
- 관련 문서: `docs/collector/DataSchema.md`(프로젝트 전체 스키마 표준),
  `docs/loader/implementation-plan.md`(Silver→Gold 매핑), `docs/collector/ml-integration-requests.md`

---

## 0. 공통 처리 규칙 (이 문서를 읽는 데 필요한 전제)

raw 한 행이 silver 한 행이 되기까지 거치는 단계:

```
raw JSON → adapter.normalize()  → list[dict]  → validate_batch()  → silver parquet
             (구조만 변환)                        (컬럼별 판정·정책)
```

### 선언되지 않은 컬럼은 조용히 버려진다

`validation/engine.py:171`의 `_process_columns`는 `config.columns`만 순회한다. raw에 있어도
YAML에 선언하지 않은 키는 silver에 **아예 나타나지 않고, 경고도 로그도 남지 않는다.**
그래서 "선언 안 된 raw 컬럼" 목록이 이 점검의 핵심 산출물이다.

### 컬럼 판정은 null → type → range/enum 3단계

| 판정 | 조건 | 적용 정책(현재 전 소스 공통) |
| --- | --- | --- |
| `MISSING` | 값이 `None` 또는 `""` | required → `drop_row` / optional → `keep_null` |
| `TYPE_ERROR` | `types` 전부 캐스팅 실패 | required → `drop_row` / optional → **`set_null`** |
| `OUTLIER` | `range`/`enum` 위반 | required → `drop_row` / optional → `set_null` |

`TYPE_ERROR`가 `optional_outlier`(=`set_null`) 경로를 타는 것이 중요하다. **캐스팅에 실패한
값은 아무 경고 없이 null이 된다.**

### `types`는 선언 순서대로 첫 성공을 채택한다 — `str`을 먼저 쓰면 뒤는 죽은 코드

`_try_cast`(engine.py:43)는 `types` 순서대로 시도해 첫 성공을 쓴다. `str(x)`는 실패하지
않으므로 `types: [str, int]`는 `types: [str]`과 **완전히 동일**하다. 뒤에 붙은 `int`/`float`는
절대 실행되지 않는다.

### 완결도 게이트는 컬럼 null을 보지 않는다

`quality.max_missing_ratio`는 `pipeline.py:389`의 `_missing_ratio` — **조각(chunk)·행 수 기준
수집 완결도**다. `max_drop_ratio`는 폐기된 **행** 비율이다. 컬럼이 몇 개나 null로 바뀌었는지는
어느 게이트도 보지 않는다. manifest의 `column_issues`에 집계는 남지만 실패 조건이 아니다.

---

## 1. bike 계열

### 1-1. `bike_station_realtime` (`bikeList`, 5분)

raw 7개 컬럼 = 선언 7개 컬럼. **차집합 없음(완전 일치).**
응답 필드는 전부 문자열로 온다.

| raw 컬럼 | raw 실측값 | 선언 | 처리 결과 | 비고 |
| --- | --- | --- | --- | --- |
| `stationId` | `'ST-4'` | `str`, required | 그대로 | Gold `sta_id`의 PK |
| `stationName` | `'102. 망원역 1번출구 앞'` | `str`, required | 그대로 | **표본 1,000건 중 848건에만 `"번호. "` 접두가 있다** |
| `rackTotCnt` | `'15'` | `int`, 0~200 | `15` | 실측 최대 51 |
| `parkingBikeTotCnt` | `'9'` | `int`, 0~200, `clip_to_range` | `9` | 실측 최대 100 → 상한 200에 안 닿아 clip이 한 번도 발동하지 않는다 |
| `shared` | `'60'` | `int`, 0~1000 | `60` | 거치율(%). 실측 0~730 |
| `stationLatitude` | `'37.55564880'` | `float`, 37.4~37.7 | `37.5556488` | |
| `stationLongitude` | `'126.91062927'` | `float`, 126.7~127.2 | `126.91062927` | |

- **전용 파싱 필요**: `stationName`의 `"102. "` 접두 분리. 현재 loader는 `sta_nm`에도
  `sta_addr`에도 이 문자열을 **그대로 두 번** 넣는다(`loader/transform.py:48-51`).
- **교차 컬럼 불변식 미검증**: 표본 1,000건 중 363건이 `parkingBikeTotCnt > rackTotCnt`다.
  따릉이는 거치대 밖 주차가 가능해 정상 상황이지만, 현재 엔진에 교차 컬럼 규칙이 없어
  "10배 튀는 값"과 "정상 초과"를 구분할 방법이 없다.
- 🔴 **컬럼은 완전 일치하지만 행의 63.4%가 안 들어온다.** 이 소스는 1페이지(1,000행)에서
  멈추고, 실제 운영 대여소는 2,736곳이다. 원인은 `list_total_count`의 의미가 이 API만
  다르다는 것 — 자세한 것은 **5-18**을 보라. 표본을 "1,000건"으로 적은 위 서술은
  페이지 하나가 곧 전체였기 때문이 아니라, **수집이 거기서 끊겨서**였다.

### 1-2. `bike_station_master` (`bikeStationMaster`, 1일)

raw 5개 = 선언 5개. **차집합 없음.** 단, 이 소스만 `LAT`/`LOT`가 JSON **숫자**로 온다
(다른 서울 API는 전부 문자열).

| raw 컬럼 | raw 실측값 | 선언 | 처리 결과 | 비고 |
| --- | --- | --- | --- | --- |
| `RNTLS_ID` | `'ST-10'` | `str`, required | 그대로 | `bike_station_realtime.stationId`와 같은 체계 |
| `ADDR1` | `'서울특별시 마포구 양화로 93'` | `str` | 그대로 | 빈값 0건. **진짜 도로명주소** |
| `ADDR2` | `'427'` / `'더샵스타시티 C동 앞'` | `str` | 그대로 | 상세주소와 거치대번호가 섞여 있다 |
| `LAT` | `37.552746` (float) | `float`, **range 없음** | 그대로 | **전량 3,428건 중 77건(2.2%)이 `0.0`** (2차 재점검에서 전 페이지로 재측정) |
| `LOT` | `126.918617` (float) | `float`, **range 없음** | 그대로 | 같은 77건이 `0.0`. `RNTLS_ID` 중복은 0건 |

- **버그**: `LAT`/`LOT`에 `range`가 없어 `0.0`(기니만 해상)이 정상값으로 silver에 들어간다.
  `bike_station_realtime`은 같은 의미의 컬럼에 `range`가 선언돼 있다 — 선언 불일치.
  단, 2차 재점검에서 확인한 바로는 `normalizer/station_master.py:98`이 `_valid_wgs84`로
  좌표를 검사해 실패하면 실시간 좌표로 폴백하므로 **downstream 실피해는 없다**.
  다만 그 폴백 소스가 5-18 때문에 36.6%밖에 없어, 77건 중 상당수는 폴백도 못 받는다.
- **미활용**: 이 소스는 수집만 되고 Gold에 적재되지 않는다(`loader/tables.yaml`에 항목 없음).
  그런데 loader는 `sta_addr`에 넣을 실주소가 없어서 `stationName`을 복제하고 있다. 여기 있는
  `ADDR1`이 바로 그 값이다.

### 1-3. `bike_rental_history` (`tbCycleRentData`, 5분, hourly API)

raw 20개 = 선언 20개. **차집합 없음.** 단 raw는 행마다 키 개수가 다르다(표본 1,000행 기준
`SEX_CD` 772행 · `BIRTH_YEAR` 977행 · `RTN_ID`/`RTN_NM`/`RTN_HOLD`/`RETURN_STATION_ID` 999행만 존재).
없는 키는 `row.get()`이 `None`을 주고 optional이라 `keep_null` → 정상 처리된다.

| raw 컬럼 | raw 실측값 | 선언 | 처리 결과 | 비고 |
| --- | --- | --- | --- | --- |
| `BIKE_ID` | `'SPB-54742'` | `str`, required | 그대로 | |
| `RENT_DT` | `'2026-08-19 01:00:05'` | `str`, required | 그대로 | 파싱은 downstream 몫 |
| `RENT_ID` | `'01050'` | `str` | 그대로 | 대여소 **번호**(0 패딩) |
| `RENT_STATION_ID` | `'ST-1420'` | `str` | 그대로 | 대여소 **ID**. `RENT_ID`와 다른 체계 |
| `RENT_NM` / `RENT_HOLD` | `'둔촌역 3번 출입구'` / `'0'` | `str` | 그대로 | |
| `RTN_DT`/`RTN_ID`/`RTN_NM`/`RTN_HOLD`/`RETURN_STATION_ID` | 대여 쪽과 동일 형식 | `str` | 그대로 | 999/1,000행에 존재 |
| `USE_MIN` | `'0'`, `'1'` | `[str, int]` | **`'0'` (문자열)** | `int`는 죽은 선언 |
| `USE_DST` | `'0.00'`, `'461.60'` | `[str, float]` | **`'0.00'` (문자열)** | 단위는 m. `float`은 죽은 선언 |
| `USR_CLS_CD` | `'USR_001'`(994) `'USR_003'`(5) `'USR_002'`(1) | `str` | 그대로 | 내국인/비회원/외국인. **CSV bootstrap이 한글→코드 `value_map`으로 통일함** |
| `SEX_CD` | `'M'`(585) `'F'`(156) `''`(31) 키없음(228) | `str` | `''`·없음 → null | bootstrap이 `m`/`f` → `M`/`F` 정규화 |
| `BIRTH_YEAR` | `'2001'` | `[str, int]` | **문자열** | `int` 죽은 선언 |
| `BIKE_SE_CD` | `'일반자전거'`(963) `'새싹자전거'`(37) | `str` | 그대로 | 이름은 `CD`인데 **값은 한글 레이블**. bootstrap이 `value_map` 없이 그대로 매핑 |
| `START_INDEX` / `END_INDEX` | `0` / `0` (정수) | `int` | `0` | **페이지네이션 에코. 항상 (0,0). 전 행 무의미** |
| `RNUM` | `'1'`…`'1000'` | `[str, int]` | 문자열 | **응답 내 행번호. 데이터가 아니다** |

**API 의미 구조(중요)** — 실측으로 확인:

- 엔드포인트는 `/{날짜}/{시}`로 **대여 시각(`RENT_DT`) 기준** 한 시간치를 준다
  (`2026-08-18/18` → 16,896행 전부 `RENT_DT` 18시대).
- 그런데 정렬 순서는 **반납 시각(`RTN_DT`) 오름차순**이다(16,896행 중 역전 2건, 초 단위 동시 반납).
- 즉 **반납이 완료돼야 그 시각 순서로 목록 뒤에 붙는다.** `RNUM`은 전역 연속이고 뒤에만
  추가되므로, 같은 시간대를 반복 수집해도 기존 행의 `RNUM`은 흔들리지 않는다
  → `compaction.dedup`(전체 데이터 컬럼 group by)이 의도대로 작동한다.

**전용 파싱 / 매핑 필요**

- `RENT_ID`(번호)와 `RENT_STATION_ID`(ID) 두 체계가 병존한다. Gold의 `stations.sta_id`는
  `ST-*` 체계이므로 조인에는 `*_STATION_ID`를 써야 한다.
- `USE_DST` 단위는 m인데 컬럼명에 단위가 없다(bootstrap 매핑의 원본 한글은 `이용거리(M)`).

### 1-4. bike 계열 종합

- 세 소스 모두 raw ↔ 선언 차집합이 **없다**. 선언 누락 문제는 bike 계열에는 없다.
- 대신 `[str, int]` 같은 **무효 타입 선언 4건**, `bike_station_master`의 **좌표 range 누락**,
  `RNUM`/`START_INDEX`/`END_INDEX` **무의미 컬럼 3건 적재**가 남아 있다.

---

## 2. event 계열

### 2-1. `cultural_event` (`culturalEventInfo`, 1일)

raw **24개** 컬럼 중 **9개만 선언**. `list_total_count = 19,495`.

**선언되지 않은 raw 컬럼 15개**:
`DATE`, `ORG_NAME`, `USE_TRGT`, `USE_FEE`, `INQUIRY`, `PLAYER`, `PROGRAM`, `ETC_DESC`,
`ORG_LINK`, `MAIN_IMG`, `RGSTDATE`, `TICKET`, `THEMECODE`, `HMPG_ADDR`, `PRO_TIME`

| raw 컬럼 | raw 실측값 | 선언 | 처리 결과 |
| --- | --- | --- | --- |
| `TITLE` | `'2026 카즈미 타테이시 트리오 내한공연…'` | `str`, required | 그대로 |
| `CODENAME` | `'콘서트'` | `str` | 그대로 → Gold `category` |
| `GUNAME` | `'종로구'`(229) `'중구'`(117) … `''`(2) | `str` | 빈값 → null |
| `PLACE` | `'강동아트센터 대극장 한강'` | `str` | 그대로 |
| `STRTDATE` | `'2026-12-24 00:00:00.0'` | `str`, required | 그대로. loader `_parse_date`가 `%Y-%m-%d %H:%M:%S.%f`로 처리 |
| `END_DATE` | 같은 형식 | `str`, required | 그대로 |
| `IS_FREE` | `'무료'`(657) `'유료'`(343) | `str` | 그대로 |
| `LAT` | `'37.5512204558342'` | `float`, 37.4~37.7 | 캐스팅 |
| `LOT` | `'127.157342546961'` | `float`, 126.7~127.2 | 캐스팅 |

**실측으로 드러난 문제**

- `LAT`에 `'45.4215°N'` 같은 **도분 표기가 섞여 있다**(표본 1,000건 중 1건). `float` 캐스팅
  실패 → `TYPE_ERROR` → `set_null`. 값이 사라지고 경고도 없다. (범위 위반은 0건이었다.)
- `END_DATE` 최대값이 **`2626-08-08`** 이다(원본 오타). loader `_parse_date`가 정상 파싱해
  이 행사는 영구히 "진행 중"으로 남는다.
- 표본의 **68.7%가 이미 종료된 행사**다. 수집은 19,495행 전부 하고 loader가 걸러낸다.
  기능상 문제는 없지만 매일 20페이지를 받아 대부분 버린다.
- `PRO_TIME`(`'19:30'`)이 선언되지 않아 **행사 시각이 silver에 없다.** 날짜만으로는
  "지금 열려 있는 행사"를 판단할 수 없다.

**전용 파싱 필요**: `DATE`(`'2026-12-24~2026-12-24'` 표기 기간)는 `STRTDATE`/`END_DATE`와
중복이라 선언 불필요. `USE_FEE`는 자유 텍스트(`'VIP석 88,000원 / R석 77,000원…'`)라
정규화 없이는 쓸 수 없다.

### 2-2. `performance_event` (`ListPublicReservationSport`, 1일)

raw **24개** 중 **10개만 선언**. `list_total_count = 602`.

**선언되지 않은 raw 컬럼 14개**:
`GUBUN`, `MAXCLASSNM`, `SVCSTATNM`, `USETGTINFO`, `SVCURL`, `RCPTBGNDT`, `RCPTENDDT`,
`IMGURL`, `DTLCONT`, `TELNO`, `V_MIN`, `V_MAX`, `REVSTDDAYNM`, `REVSTDDAY`

| raw 컬럼 | raw 실측값 | 선언 | 처리 결과 |
| --- | --- | --- | --- |
| `SVCID` | `'S251121100349891778'` | `str`, required | 그대로 → Gold `event_id`(중복 0건) |
| `SVCNM` | `'테니스장1(평일)-2026년 응봉공원(대현산배수지)'` | `str`, required | 그대로 → `title` |
| `MINCLASSNM` | `'테니스장'`(285) `'풋살장'`(82) … | `str` | 그대로 → `category` |
| `AREANM` | `'성동구'` / **`'고양시'`, `'과천시'`** | `str` | 그대로 → `gu` |
| `PLACENM` | `'응봉공원'` | `str` | 그대로 |
| `SVCOPNBGNDT` / `SVCOPNENDDT` | `'2025-12-01 00:00:00.0'` / `'2026-12-31 00:00:00.0'` | `str`, required | 그대로 |
| `PAYATNM` | `'유료'`(506) `'무료'`(58) **`'유료(요금안내문의)'`(38)** | `str` | 그대로 |
| `X` (경도) | `'127.02182026085195'` | `float`, 126.7~127.2 | 캐스팅. **빈값 33건(5.5%)** |
| `Y` (위도) | `'37.5569473910838'` | `float`, 37.4~37.7 | 캐스팅 |

**실측으로 드러난 문제**

- **`SVCOPNBGNDT`/`SVCOPNENDDT`는 행사 일자가 아니라 "예약 서비스 개설 기간"이다.**
  기간 길이 중앙값 **244일**, 최대 **3,925일**. 상위 조합은 `2025-12-01~2026-12-31`(84건),
  `2026-01-01~2026-12-31`(33건). loader가 이걸 `cultural_events.start_date`/`end_date`로
  넣으므로, 테니스장 예약 서비스 하나가 1년 내내 열리는 "행사"로 표시된다.
  실제 개별 이용 일자는 raw에 없다(예약 API에서만 조회 가능).
- `AREANM`에 서울 자치구가 아닌 `'고양시'`, `'과천시'`가 섞인다. Gold `gu`로 그대로 들어간다.
- `PAYATNM`에 `'유료(요금안내문의)'` 제3값이 있다. loader는 `is_free = "무료" if v == "무료" else "유료"`로
  정규화하므로 결과적으로 `'유료'`가 되어 맞지만, **값이 없을 때도 `'유료'`가 된다**
  (`row.get("PAYATNM")` → `None` → `else` 분기).
- `X`/`Y` 빈값 33건은 `MISSING` → `keep_null` → 지도 표시 불가. 이건 정상 처리다.
- `V_MIN`/`V_MAX`(`'07:00'`/`'19:00'`, 이용 가능 시각)가 선언되지 않았다. `cultural_event`의
  `PRO_TIME`과 같은 성격의 누락이다.

### 2-3. event 계열 종합

두 소스가 같은 Gold 테이블(`cultural_events`)을 공유하는데, **날짜 컬럼의 의미가 서로 다르다.**
`cultural_event`는 실제 행사 기간, `performance_event`는 예약 서비스 개설 기간이다.
같은 테이블에 섞으면 "오늘의 행사" 질의가 왜곡된다.

---

## 3. population 계열

두 소스 모두 `loader/tables.yaml`에 항목이 없다 — **Gold 미적재이고 silver를 ml이 직접 읽는
설계**다(`docs/collector/ml-integration-requests.md` 참고). 아래 지적은 silver 품질에 관한 것이다.

### 3-1. `living_population_grid` (`Se250MSpopLocalResd`, 1일)

raw 33개 = 선언 33개. **차집합 없음.** `list_total_count = 253,699`
(≈ 서울 250m 격자 10,571개 × 24시간 = 하루치).

| raw 컬럼 | raw 실측값 | 선언 | 처리 결과 |
| --- | --- | --- | --- |
| `YMD` | `'20260814'` | `str`, required | 그대로 |
| `TT` | `'00'` | `str`, required | 그대로 (시각, 00~23) |
| `H_DNG_CD` | **`'11110515     '`** (13자, 뒤 공백 5칸) | `str`, required | **공백 포함 그대로** |
| `CELL_ID` | `'다사52255350'` | `str`, required | 그대로 |
| `SPOP` | `'5.32'` / **`'*'`** | `float`, 0~10,000,000 | `'*'`은 `TYPE_ERROR` → `set_null` |
| `M00`~`M70`, `F00`~`F70` (28개) | `'9.44'` / **`'*'`** | `float` | 같음 |

**실측으로 드러난 문제 (심각)**

- **마스킹 표기 `'*'`가 결측이 아니라 타입 오류로 판정된다.**
  표본 1,000행에서 나이·성별 28개 컬럼 28,000칸 중 **14,945칸(53.4%)이 `'*'`**,
  `SPOP` 자체도 **98행(9.8%)이 `'*'`**. 전부 `TYPE_ERROR` → `set_null`.
  null이 되는 것 자체는 `DataSchema.md`가 명시한 의도된 동작이지만, 판정 라벨이
  `missing`이 아니라 `type_error`라서 manifest 지표와 정책 손잡이가 어긋난다(→5-2).
- `H_DNG_CD` **뒤 공백 미제거**. 행정동 코드 조인 시 `'11110515'`와 매칭되지 않는다.
- **날짜 지연**: 2026-08-19에 호출했는데 `YMD`가 전부 `20260814`(5일 전)다. API에 날짜
  파라미터가 없어 "최근 공표분"을 그대로 받는다. 그런데 silver 파티션은 수집일(`dt=2026-08-19`)로
  잡히므로 **파티션 날짜와 데이터 날짜가 어긋난다**. (이미
  `docs/collector/ml-integration-requests.md` §8에 같은 지적이 있다.)
- `CELL_ID`(`'다사52255350'`)는 격자 좌표를 인코딩한 문자열이다. 대여소·기상 격자와 조인하려면
  **전용 디코더가 필요**하고, 현재 어디에도 없다.

### 3-2. `population_realtime` (`citydata_ppltn`, 5분, POI001~116 순회 — 실제 유효는 131)

raw **22개** 중 **7개만 선언**. 그리고 🔴 **순회 상한이 실제보다 15개 모자란다(5-19)**.

**선언되지 않은 raw 컬럼 15개**:
`AREA_CONGEST_MSG`, `PPLTN_RATE_0`~`PPLTN_RATE_70`(8개), `RESNT_PPLTN_RATE`,
`NON_RESNT_PPLTN_RATE`, `REPLACE_YN`, **`PPLTN_TIME`**, `FCST_YN`, `FCST_PPLTN`

| raw 컬럼 | raw 실측값 | 선언 | 처리 결과 |
| --- | --- | --- | --- |
| `AREA_NM` | `'강남 MICE 관광특구'` | `str`, required | 그대로 |
| `AREA_CD` | `'POI001'` | `str`, required | 그대로 |
| `AREA_CONGEST_LVL` | `'여유'` | `str`, enum 4값 | 그대로 |
| `AREA_PPLTN_MIN` / `MAX` | `'2500'` / `'3000'` | `int`, 0~500,000 | 캐스팅 |
| `MALE_PPLTN_RATE` / `FEMALE_PPLTN_RATE` | `'54.7'` / `'45.3'` | `float`, 0~100 | 캐스팅 |

**실측으로 드러난 문제**

- **`PPLTN_TIME`(`'2026-08-19 02:05'`, 관측 시각)이 선언되지 않았다.** 5분 주기 소스인데
  silver에 관측 시각이 없어 파티션(`dt`/`hh`)에만 의존해야 한다. `REPLACE_YN='N'`
  (대체값 여부)도 함께 빠져 있어, 실측값과 추정 대체값을 구분할 수 없다.
- **`FCST_PPLTN`은 12개짜리 중첩 배열**이다
  (`{FCST_TIME, FCST_CONGEST_LVL, FCST_PPLTN_MIN, FCST_PPLTN_MAX}` × 12 = 향후 12시간 예측).
  현재 엔진의 `types`(`str`/`int`/`float`/`bool`/`precip`)로는 표현할 수 없다.
  선언해도 처리되지 않으므로 별도 소스나 전용 flatten이 필요하다.
- `PPLTN_RATE_0`~`70`(연령대 비율 8개)과 `RESNT_PPLTN_RATE`(상주/비상주)가 선언되지 않아,
  성별 비율만 있고 연령 구성은 silver에 없다.
- `AREA_CD`는 POI 코드이고 **좌표가 응답에 없다.** 대여소나 자치구와 공간 조인할 수 없다.
  POI ↔ 좌표/자치구 매핑 테이블이 별도로 필요하다.
- 🔴 **어댑터의 `expected = 116`이 실제 유효 범위보다 작다.** 2차 재점검에서 POI117~POI131을
  하나씩 호출해 전부 정상 응답(200 + 행 1건)을 확인했다. POI132부터는 래퍼 없이
  `RESULT.CODE`만 온다. 상세는 **5-19**.

---

## 4. weather 계열

이 계열은 2026-08-19에 별도로 점검·수정을 완료했다. 격자 매칭 개선은
`docs/superpowers/specs/2026-08-19-weather-grid-matching-design.md`, 페이지네이션 수정은
커밋 `d8b3622`를 참고한다. 아래는 **현재 상태 재확인 결과**다.

세 소스 모두 `grids`가 34개로 동일하다(실제 대여소 2,746곳의 최근접 격자 집합).
raw는 long format(`category`/`fcstValue` 쌍)으로 오고, 어댑터의 `pivot`이 wide로 바꾼다.

### 4-1. `weather_ultra_short_live` (`getUltraSrtNcst`, 10분)

`totalCount = 8`. raw 카테고리 8개 = 선언 8개. **차집합 없음.**

| 카테고리 | raw 실측값 | 선언 | 비고 |
| --- | --- | --- | --- |
| `T1H` | `'25'` | `float`, -50~50 | 기온 ℃ |
| `REH` | `'81'` | `float`, 0~100 | 습도 % |
| `WSD` | `'0.7'` | `float`, 0~50 | 풍속 m/s |
| `RN1` | `'0'` | **`float`**, 0~500 | **실황은 숫자로 온다.** 예보 계열의 `RN1`(범주 문자열)과 이름만 같다 |
| `PTY` | `'0'` | `int`, enum `[0,1,2,3,5,6,7]` | 강수형태 |
| `UUU`/`VVV` | `'-0.5'`/`'-0.3'` | `float`, -50~50 | 동서·남북 바람성분 |
| `VEC` | `'56'` | `float`, 0~360 | 풍향 |

### 4-2. `weather_ultra_short_forecast` (`getUltraSrtFcst`, 30분)

`totalCount = 66` (11 카테고리 × 6 시각). raw 11개 = 선언 11개. **차집합 없음.**

`T1H`/`REH`/`WSD`/`SKY`/`PTY`/`LGT`/`POP`/`UUU`/`VVV`/`VEC`는 4-1과 같은 규칙.

| 카테고리 | raw 실측값 | 선언 | 비고 |
| --- | --- | --- | --- |
| `RN1` | **`'강수없음'`** | **`precip`** | 예보는 범주 문자열. `core.precip`이 mm 실수로 변환 |
| `SKY` | `'1'`, `'3'` | `int`, enum `[1,3,4]` | 하늘상태 |
| `POP` | `'0'`, `'20'` | `float`, 0~100 | 강수확률. **초단기예보에도 존재한다** |

### 4-3. `weather_short_term_forecast` (`getVilageFcst`, 3시간)

`totalCount = 944` (base 0200 기준). raw **14개** 카테고리 중 **12개 선언**.

**선언되지 않은 raw 카테고리 2개: `TMN`(일 최저기온), `TMX`(일 최고기온)**

| 카테고리 | raw 실측값 | 선언 | 비고 |
| --- | --- | --- | --- |
| `TMP` | `'25'` | `float`, -50~50 | 3시간 기온 |
| `TMN` | **`'24.0'`** | **없음** | 일 최저기온. 하루 1회(0600)만 온다 → 표본 78행 중 4행 |
| `TMX` | **`'32.0'`** | **없음** | 일 최고기온. 하루 1회(1500)만 온다 → 4행 |
| `PCP` | `'강수없음'` | `precip` | 3시간 강수량 범주 |
| `SNO` | `'적설없음'` | `precip` | **3시간 신적설. 단위가 cm다 — §5-3 참고** |
| `POP`/`REH`/`WSD`/`SKY`/`PTY`/`UUU`/`VVV`/`VEC`/`WAV` | | 선언됨 | `WAV`는 파고(m), 서울 내륙은 항상 `'0'` |

`TMN`/`TMX`는 특정 `fcstTime`에만 오므로 선언하면 대부분 행에서 null이 되지만,
`optional_missing: keep_null`이 그대로 처리한다. `pivot`의 group key는 `category`/`fcstValue`를
제외한 나머지 필드이므로, 해당 시각 행에 자연스럽게 합쳐진다.

---

## 5. 수정이 필요한 부분

대략 심각도 순. 각 항목의 근거는 위 본문에 있다.

- 5-2는 재조사 후 🔴에서 🟠로 내렸다(마스킹 → null은 의도된 동작이었다).
- **수정 완료: 5-1 · 5-2 · 5-3 · 5-10 · 5-15 · 5-16 · 5-17.** 나머지는 미착수다.
  번호는 상호 참조 때문에 그대로 뒀다.
- **2차 재점검(2026-08-19)에서 추가된 항목: 5-18 🔴 · 5-19 🔴 · 5-20 🟠.**
  이 셋은 컬럼 선언이 아니라 **수집 범위**의 문제라 1차의 차집합 비교로는 보이지 않았다.
  현재 최우선은 5-18이다 — 다른 모든 대여소 지표의 분모를 바꾸기 때문이다.

### 2차 재점검에서 확인한 수정 반영 상태

| 항목 | 실데이터 재확인 결과 |
| --- | --- |
| 5-3 `SNO` snow 캐스터 | 단기예보 207행 전부 캐스팅 성공, `TYPE_ERROR` 0 |
| 5-2 `masked_float` | `'*'`이 `MISSING`으로 판정됨(SPOP 9.8%, 연령·성별 40~85%) |
| 5-10 무의미 컬럼 | `RNUM`/`START_INDEX`/`END_INDEX`가 raw에는 여전히 오지만 선언에서 빠짐 |
| UUU/VVV 분해 | 기상 3개 소스 모두 선언·범위 정상, 실측값이 range 안에 들어옴 |
| 5-1 lookback | 아래 "잔여 누락" 참고 — 회수는 확인됐고 꼬리가 남는다 |
| 5-4 · 5-5 · 5-6 · 5-7 · 5-8 · 5-9 · 5-11 · 5-12 | **미착수 상태 그대로** 재확인됨 |

### 수정 순서와 그 이유

5-1(A2)을 마지막에 둔 것은 앞의 두 가지가 선행 조건이었기 때문이다.

1. **5-15 (httpx timeout)** — 기본값 5초가 실측 페이지 지연(최대 7.19초)보다 짧아
   느린 시점에 페이지마다 헛되게 재시도했다. A2는 이 경로를 2배 더 자주 태운다.
2. **5-10 (`RNUM` 등 제거)** — A2가 같은 시간대를 24번 재수집하므로 dedup 키를
   실데이터만으로 좁혀둬야 했다.
3. **5-16 (페이지 병렬 조회)** — 피크 17페이지가 순차로 최대 122초였고 예산은 150초다.
   A2 이전에 이 여유를 확보해야 했다.
4. **5-1 (A2)** — 위 셋이 끝난 뒤 DAG만 바꿨다.

### 5-1. `bike_rental_history` — 시간대별 대여의 26.4%를 영구히 놓친다 🔴

**증상.** `2026-08-18/18` 시간대 전체 16,896행을 받아 확인한 결과, `RTN_DT`가 19:00:00을
넘는 행이 **4,455건(26.4%)** 이다.

**원인.** 세 가지가 겹친다.

1. API는 `RENT_DT`(대여 시각) 기준으로 필터링하지만, **반납이 완료된 기록만** 목록에 나타난다
   (정렬이 `RTN_DT` 오름차순인 것으로 확인).
2. `path_suffix`가 `window_last`(= `window_start - 1s`)를 쓰므로, 18시대는
   18:05~19:00 윈도우 12개만 조회한다. 19:05 윈도우부터는 19시대로 넘어간다.
3. `backfill`은 **실패한 조각만** 재시도한다. 모든 조각이 성공했으므로 재조회가 일어나지 않는다.

결과: 18:41에 대여해 19:00 이후 반납한 기록은 **어떤 윈도우도 조회하지 않는다.**
반납 시각 분포는 18시 12,438 / 19시 3,890 / 20시 516 / 21시 39 / 22시 8 / 23시 1 /
다음날 00~01시 4 로, 최대 7시간 뒤까지 꼬리가 있다.

**다른 시간대에서도 재현된다** (`2026-08-18` 실측):

| 대여 시각대 | 전체 | 윈도우 내 | 누락 | 누락률 | +1h 재조회 시 회수 | +2h 재조회 시 회수 |
| --- | --- | --- | --- | --- | --- | --- |
| 03시 | 734 | 533 | 201 | 27.4% | 88.1% | 98.0% |
| 08시 | 11,481 | 9,943 | 1,538 | 13.4% | 87.1% | 98.6% |
| 12시 | 4,849 | 3,365 | 1,484 | 30.6% | 86.7% | 98.5% |
| 18시 | 16,896 | 12,441 | 4,455 | 26.4% | 87.3% | 98.8% |
| 21시 | 8,942 | 6,082 | 2,860 | 32.0% | 90.2% | 100.0% |
| **합계** | **42,902** | **32,364** | **10,538** | **24.6%** | | |

각 시간대를 그 종료 후 **2시간까지 재조회하면 누락의 98~100%를 회수**한다.

**수정 내용 (A2: 윈도우 재실행)** — 어댑터를 건드리지 않고 Airflow가 과거 윈도우를
`--force`로 다시 돌린다. 각 호출은 독립된 manifest 윈도우라 조각 키·`expected_total`·
재시도·백필 로직이 전부 그대로다.

- `airflow/config/sources.py`: `RENTAL_HISTORY_LOOKBACK_HOURS = 1`
- `airflow/orchestration/templates.py`: `kst_window_start_shifted(hours)` — 공통 기준
  시각을 시간 단위로 당긴다. `window_last`도 같이 당겨져 그 시간대를 조회하고,
  silver도 그 시간대의 `dt`/`hh` 파티션에 쓰인다(파티션·데이터 시각 불일치 없음).
- `airflow/orchestration/collector_task.py`: `build_collector_replay_task()`.
  `trigger_rule=ALL_DONE`이라 현재 tick의 성패와 무관하게 돌고, `run_inference`의
  상위가 아니라서 실패해도 추론을 막지 않는다.
- `airflow/dags/realtime_5min.py`: 현재 tick 수집 뒤에 사슬로 이어 붙인다 — 동시에
  띄우면 페이지 병렬(4) × 호출 수만큼 동시 요청이 늘어난다.

`--backfill`이 아니라 `--force`인 이유: backfill은 실패한 조각만 채우는데
(`pipeline.py:290`의 분기 4가 `skip=have_parts`로 부른다) 여기서 놓치는 것은 실패가
아니라 "그때는 아직 존재하지 않았던 데이터"다. 조각이 전부 성공했으므로 재시도 마커도
남지 않고(`pipeline.py:197`), 그냥 재실행하면 완결된 윈도우라 `SKIPPED`가 된다.

**실API 검증 (2026-08-18 18시대, 16,899건)**

| | 수집 건수 | 누락 |
| --- | --- | --- |
| 현행 (19:00까지 반납분) | 12,441 | 4,458 (**26.4%**) |
| 재조회 포함 (20:00까지) | **16,328** | 571 (**3.4%**) |

누락의 **87.2%**를 회수했다. 정상 tick(19:00 윈도우)과 재조회(20:00 tick − 1h)가 같은
`/2026-08-18/18`을 조회하고 조각 키도 `page-00001-01000`으로 동일함을 확인했다.

**2차 재점검 — 잔여 누락 🟡.** `RENTAL_HISTORY_LOOKBACK_HOURS = 1`이므로 회수 범위는
`+1h`까지다. 반납 지연의 꼬리를 시간대별로 다시 재어 보면:

| 대여 시각대 | 전체 | `>=+1h` (현행이 놓치던 양) | `>=+2h` (**lookback=1로도 못 잡는 잔여**) |
| --- | --- | --- | --- |
| 2026-08-18 18시 | 16,900 | 4,454 (26.4%) | **564 (3.3%)** |
| 2026-08-18 22시 | 6,948 | 1,547 (22.3%) | 0 (0.0%) |
| 2026-08-19 08시 | 13,233 | 1,680 (12.7%) | 201 (1.5%) |

`+2h`까지 늘리면 위 표의 잔여가 사실상 0이 된다(1차 표의 "+2h 재조회 시 회수" 98~100%와
일치한다). 비용은 tick당 이 소스 호출이 2개 → 3개로 느는 것이고, 실측 14.4초/호출이라
5분 tick 안에 그대로 들어간다. `RENTAL_HISTORY_LOOKBACK_HOURS = 2` 한 줄이 전부다.

한편 반납 시각이 대여 시각보다 **앞선 것처럼 보이는 행**(offset `-22`, `-17` 등)이
18시대에 7건, 22시대에 140건 있는데, 전부 자정을 넘겨 반납된 정상 기록이다
(`RTN_DT`의 날짜가 다음 날). 시(hour)만 비교하면 음수로 보일 뿐이다.

**`_window_start` 의미 변화(감수한 대가)**: 재조회가 같은 silver 파일을 덮어쓰므로
`compaction.dedup`의 `min` 집계 결과가 그 시간대의 첫 윈도우 값으로 균일해진다.
현행은 실제 첫 관측 시각을 담았다. 이 소스는 행에 `RENT_DT`/`RTN_DT`가 있어 데이터
손실은 아니지만, `compaction.py:166`이 밝힌 "처음 보인 시점" 근거는 이 소스에서
성립하지 않게 된다.

### 5-2. `living_population_grid` — 마스킹 `'*'`을 결측이 아니라 타입 오류로 판정한다 ✅ 수정 완료

**먼저 정정**: `'*'` → null 자체는 **의도된 동작**이다. `docs/collector/DataSchema.md:199-213`이
`M00`~`F70`의 결측 기준을 "값 없음(마스킹 `*` → null)"으로, `SPOP`을 "값 없음(null 실제 발생)"으로
명시하고 있다. 값이 사라지는 게 문제라고 본 앞선 판단은 틀렸다.

**실제로 남는 문제는 판정 경로다.** `'*'`은 `_judge_column`에서 결측(`MISSING`)이 아니라
**타입 오류(`TYPE_ERROR`)** 로 분류되고, `optional_outlier`(=`set_null`) 정책을 탄다.
문서가 말하는 "결측"과 엔진이 매기는 라벨이 어긋나 있어서:

- manifest의 `column_issues`에 `missing`이 아니라 `type_error`로 집계된다. 정상 마스킹과
  진짜 형식 오류가 같은 칸에 섞여, 지표로 둘을 구분할 수 없다.
- 누군가 `optional_outlier`를 `drop_row`로 바꾸거나 컬럼에 `on_outlier`를 얹으면
  **정상 마스킹 행이 통째로 폐기된다.** 반대로 `optional_missing`을 조정해도 `'*'`에는
  아무 영향이 없다 — 의도한 손잡이가 안 먹는다.
- 실측 규모: 표본 1,000행에서 나이·성별 28컬럼 28,000칸 중 **14,945칸(53.4%)**,
  `SPOP` **98행(9.8%)**. 즉 `type_error` 집계가 사실상 마스킹 카운터로 채워진다.

**수정 내용**: `core.masked.parse_masked_float`를 만들고 `types: [masked_float]`로 바꿨다
(`SPOP` + 나이·성별 28컬럼 = 29개). 이 캐스터는 `'*'`에 대해 `MaskedValue`를 던지고,
`_judge_column`이 그것만 잡아 `MISSING`으로 되돌린다. `MaskedValue`는 `ValueError`·`TypeError`를
상속하지 않는다 — `_try_cast`가 그 둘을 삼켜 다음 타입으로 넘어가므로, 상속하면 TYPE_ERROR로
돌아간다. 진짜 형식 오류(`'알수없음'`)는 그대로 `ValueError` → TYPE_ERROR로 남는다.

실데이터 1,000행 검증 (2026-08-19):

| | 수정 전 | 수정 후 |
| --- | --- | --- |
| `type_error` 총계 | 15,043 | **0** |
| `missing` 총계 | 0 | **15,043** |
| `SPOP` | `type_error: 98` | `missing: 98` |
| `kept` / `dropped` | 1,000 / 0 | 1,000 / 0 (변화 없음) |
| 실제 값 | `[5.32, 11.57, …]` | 동일 |

**함께 미구현**: `DataSchema.md:246`은 "모든 연령·성별 세부값이 존재하는 경우 세부합과
`SPOP`의 차이를 허용 오차 기반 Soft Validation으로 검사한다"고 적어뒀지만,
`living_population_grid.yaml`에 `policies.row`가 없어 이 검사가 존재하지 않는다.

### 5-3. `SNO`의 cm 표기가 파싱 실패한다 (겨울에 터지는 잠복 버그) ✅ 수정 완료

`core.precip.parse_precip`은 `"mm"`만 제거한다. 적설 표기를 넣으면:

| 입력 | 결과 |
| --- | --- |
| `'적설없음'` | `0.0` ✅ |
| `'1.0cm 미만'` | `0.5` ⚠️ (mm 규칙의 0.5를 cm에 그대로 적용) |
| `'1.0~4.9cm'` | `1.0` ⚠️ (cm 값이 mm 컬럼에 들어간다) |
| `'5.0cm'` | **`ValueError`** → `TYPE_ERROR` → `set_null` ❌ |
| `'5.0cm 이상'` | **`ValueError`** ❌ |

위 표의 **입출력은 `parse_precip`을 직접 실행해 확인한 실측**이다. 다만 `SNO`가 실제로
cm 표기로 오는지는 기상청 활용가이드의 값 규격(`1.0cm 미만` / `1.0~4.9cm` / `5.0cm 이상`)에
근거한 것이고, 8월 표본에는 `'적설없음'`만 나와 **눈 오는 날 응답으로 직접 확인하지는 못했다.**
확인 방법은 눈 예보가 있는 날 `getVilageFcst`를 호출해 `SNO` 원문을 보는 것뿐이다.

`collector/tests/test_precip.py`도 `'적설없음'` 한 값만 검증하고 cm 형태는 다루지 않았다.

**수정 내용**: 형태 규칙을 `core._amount.parse_amount(value, unit, none_label)`로 뽑고,
`core.precip.parse_precip`(mm/강수없음)과 `core.snow.parse_snow`(cm/적설없음)를 그 위에 얹었다.
`SNO`는 `types: [snow]`로 바꿨다. 두 파서는 **서로의 표기를 거부한다** — 자기 단위가 아닌
단위 문자열이 값에 보이면 `ValueError`다. `"1.0cm 미만"`이 `"미만"` 분기에 삼켜져 mm 파서를
조용히 통과하던 경로도 이 검사로 막힌다.

수정 후 동작 (`parse_snow`):

| 입력 | 결과 |
| --- | --- |
| `'적설없음'` | `0.0` |
| `'1.0cm 미만'` | `0.5` (cm 기준 대표값) |
| `'1.0~4.9cm'` | `1.0` |
| `'5.0cm'` | **`5.0`** (수정 전: `ValueError` → null) |
| `'5.0cm 이상'` | **`5.0`** (수정 전: `ValueError` → null) |
| `'강수없음'` | `ValueError` (단위 혼용 방지) |

실데이터 검증: 오늘자 단기예보 78행을 검증 엔진에 통과시켜 `SNO` 이슈 0건 · 값 `0.0` 유지를
확인했고, 겨울 표기 5종을 주입해 `[0.0, 0.5, 1.0, 5.0, 5.0]` · 이슈 0건을 확인했다.
loader는 `SNO`를 쓰지 않아 영향이 없다.

### 5-4. `performance_event`의 날짜가 행사 기간이 아니다 🟠

`SVCOPNBGNDT`/`SVCOPNENDDT`는 **예약 서비스 개설 기간**이다(중앙값 244일, 최대 3,925일).
이 값이 `cultural_events.start_date`/`end_date`로 적재되므로, 테니스장 예약 서비스가
1년 내내 진행되는 "행사"로 표시된다. `cultural_event`의 같은 컬럼은 실제 행사 기간이라
**한 테이블에 의미가 다른 두 날짜가 섞인다.**

선택지는 (a) `performance_event`를 별 테이블로 분리, (b) Gold에 `date_semantics` 구분 컬럼
추가, (c) `performance_event`를 행사 목록에서 제외 — 어느 쪽이든 의사결정이 필요하다.

**2차 재점검 — 규모를 실측했다. 1차 판단보다 심각하다.** 전량 606건 기준:

- **606건 전부(100%)가 `loader/transform.py:268`의 종료 필터(`end_date < today`)를 통과한다.**
  API가 이미 운영 중인 서비스만 돌려주기 때문이다. 즉 필터가 아무것도 걸러내지 못한다.
- 그 606건의 **기간 길이는 중앙값 230일 · 평균 450일 · 최대 3,925일**이고, 80.4%가 30일을
  넘는다. 종료일 최빈값은 `2026-12-31`(234건) · `2026-08-31`(228건)이라, 234건은 연말까지
  매일 "진행 중인 행사"로 남는다.
- **같은 테이블의 `cultural_event`는 기간 중앙값이 0일(당일 행사) · 90퍼센타일 49일이다.**
  한 테이블에 중앙값 0일짜리와 230일짜리가 섞인다 — 행사 밀도를 세면 후자가 상수처럼 깔린다.
- 운영 상태로 보면 606건 중 **`예약마감` 152건 · `접수종료` 72건**이다. 예약이 이미 끝난
  224건도 "진행 중인 행사"로 적재된다.
- 한 행 예시: `SVCNM='테니스장1(평일)-2026년 응봉공원(대현산배수지)'`,
  `SVCOPNBGNDT='2025-12-01'`, `SVCOPNENDDT='2026-12-31'`.
- 실제 이용 시각은 **미선언 컬럼** `V_MIN`/`V_MAX`(`'07:00'`/`'19:00'`)에 있고,
  운영 상태는 `SVCSTATNM`(접수중 333 · 예약마감 152 · 접수종료 72 · 안내중 40 ·
  예약일시중지 9)에 있다. 접수 기간은 또 `RCPTBGNDT`/`RCPTENDDT`로 따로 온다.
- 소스 `description`("서울시 체육시설 공연행사 정보")도 실제 내용(체육시설 **예약 서비스
  목록**)과 맞지 않는다. `MINCLASSNM` 분포가 테니스장 285 · 풋살장 82 · 축구장 55다.

따릉이 수요에 영향을 주는 것은 "예약 서비스가 열려 있다"가 아니라 "그 시간에 사람이
모인다"이므로, 현재 매핑은 피처로서 의미가 거의 없고 노이즈만 더한다. 위 (c)를 기본값으로
두고, 남긴다면 `V_MIN`/`V_MAX`/`SVCSTATNM`을 선언해 시간대 단위로 좁혀야 한다.

### 5-5. `weather_short_term_forecast`에 `TMN`/`TMX` 선언 누락 🟠

raw에 오는데 선언되지 않아 버려진다. 일 최저·최고기온은 "오늘 자전거 타기 좋은가"
같은 판단에 직접 쓰이는 값이다. 선언만 추가하면 된다(`float`, `-50~50`).

### 5-6. `population_realtime`에 관측 시각(`PPLTN_TIME`) 선언 누락 🟠

5분 주기 소스인데 silver에 관측 시각이 없다. `REPLACE_YN`(대체값 여부)도 함께 빠져
실측/추정 구분이 불가능하다. 둘 다 `str` 선언 추가로 해결된다.
연령대 비율 8개(`PPLTN_RATE_0`~`70`)와 `RESNT_PPLTN_RATE`/`NON_RESNT_PPLTN_RATE`도
필요 여부를 판단해야 한다.

`FCST_PPLTN`(12개 중첩 배열)은 현재 엔진 타입으로 표현 불가 — 별도 설계가 필요하다.

### 5-7. `bike_station_master`의 좌표 `range` 누락 🟠

`LAT`/`LOT`에 `range`가 없어 `0.0`이 정상값으로 통과한다. 표본 1,000건 중 **62건(6.2%)** 이
`0.0`이다. `bike_station_realtime`과 같은 값(`37.4~37.7` / `126.7~127.2`)을 선언하면 된다.

### 5-8. `cultural_event`의 `LAT`에 도분 표기가 섞인다 🟡

`'45.4215°N'` 같은 값이 `TYPE_ERROR` → `set_null`로 사라진다(표본 1,000건 중 1건).
좌표 정규화 캐스터를 붙이거나, 최소한 `quarantine`에 남도록 정책을 조정해야 한다.
`END_DATE`의 `'2626-08-08'`(원본 오타)도 loader에서 상한 검증이 없어 영구 진행 행사가 된다.

### 5-9. 무효한 `types` 선언 4건 🟡

`str`을 앞에 둔 다중 타입 선언은 뒤가 실행되지 않는다(§0).
전부 `bike_rental_history`에 있다.

| 컬럼 | 현재 | 실제 동작 | 의도했을 선언 |
| --- | --- | --- | --- |
| `USE_MIN` | `[str, int]` | `str` | `[int]` |
| `USE_DST` | `[str, float]` | `str` | `[float]` |
| `BIRTH_YEAR` | `[str, int]` | `str` | `[str]`(연도는 문자열이 안전) 또는 `[int]` |
| `RNUM` | `[str, int]` | `str` | 삭제 대상(5-10) |

### 5-10. 무의미 컬럼 3건이 적재된다 ✅ 수정 완료

`bike_rental_history`의 `START_INDEX`/`END_INDEX`(페이지네이션 에코, 전 행 `(0,0)`),
`RNUM`(응답 내 행번호). 데이터가 아니라 요청 메타다.

**수정 내용**: 세 컬럼을 `bike_rental_history.yaml`에서 제거했다. 회귀 방지는
`tests/test_source_configs.py::test_no_source_declares_response_pagination_meta`가
모든 소스에 대해 맡는다.

5-1(A2)보다 **먼저** 처리한 이유가 있다. `compaction.dedup`은 `_window_start`를 제외한
**전체 데이터 컬럼**으로 group by하므로 `RNUM`이 dedup 키에 들어간다. 이 API의 목록은
`RTN_DT` 오름차순이라, 반납이 지연 등록되면 앞자리에 끼어들어 뒤 기록의 `RNUM`이 한 칸씩
밀린다. 그러면 같은 대여가 서로 다른 행으로 남아 중복이 걷히지 않는다. A2는 같은
시간대를 24번 재수집하므로 이 노출이 2배가 된다.

`docs/collector/bootstrap-design.md:277`도 이 세 컬럼을 "API 페이지네이션 메타라 CSV에
없고 archive에도 의미가 없다"고 이미 적어뒀다 — 문서와 설정을 맞춘 셈이다.

### 5-11. `loader`가 `sta_addr`에 대여소명을 복제한다 🟡

`loader/transform.py:51`이 `"sta_addr": row["stationName"]`이다. 실제 도로명주소는
`bike_station_master.ADDR1`에 있고 수집도 되고 있지만, `loader/tables.yaml`에 항목이 없어
Gold에 들어가지 않는다. `docs/loader/implementation-plan.md`는 "`stationName` 파싱 혹은
별도 매핑"으로 적어뒀는데 어느 쪽도 구현되지 않았다.

덧붙여 `stationName`의 `"102. "` 접두는 표본 1,000건 중 848건에만 있어, 접두 제거로는
일관된 이름을 얻을 수 없다.

### 5-12. `living_population_grid`의 `H_DNG_CD` 공백 미제거 🟡

`'11110515     '`(13자)로 들어온다. 행정동 코드 조인 시 매칭 실패한다.
문자열 `strip` 처리가 필요하다(엔진에 공통 `trim` 옵션이 없어 캐스터나 전처리가 필요).

### 5-13. 선언 안 된 raw 컬럼 — 필요 여부 판단 필요 🔵

기능 결함은 아니지만 "왜 안 쓰는지"가 어디에도 없다. 의도적 제외라면 YAML에 한 줄 주석을
남기는 편이 안전하다(선언 누락과 의도적 제외를 구분할 방법이 현재 없다).

| 소스 | 미선언 개수 | 그중 검토 가치가 있는 것 |
| --- | --- | --- |
| `cultural_event` | 15 / 24 | `PRO_TIME`(행사 시각), `USE_TRGT`(대상), `THEMECODE` |
| `performance_event` | 14 / 24 | `V_MIN`/`V_MAX`(이용 시각), `SVCSTATNM`(접수 상태), `RCPTBGNDT`/`RCPTENDDT` |
| `population_realtime` | 15 / 22 | `PPLTN_TIME`·`REPLACE_YN`(→5-6), 연령대 비율 8개, `FCST_PPLTN` |
| `weather_short_term_forecast` | 2 / 14 | `TMN`/`TMX`(→5-5) |
| bike 3소스 · `living_population_grid` | 0 | — |

### 5-14. 컬럼 단위 품질 게이트가 없다 🔵

`max_missing_ratio`는 수집 완결도, `max_drop_ratio`는 폐기 행 비율이다. **컬럼이 몇 개나
null로 바뀌었는지는 어느 게이트도 보지 않는다.** 5-2(53% 유실), 5-3(겨울에 100% 유실),
5-8이 전부 "완결도 1.000 통과"로 지나간다. manifest의 `column_issues`에 집계는 남으므로,
컬럼별 `set_null` 비율 임계값을 게이트로 승격시키는 것을 검토할 만하다.

### 5-15. `httpx` 기본 timeout 5초가 실측 지연보다 짧았다 ✅ 수정 완료

`main.py:123`이 `httpx.Client()`를 인자 없이 만들어 httpx 기본값
`Timeout(timeout=5.0)`이 모든 단계에 적용됐다. 그런데 서울 API의 1000행 페이지 응답을
실측하면 시점에 따라 **0.6~7.19초**로 흔들린다. 5초를 넘는 응답은 `ReadTimeout` →
`TRANSIENT` → 라운드 재시도(15초·30초 대기)로 넘어간다. 데이터를 잃지는 않지만
`effective_fetch_budget()`을 헛되게 태운다.

**수정 내용**: `_HTTP_TIMEOUT = httpx.Timeout(connect=10.0, read=30.0, write=30.0, pool=30.0)`.
read 30초는 실측 최댓값의 4배 여유다. connect는 짧게 뒀다 — 연결이 안 되는 상황은
기다려서 나아지지 않고 라운드가 재시도한다.

### 5-16. 페이지 조회가 순차라 피크 시간대가 예산에 근접했다 ✅ 수정 완료

`bike_rental_history` 피크(18시)는 17페이지다. 페이지당 지연 실측 범위를 적용하면:

| 페이지당 지연 | 17페이지 순차 | 예산 150s 대비 |
| --- | --- | --- |
| 0.6s (관측 최선) | 10s | 여유 |
| 4.08s | 69s | 여유 |
| **7.19s (관측 최악)** | **122s** | **여유 28초** |

재시도 라운드 대기(15s+30s)가 한 번이라도 끼면 초과한다.

**수정 내용**: `adapters/seoul_openapi.py`에 `adapter_params.concurrency`를 추가했다
(기본 1 = 순차, 소스별 opt-in). `bike_rental_history`만 `concurrency: 4`를 켰다.

설계상 지킨 것 세 가지:

- **1페이지는 순차.** `total`을 모르면 페이지 목록을 만들 수 없고, 범위를 넘겨
  요청하면 서울 API가 래퍼 없이 `INFO-200`을 준다(→5-17).
- **순서 유지.** 완료 순서대로 내보내면(`as_completed`) 조각 키 순서가 흔들린다.
  앞 페이지를 기다리는 동안에도 뒤 요청은 이미 나가 있으므로 전체 소요는 완료순과 같다.
  미리 요청하는 개수를 `concurrency × 2`로 묶어 버퍼 메모리를 제한한다
  (`living_population_grid`는 254페이지라 전부 던지면 수백 MB가 된다).
- **`shutdown(wait=False, cancel_futures=True)`.** `with ThreadPoolExecutor(...)`의
  `__exit__`은 `shutdown(wait=True)`라, `fetch_with_rounds`가 마감 시한을 넘겨
  제너레이터를 버릴 때 큐가 비기를 기다려 마감 시한 방어를 무력화한다.

**실API 검증 (2026-08-18 18시대, 17페이지)**

| 방식 | 소요 | 조각 | 행 |
| --- | --- | --- | --- |
| 순차 | 45.5s | 17/17 | 16,899 |
| **병렬 4** | **14.4s** | 17/17 | 16,899 |

키 순서·`expected_total`·행 수가 모두 동일했다. 별도 벤치(더 느린 시점)에서는
순차 72.7s / 4워커 17.0s / 8워커 10.5s였다. 8이 아니라 4로 정한 이유는 17초면 예산에
8배 여유라 더 줄일 이득이 없고, A2가 한 tick에 이 소스 호출을 2개 띄우므로 같은 API에
대한 동시 요청이 배수로 늘기 때문이다.

### 5-17. 어댑터가 최상단 `CODE`를 읽지 않아 `INFO-200`을 영구 실패로 오판했다 ✅ 수정 완료

시작 인덱스가 `list_total_count`를 넘으면 서울 API는 서비스명 래퍼 없이 최상단에
`CODE`만 준다. 실측:

```
GET /tbCycleRentData/1001/2000/2026-08-18/03/   (total=734)
→ {"CODE": "INFO-200", "MESSAGE": "해당하는 데이터가 없습니다."}
```

`seoul_openapi.py:175`가 `wrapper.get("RESULT", {}).get("CODE")`만 읽어 `None`이 되고,
`_classify(None)`이 `PERMANENT`를 돌려줬다. `citydata_ppltn` 경로는 이 형태를 처리하는데
(`body.get("RESULT.CODE") or ...`) 페이지네이션 경로는 하지 않았다.

현재 코드는 `page_start <= total`로 범위를 넘지 않아 도달하지 않는 경로였지만, 병렬화가
페이지 경계를 더 자주 건드리므로 함께 고쳤다 — `_result_code()`가 래퍼와 최상단을 모두 본다.

**5-18의 전제**: 이 수정 덕분에 "범위를 넘겨 요청하면 `INFO-200`이 온다"는 것이 이미
정상 종료 신호로 처리된다. 5-18의 수정은 이 위에 얹으면 된다.

### 5-18. `bike_station_realtime` — 대여소의 63.4%를 매 tick 놓친다 🔴 (2차 재점검 신규)

**증상.** 실시간 대여소 정보가 매 5분마다 **1,000행에서 멈춘다.** 실제 운영 대여소는
**2,736곳**이다. 어댑터를 그대로 돌려 확인했다:

```
어댑터가 실제로 가져온 조각: [('page-00001-01000', expected_total=1000, rows=1000)]
총 행: 1000
```

**원인.** `bikeList`만 `list_total_count`의 의미가 다르다. 다른 서울 API는 전체 건수를
주는데(마스터 3,428 · 문화행사 19,495 · 생활인구 253,699 · 체육행사 606), 이 API는
**그 응답에 담긴 행 수**를 준다:

```
GET /bikeList/1/1000/     → list_total_count=1000, rows=1000
GET /bikeList/1001/2000/  → list_total_count=1000, rows=1000
GET /bikeList/2001/3000/  → list_total_count= 736, rows= 736   ← 여기가 끝
GET /bikeList/3001/4000/  → 래퍼 없음(INFO-200)
```

`seoul_openapi.py:227`이 `total = outcome.total or 0`으로 **1000**을 잡고, 이어지는
`while start <= total`이 `page_start = 1001 > 1000`이라 곧장 끝난다. 페이지 목록이 비어
`if not pages: return`으로 나간다.

**왜 지금까지 안 걸렸나.** 품질 게이트가 잡을 수 없는 형태다. `expected_total`도 1000,
수집한 행도 1000이라 `_missing_ratio`가 0이고, `max_missing_ratio: 0.0`을 정확히 통과한다.
**manifest에 "완결"로 기록되고 경고 한 줄 남지 않는다.** 1차 점검이 raw 키 ↔ 선언
차집합만 봐서 놓친 것도 같은 이유다 — 컬럼은 완전히 일치했다.

**영향.** 이 소스는 프로젝트에서 가장 아래에 깔린 데이터다.

- `loader/transform.py:31` `stations_from_silver` → `stations` 테이블(대여소 마스터 좌표·
  자치구·격자 매핑)이 36.6%만 채워진다.
- 같은 파일 `station_stock_from_silver` → `station_stock`(재고 시계열)도 같은 비율.
- `normalizer/station_master.py:98` → API 마스터의 좌표 `0.0` 77건을 실시간 좌표로
  폴백하는 경로가 대부분 빈손이 된다(5-7 참고).
- 추론 입력과 예측 대상 대여소 집합 전체.

**수정 방향.** `list_total_count`를 신뢰할 수 없는 소스를 위한 페이지네이션 모드가 필요하다.
`adapter_params`에 플래그(예: `pagination: probe`)를 두고, 그 소스는 total을 쓰지 않고
**빈 페이지(또는 `INFO-200`)가 나올 때까지** 진행한다. 5-17 덕분에 `INFO-200`은 이미
정상 종료로 분류되므로, 그 신호를 "더 없음"으로 해석하는 분기만 추가하면 된다.
`expected_total`을 모르는 동안은 병렬화할 수 없다는 기존 제약(주석 (1))이 그대로 적용돼
순차가 되는데, 3페이지짜리라 문제되지 않는다.

### 5-19. `population_realtime` — POI 순회 상한이 실제보다 15개 작다 🔴 (2차 재점검 신규)

**증상.** `seoul_openapi.py:168`의 `expected = 116`이 하드코딩이다. POI117~POI131을
하나씩 호출해 보니 **전부 정상 응답**한다:

> 신정네거리역 · 잠실새내역 · 잠실역 · 잠실롯데타워·석촌호수 · 송리단길·호수단길 ·
> 신촌 스타광장 · 보라매공원 · 서대문독립공원 · 안양천 · 여의서로 · 올림픽공원 ·
> 홍제폭포 · 송현녹지광장 · 시의회 앞 · 숭례문

POI132부터는 래퍼 없이 `RESULT.CODE`/`RESULT.MESSAGE`만 온다(= 유효 범위의 끝).
즉 **131개 중 15개(11.4%)가 영구 누락**이다.

**영향.** 누락 목록에 여의서로 · 안양천 · 올림픽공원 · 보라매공원처럼 따릉이 수요가
집중되는 하천·공원 지점이 들어 있다. 혼잡도 피처가 이 지역들에서만 통째로 비어 있다.

**게이트도 못 잡는다.** `expected_total=116`을 어댑터가 스스로 선언하므로, 116개를 다 받으면
완결로 기록된다. 5-18과 같은 구조의 결함이다 — **어댑터가 자기가 정한 분모로 자기를
채점한다.**

**수정 방향.** `expected`를 하드코딩에서 빼서 YAML `adapter_params`로 올린다
(예: `poi_range: [1, 131]`). 상한이 또 늘 수 있으므로, 상한 자체를 넘겨 보고 래퍼가
없으면 멈추는 탐색 방식이 더 낫지만, 매 tick 헛호출이 붙으므로 설정값 + 정기 확인
테스트 쪽이 비용이 싸다.

### 5-20. `living_population_grid` — 관측일이 수집일보다 5일 늦다 🟠 (2차 재점검 신규)

2026-08-19에 호출한 응답 253,699건이 **전량 `YMD=20260814`** 다(1페이지 `TT=00`,
중간 `TT=12`, 마지막 `TT=23` — 하루치 24시간이 통째로 온다). 즉:

- `dt` 파티션(수집일)과 `YMD`(관측일)가 **5일 어긋난다.** 파티션만 보고 시점을 판단하면
  5일 밀린 값을 쓰게 된다.
- API에 날짜 파라미터가 없어 `backfill: {enabled: true, max_age: 7d}`도 실질적으로
  **같은 최신 1일치를 다시 받는 것**뿐이다. 과거를 지정해 받을 수단이 없다.
- 지연 폭이 고정이라는 보장도 없다(휴일 등으로 늘어날 수 있다).

**현재 실피해는 없다.** `normalizer/station_master.py:79`가 이 소스에서 `CELL_ID`만 뽑아
격자 폴리곤을 만들고 인구 수치는 쓰지 않는다. 격자 경계는 날짜와 무관하다.
**단, 인구 수치를 피처로 쓰는 순간 5일 밀린 값이 된다.** `YMD`가 이미 선언돼 있으므로
downstream이 `dt`가 아니라 `YMD`를 기준으로 삼기만 하면 된다 — 지금 명시해 두는 편이 낫다.

### 5-21. `cultural_event` — 매일 받는 19,495건의 91.9%가 이미 끝난 행사다 🔵 (2차 재점검 신규)

표본 5,000건 중 `END_DATE`가 오늘 이전인 행이 4,597건(91.9%)이고, `STRTDATE` 연도는
2025년 2,617 · 2026년 2,383이다. `loader/transform.py:232`가 종료된 행사를 버리므로
**결과는 정상**이고, 이건 결함이 아니라 비용 항목이다. API에 기간 필터가 없어 전량을
받을 수밖에 없다. 20페이지 순차 조회라 1일 주기에서는 부담이 아니므로 **조치 불필요**로
남긴다 — 나중에 "왜 이렇게 많이 받나"를 다시 묻지 않도록 기록만 해 둔다.

---

## 부록: 점검 재현 방법

```bash
# 서울 열린데이터광장 계열
set -a && source .env && set +a
collector/.venv/bin/python - <<'PY'
import json, os, httpx, yaml
K = os.environ["SEOUL_OPENAPI_KEY"]
B = "http://openapi.seoul.go.kr:8088"
sid, svc, wrap = "cultural_event", "culturalEventInfo", "culturalEventInfo"
w = json.loads(httpx.get(f"{B}/{K}/json/{svc}/1/1000/", timeout=90).content)[wrap]
rows = w["row"]
declared = set(yaml.safe_load(open(f"collector/sources/{sid}.yaml"))["columns"])
print("total:", w["list_total_count"])
print("미선언 raw 컬럼:", sorted(set(rows[0]) - declared))
print("선언됐지만 raw에 없음:", sorted(declared - set(rows[0])))
PY
```

```bash
# 기상청 계열 (long format이라 category 집합을 비교한다)
collector/.venv/bin/python - <<'PY'
import json, os, httpx, yaml, collections
K = os.environ["KMA_APIHUB_KEY"]
B = "https://apihub.kma.go.kr/api/typ02/openApi/VilageFcstInfoService_2.0"
u = (f"{B}/getVilageFcst?authKey={K}&dataType=JSON&numOfRows=1000&pageNo=1"
     f"&base_date=20260819&base_time=0200&nx=60&ny=127")
body = json.loads(httpx.get(u, timeout=60).content)["response"]["body"]
cats = set(i["category"] for i in body["items"]["item"])
declared = set(yaml.safe_load(open("collector/sources/weather_short_term_forecast.yaml"))["columns"])
print("totalCount:", body["totalCount"])
print("미선언 category:", sorted(cats - declared))
PY
```

`bike_rental_history`의 누락률(5-1) 재현:

```bash
collector/.venv/bin/python - <<'PY'
import json, os, httpx, collections
K = os.environ["SEOUL_OPENAPI_KEY"]
B = "http://openapi.seoul.go.kr:8088"
d, h = "2026-08-18", "18"
def page(a, b):
    return json.loads(httpx.get(f"{B}/{K}/json/tbCycleRentData/{a}/{b}/{d}/{h}/", timeout=180).content)["rentData"]
tot = int(page(1, 1)["list_total_count"])
rows = [r for s in range(1, tot + 1, 1000) for r in page(s, min(s + 999, tot))["row"]]
missed = [r for r in rows if r["RTN_DT"] > f"{d} 19:00:00"]
print(f"{len(missed)}/{len(rows)} = {len(missed)/len(rows):.1%} 가 현행 윈도우 밖에서 반납된다")
print("RTN_DT 시간대 분포:", collections.Counter(r["RTN_DT"][11:13] for r in rows))
PY
```

### 2차 재점검(수집 범위) 재현 방법

컬럼 차집합이 아니라 **행이 다 들어오는지**를 보는 검사다. 5-18·5-19가 여기서 나왔다.

`bikeList`의 `list_total_count`가 전체가 아님을 보이는 검사:

```bash
set -a && source .env && set +a
collector/.venv/bin/python - <<'PY'
import os, json, httpx
K = os.environ["SEOUL_OPENAPI_KEY"]; B = "http://openapi.seoul.go.kr:8088"
for a, b in [(1, 1000), (1001, 2000), (2001, 3000), (3001, 4000)]:
    w = json.loads(httpx.get(f"{B}/{K}/json/bikeList/{a}/{b}/", timeout=90).content).get("rentBikeStatus", {})
    print(a, b, "list_total_count=", w.get("list_total_count"), "rows=", len(w.get("row", [])))
PY
```

어댑터가 실제로 몇 조각을 가져오는지(= 위 결함이 수집에 그대로 반영되는지):

```bash
cd collector && ../collector/.venv/bin/python - <<'PY'
import json, httpx
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
import config.loader as cl
from adapters.seoul_openapi import SeoulOpenApiAdapter
cfg = cl.load("bike_station_realtime", base_dir=Path("sources"))
w = SimpleNamespace(window_start=datetime.now(), window_end=datetime.now() + timedelta(minutes=5))
with httpx.Client(timeout=60) as c:
    parts = [(r.key, r.expected_total, len(json.loads(r.payload)["rentBikeStatus"]["row"]))
             for r in SeoulOpenApiAdapter.fetch(cfg, w, client=c)]
print(parts, "총 행:", sum(p[2] for p in parts))
PY
```

`citydata_ppltn`의 유효 POI 상한:

```bash
collector/.venv/bin/python - <<'PY'
import os, json, httpx
K = os.environ["SEOUL_OPENAPI_KEY"]; B = "http://openapi.seoul.go.kr:8088"
ok = []
for i in range(1, 146):
    r = json.loads(httpx.get(f"{B}/{K}/json/citydata_ppltn/1/5/POI{i:03d}/", timeout=60).content)
    rows = r.get("SeoulRtd.citydata_ppltn")
    if rows: ok.append((i, rows[0]["AREA_NM"]))
print("유효 POI 개수:", len(ok), "상한:", ok[-1])
PY
```

반납 지연 꼬리(5-1 잔여 누락) 측정:

```bash
collector/.venv/bin/python - <<'PY'
import os, json, httpx, collections
K = os.environ["SEOUL_OPENAPI_KEY"]; B = "http://openapi.seoul.go.kr:8088"
d = "2026-08-18"
def page(h, a, b):
    return json.loads(httpx.get(f"{B}/{K}/json/tbCycleRentData/{a}/{b}/{d}/{h}/", timeout=180).content)["rentData"]
for h in ["18", "22"]:
    p1 = page(h, 1, 1000); tot = int(p1["list_total_count"]); rows = p1["row"]
    for s in range(1001, tot + 1, 1000): rows += page(h, s, min(s + 999, tot))["row"]
    off = collections.Counter((int(r["RTN_DT"][11:13]) - int(h)) if r.get("RTN_DT") else "none" for r in rows)
    over2 = sum(v for k, v in off.items() if isinstance(k, int) and k >= 2)
    print(f"[{d} {h}시] total={tot} >=+2h(lookback=1로도 못 잡음)={over2} ({over2/tot:.1%})")
PY
```

모든 소스에 대해 raw ↔ 선언을 한 번에 대조하고 컬럼별 판정을 집계하는 스크립트는
`_judge_column`을 직접 불러 쓴다(엔진과 판정이 어긋나지 않게):

```python
from validation.engine import _judge_column
value, issue = _judge_column(raw_row.get(col), col, config.columns[col])
# issue.kind ∈ {MISSING, TYPE_ERROR, OUTLIER} 를 소스·컬럼별로 집계한다
```
