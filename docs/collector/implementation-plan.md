# collector 모듈 구현 계획

> 설계 결정의 배경과 근거는 [ADR 0001](../adr/0001-collector-module-design.md)을 참고한다. 이 문서는 그 결정을 어떻게 구현할지를 다룬다.

## 목차

1. [배경과 목표](#1-배경과-목표)
2. [설계 결정 요약](#2-설계-결정-요약)
3. [디렉토리 구조](#3-디렉토리-구조)
4. [config 스키마](#4-config-스키마)
5. [검증 엔진](#5-검증-엔진)
6. [상태 어휘와 manifest](#6-상태-어휘와-manifest)
7. [재개 로직](#7-재개-로직)
8. [로깅](#8-로깅)
9. [실행 인터페이스](#9-실행-인터페이스)
10. [구현 순서](#10-구현-순서)
11. [검증 방법](#11-검증-방법)

---

## 1. 배경과 목표

`collector/`는 아키텍처의 **Extract 단계**를 담당한다. 서로 다른 7개 소스를 각기 다른 주기로 수집한다.

| 소스 | 제공처 | 주기 |
| --- | --- | --- |
| 따릉이 실시간 대여정보 | 서울 열린데이터광장 | 5분 |
| 따릉이 대여이력 정보 | 서울 열린데이터광장 | 5분 |
| 서울 실시간 인구 데이터 | 서울 열린데이터광장 | 5분 |
| 기상청 초단기 실황·예보 | 기상청 API 허브 | 10분 |
| 기상청 단기예보 | 기상청 API 허브 | 3시간 |
| 서울시 문화행사·공연행사 | 서울 열린데이터광장 | 1일 |
| 서울 생활인구(250m) | 서울 열린데이터광장 | 1일 |

**설계 목표: 소스가 늘어도 공통 코드는 바뀌지 않는다.** 상태 어휘·재개·검증·manifest·로깅은 소스와 무관하게 완전히 동일하고, 소스마다 달라지는 것은 **fetch 어댑터 + YAML config** 뿐이다.

수집 흐름은 한 프로세스 안에서 완결된다.

```
fetch() → raw(메모리) ─┬→ bronze/  (API 응답 원본, 무손실)
                       │
                       └→ normalize() → rows → 검증·정책 적용 ─┬→ silver/     (정제본, Parquet)
                                                              └→ quarantine/ (폐기된 행 + 사유, JSONL)

                                                   → _manifest/ (실행 상태·집계·아티팩트 경로)
```

---

## 2. 설계 결정 요약

| 항목 | 결정 |
| --- | --- |
| bronze | API 응답 **원본 그대로** 무손실 저장. 정책이 바뀌어도 재수집 없이 재처리 가능 |
| 검증 정책 배치 | **소스 기본 4분면 + 컬럼별 오버라이드** |
| 정책 2단계 | ① 컬럼 정책이 값 교정 → ② 행 정책이 최종 판정 (행 정책은 선택, 생략 시 스킵) |
| 실패 시 재개 | **단계 체크포인트 + 멱등 재실행.** bronze가 있으면 fetch 스킵 |
| 멱등 키 | `(source_id, window_start)` — S3 경로와 manifest 경로를 결정 |
| 교정 행 표시 | silver에 메타 컬럼 `_row_status` (`ok` / `repaired`) 하나만 추가 |
| manifest 저장소 | **S3 단독.** collector가 DB 커넥션 없이 동작 |
| 코드 위치 | **`collector/` 단독.** 소비자가 생기면 그때 `libs/core`로 승격 |
| config 형식 | **YAML + 기동 시 검증.** 함수는 문자열 이름 → 레지스트리 조회 |
| 행 처리 | **행 단위 dict 순회.** pandas 벡터화 아님 |
| 어댑터 계약 | `fetch`(원본 획득) + `normalize`(행 = 레코드 변환) **2단계.** 소스별 응답 구조 차이를 여기서 흡수 |
| 어댑터 개수 | API **제공처 단위** 2개로 소스 7개를 모두 수용 |

---

## 3. 디렉토리 구조

[개발 환경 가이드](../getting-started.md)의 규약대로 wheel 빌드 없이 `uv run python main.py`로 실행한다. 하위 디렉토리는 `sys.path[0] == collector/` 이므로 그대로 import된다.

```
collector/
├── main.py                   # CLI 진입점 (인자 파싱 → pipeline 호출)
├── pipeline.py               # fetch→bronze→validate→silver 오케스트레이션 + 재개 분기
├── manifest.py               # manifest 스키마, 읽기/쓰기, 상태 어휘(RunStatus/Stage)
├── storage.py                # S3/MinIO 입출력, 경로 규칙 생성
├── logging_setup.py          # 구조화 로그 설정 (고정 필드 주입)
├── config/
│   ├── schema.py             # SourceConfig / ColumnSpec / Policies (pydantic)
│   └── loader.py             # YAML 로드 + 스키마 검증 + 정책 이름 검증 + 해시
├── validation/
│   ├── registry.py           # @policy / @row_policy 데코레이터, 이름→함수 매핑
│   ├── policies.py           # 공통 정책 함수 구현체 (소스 무관)
│   └── engine.py             # 행 순회 → 판정 → 정책 디스패치 → 결과 집계
├── adapters/
│   ├── base.py               # Adapter 프로토콜 + 어댑터 레지스트리
│   ├── seoul_openapi.py      # 서울 열린데이터광장 공통 (소스 5종이 공유)
│   └── kma_apihub.py         # 기상청 API 허브 (소스 2종이 공유)
├── sources/                  # 소스별 YAML — 새 소스는 원칙적으로 여기만 추가
│   ├── bike_station_realtime.yaml
│   └── ... (총 7개)
└── tests/
```

**어댑터는 소스 수만큼 필요하지 않다.** 서울 열린데이터광장 5종은 `{인증키}/{포맷}/{서비스명}/{시작}/{끝}/` 형태의 동일한 페이지네이션 규약을 쓰므로 어댑터 하나로 커버되고, 소스별 차이는 config의 `adapter_params`(서비스명·페이지 크기·응답 루트 키)로 흡수한다. **어댑터 2개로 소스 7개를 모두 수용한다.**

### 어댑터 계약

어댑터는 두 단계로 나뉜다. **소스마다 다른 응답 구조를 흡수하는 것이 어댑터의 책임**이고, 검증 엔진은 항상 "행 = 레코드"인 `list[dict]`만 받는다.

```python
class Adapter(Protocol):
    def fetch(self, config: SourceConfig, window: Window) -> RawResponse:
        """API를 호출해 원본 응답을 그대로 반환한다. bronze에 이 값이 저장된다."""

    def normalize(self, raw: RawResponse) -> list[dict]:
        """원본 응답을 행 = 레코드 형태로 변환한다."""
```

기상청 응답은 기온·습도·풍속이 **각각 별도 행으로 쌓인 long format**이라 `normalize`에서 pivot이 필요하다.

```python
# fetch 원본 (bronze에 저장되는 형태)
{"category": "T1H", "obsrValue": "31.6", "nx": 60, "ny": 127, "baseDate": "20260812", "baseTime": "1400"}
{"category": "REH", "obsrValue": "42",   ...}
{"category": "WSD", "obsrValue": "3.2",  ...}

# normalize 결과 (검증 엔진이 받는 형태)
{"baseDate": "20260812", "baseTime": "1400", "nx": 60, "ny": 127,
 "T1H": "31.6", "REH": "42", "WSD": "3.2", "PTY": "0", "RN1": "0", "UUU": "-3", "VVV": "-0.9", "VEC": "72"}
```

이 정규화 덕분에 config가 `T1H`·`REH`처럼 **컬럼별로 서로 다른 정상 범위**를 자연스럽게 선언할 수 있고, 검증 엔진에는 조건부 range 같은 개념이 필요 없다.

**어댑터가 공통으로 처리할 것**

- **응답 코드 검사**: 서울 API는 HTTP 200으로 응답하면서 본문에 에러 코드를 담는다. `rentBikeStatus.RESULT.CODE`가 `INFO-000`이 아니면 실패로 처리한다.
- **페이지네이션**: 서울 API는 `list_total_count`와 `{시작}/{끝}` 인덱스로 순회한다. 한 번에 최대 1,000건.
- **반복 호출**: 기상청은 격자(`nx`, `ny`)마다 별도 호출이 필요하다. 서울 전역을 커버할 격자 목록은 `adapter_params.grids`에 둔다.
- **재시도**: 타임아웃·429·5xx는 지수 백오프로 재시도한다.

---

## 4. config 스키마

```yaml
# collector/sources/bike_station_realtime.yaml
source_id: bike_station_realtime
description: 따릉이 실시간 대여정보

adapter: seoul_openapi              # adapters 레지스트리 키
adapter_params:
  service: bikeList
  page_size: 1000
  root_key: rentBikeStatus.row      # 응답에서 행 배열을 꺼낼 경로

schedule:
  interval: 5m                      # 문서화·window 계산용 (실제 스케줄러는 Airflow)

storage:
  bronze_format: json               # 원본 그대로 + gzip
  silver_format: parquet
  partition: [dt, hh]               # dt=YYYY-MM-DD/hh=HH

quality:
  max_drop_ratio: 0.05              # 초과 시 PARTIAL이 아니라 FAILED
  allow_empty: false                # 행 0건이면 FAILED (행사 소스는 true)

policies:                           # 4분면 기본값
  required_missing: drop_row
  required_outlier: drop_row
  optional_missing:  keep_null
  optional_outlier:  set_null
  row: null                         # 행 정책 (선택). null이면 이 단계 스킵

columns:                            # 실제 API 응답 기준 (전 필드가 문자열로 내려온다)
  stationId:
    types: [str]
    required: true
  stationName:
    types: [str]
    required: true
  rackTotCnt:
    types: [int]                    # 문자열 "15"로 와도 int로 해석되면 통과 + 캐스팅
    range: { min: 0, max: 200 }
  parkingBikeTotCnt:
    types: [int]
    range: { min: 0, max: 200 }
    on_outlier: clip_to_range       # 이 컬럼만 기본값 오버라이드
  shared:
    types: [int]
    range: { min: 0, max: 1000 }
  stationLatitude:
    types: [float]
    range: { min: 37.4, max: 37.7 }
  stationLongitude:
    types: [float]
    range: { min: 126.7, max: 127.2 }
```

기상청 소스는 어댑터의 `normalize`가 pivot한 뒤의 컬럼을 기준으로 선언한다. **`obsrValue` 하나가 아니라 관측 항목마다 별도 컬럼**이므로 범위를 정확히 걸 수 있다.

```yaml
# collector/sources/kma_ultra_srt_ncst.yaml (발췌)
adapter: kma_apihub
adapter_params:
  endpoint: getUltraSrtNcst
  root_key: response.body.items.item
  pivot: { key: category, value: obsrValue }   # long → wide 변환 기준
  grids: [[60, 127], [61, 127]]                # 서울 커버 격자 목록

columns:
  T1H: { types: [float], range: { min: -50, max: 50  } }   # 기온
  REH: { types: [float], range: { min: 0,   max: 100 } }   # 습도
  WSD: { types: [float], range: { min: 0,   max: 50  } }   # 풍속
  RN1: { types: [float], range: { min: 0,   max: 500 } }   # 1시간 강수량
  PTY: { types: [int],   enum: [0, 1, 2, 3, 5, 6, 7] }     # 강수형태
```

`loader.py`는 로드 시점에 다음을 수행한다.

1. pydantic 스키마 검증
2. `on_missing`·`on_outlier`·`row`에 적힌 이름이 레지스트리에 실제로 등록돼 있는지 검증
3. 파일 내용 SHA-256을 `config_version`으로 계산

**config 오타는 수집이 시작되기 전에 죽는다.**

---

## 5. 검증 엔진

### 판정 순서 (컬럼 하나당)

```
원시값 → ① 결측 판정 (None / "" / 센티널)
       → ② 타입 해석 (types 목록으로 캐스팅 시도)
       → ③ 범위·enum 판정
```

`types`는 "이 타입으로 해석 가능해야 한다"는 뜻이다. 서울 API는 숫자도 문자열로 주므로, 해석에 성공하면 **캐스팅된 값이 silver에 들어간다**(정규화 겸용). 실패하면 `TYPE_ERROR`로 판정한다.

판정 결과는 `Issue(column, kind, required, raw_value, spec)`이고, `kind`는 `MISSING | TYPE_ERROR | OUTLIER` 중 하나다.

### 정책 계약

```python
class Action(Enum):
    KEEP = "keep"              # 반환값으로 치환하고 행 유지
    DROP_ROW = "drop_row"      # 이 행을 silver에서 제외 → quarantine
    FAIL_BATCH = "fail_batch"  # 배치 전체 실패

@policy("clip_to_range")
def clip_to_range(value: Any, spec: ColumnSpec, row: dict, ctx: RunContext) -> tuple[Any, Action]:
    """값을 정상 범위의 경계로 잘라낸다."""
    return min(max(value, spec.range.min), spec.range.max), Action.KEEP
```

**초기 컬럼 정책 함수** (소스 무관, 전부 `validation/policies.py`)

`keep_null` · `set_null` · `fill_zero` · `fill_default` · `clip_to_range` · `drop_row` · `fail_batch`

**초기 행 정책 함수**

`drop_if_any_required_issue` · `drop_if_issue_count_exceeds` · `keep_always`

```python
@row_policy("drop_if_any_required_issue")
def drop_if_any_required_issue(row: dict, issues: list[Issue], ctx: RunContext) -> RowVerdict:
    """필수 컬럼에 문제가 하나라도 있으면 행을 폐기한다."""
```

새 정책이 필요하면 **함수 하나를 추가하고 YAML에서 이름으로 부른다.** 엔진 코드는 건드리지 않는다.

### 행 결과

값이 하나라도 교정됐으면 `_row_status = "repaired"`, 아니면 `"ok"`. 폐기된 행은 이슈 목록과 함께 quarantine으로 간다.

```jsonl
{"_issues":[{"column":"stationId","kind":"missing","required":true,"action":"drop_row"}],
 "_row_index":417,"stationId":null,"rackTotCnt":"10","parkingBikeTotCnt":"7"}
```

---

## 6. 상태 어휘와 manifest

| 상태 | 의미 |
| --- | --- |
| `RUNNING` | 실행 중 (시작 시 기록) |
| `SUCCEEDED` | silver까지 완료, 폐기된 행 없음 |
| `PARTIAL` | silver까지 완료, 일부 행 quarantine (`drop_ratio <= max_drop_ratio`) |
| `FAILED` | 단계 실패, 또는 `drop_ratio > max_drop_ratio`, 또는 `allow_empty=false`인데 0건 |
| `EMPTY` | 행 0건이지만 `allow_empty=true`라 정상 |
| `SKIPPED` | 같은 멱등 키가 이미 `completed` — 아무것도 하지 않고 종료 |

`Stage`(재개 근거): `fetched` → `bronze_written` → `validated` → `completed`

```json
{
  "source_id": "bike_station_realtime",
  "window_start": "2026-08-12T14:10:00Z",
  "window_end":   "2026-08-12T14:15:00Z",
  "status": "PARTIAL",
  "stage":  "completed",
  "attempt": 2,
  "started_at": "...", "ended_at": "...", "duration_ms": 4310,
  "artifacts": {
    "bronze":     "s3://.../bronze/bike_station_realtime/dt=2026-08-12/hh=14/1410.json.gz",
    "silver":     "s3://.../silver/bike_station_realtime/dt=2026-08-12/hh=14/1410.parquet",
    "quarantine": "s3://.../quarantine/bike_station_realtime/dt=2026-08-12/hh=14/1410.jsonl"
  },
  "counts": { "fetched": 2765, "kept": 2740, "repaired": 31, "dropped": 25 },
  "drop_ratio": 0.009,
  "column_issues": {
    "stationId":         { "missing": 25, "outlier": 0,  "type_error": 0 },
    "parkingBikeTotCnt": { "missing": 3,  "outlier": 28, "type_error": 0 }
  },
  "policy_actions": { "drop_row": 25, "clip_to_range": 28, "set_null": 3 },
  "config_version": "sha256:a3f9…"
}
```

`config_version`은 **이 silver가 어떤 정책으로 만들어졌는지**를 남긴다. 나중에 범위 기준을 바꾸면 이 해시로 재처리 대상을 골라낼 수 있다.

경로 규칙은 `storage.py` 한 곳에서만 만든다.

```
{layer}/{source_id}/dt={date}/hh={hour}/{HHMM}.{ext}
```

---

## 7. 재개 로직

```python
manifest = manifest.load(source_id, window_start)   # 없으면 None

if manifest and manifest.stage == Stage.COMPLETED and not force:
    return SKIPPED                                          # 멱등 — 재실행해도 안전
if manifest and manifest.stage >= Stage.BRONZE_WRITTEN and not force:
    raw = storage.read_bronze(manifest.artifacts.bronze)     # fetch 건너뛰기
else:
    raw = adapter.fetch(config, window)
    storage.write_bronze(raw)

rows = adapter.normalize(raw)                                # 원본 → 행 = 레코드
# 이후 검증 → silver → manifest 갱신
```

`normalize`는 bronze 재사용 여부와 무관하게 **항상 다시 수행한다.** 네트워크를 타지 않는 순수 변환이라 비용이 없고, bronze가 정규화 전 원본을 담고 있으므로 이 편이 단순하다.

실시간 API(5분 주기)는 몇 분만 지나도 그 시점 데이터를 영영 받을 수 없다. silver 저장에서 실패했을 때 fetch부터 다시 하면 **지금 시점의 다른 데이터로 덮어쓰게 되므로**, bronze 재사용이 재개의 핵심이다. `--force`는 이 분기를 무시하고 처음부터 다시 수행한다.

```
1회차:  fetch ✓ → bronze ✓ → 정제 ✓ → silver ✗ (S3 오류)   manifest: stage=validated, status=FAILED
2회차:  fetch ⤳ skip (bronze 재사용) → 정제 ✓ → silver ✓    manifest: stage=completed, status=PARTIAL
```

---

## 8. 로깅

배치당 몇 줄, **행당 0줄**. 행 상세는 quarantine 파일이 담당한다 (2,765행 × 288회/일이면 로그가 터진다).

```
INFO  source_id=bike_station_realtime window=2026-08-12T14:10Z stage=fetched  rows=2765 ms=1203
INFO  source_id=… stage=bronze_written key=s3://…/1410.json.gz bytes=482113
WARN  source_id=… stage=validated status=PARTIAL kept=2740 repaired=31 dropped=25 drop_ratio=0.009
INFO  source_id=… stage=completed key=s3://…/1410.parquet
```

`logging_setup.py`가 `source_id`·`window`·`attempt`를 고정 필드로 주입해 모든 로그에 자동으로 붙인다.

---

## 9. 실행 인터페이스

```bash
cd collector
uv run python main.py --source bike_station_realtime --window-start 2026-08-12T14:10:00Z [--force]
```

Airflow는 소스별 태스크에서 `data_interval_start`를 `--window-start`로 넘긴다. `window_end`는 config의 `schedule.interval`로 계산한다. **collector 자체는 스케줄을 모른다.**

**추가 의존성**: `httpx` · `pyyaml` · `pydantic` · `pyarrow` · `boto3`

**필요한 환경변수**

| 변수 | 용도 | 발급처 |
| --- | --- | --- |
| `SEOUL_OPENAPI_KEY` | 서울 열린데이터광장 소스 5종 | [열린데이터광장 인증키 신청](https://data.seoul.go.kr/together/mypage/actKeyMain.do) |
| `KMA_APIHUB_KEY` | 기상청 API 허브 소스 2종 | [기상청 API 허브](https://apihub.kma.go.kr/) |

두 변수는 `.env.example`에 자리를 만들어 뒀다. S3/MinIO 관련 변수는 이미 있다.

---

## 10. 구현 순서

각 단계는 테스트를 먼저 작성한다.

| 순서 | 대상 | 내용 |
| --- | --- | --- |
| 1 | config 스키마 + 로더 | pydantic 모델, YAML 로드, 정책 이름 존재 검증, 해시 |
| 2 | 정책 레지스트리 + 공통 정책 함수 | 데코레이터 등록, 함수 10종 |
| 3 | 검증 엔진 | 판정 3단계, 4분면 × 정책 디스패치, 행 정책, 집계 |
| 4 | storage + manifest | 경로 규칙, MinIO 읽기/쓰기, 상태 어휘 |
| 5 | 어댑터 base + `seoul_openapi` | `fetch`/`normalize` 프로토콜, `RESULT.CODE` 검사, 페이지네이션, 재시도 |
| 6 | pipeline | 4단계 오케스트레이션 + 재개 분기 |
| 7 | main.py + 로깅 | CLI, 구조화 로그 |
| 8 | 소스 YAML 확장 | 1개로 end-to-end 검증 → 나머지 6개 추가 (+ `kma_apihub` 어댑터의 격자 반복·pivot) |

1~3단계는 네트워크와 S3 없이 순수 단위 테스트로 끝난다.

---

## 11. 검증 방법

### 단위 테스트

```bash
cd collector && uv run pytest
```

- 정책 함수 10종 각각
- 검증 엔진: (필수/선택) × (결측/이상치/타입오류) 6조합 × 정책별 기대 동작
- 재개: manifest `stage`별 분기 (없음 / `bronze_written` / `completed` / `--force`)
- 어댑터 `fetch`: `httpx.MockTransport`로 페이지네이션·재시도·`RESULT.CODE` 에러 처리 검증
- 어댑터 `normalize`: 기상청 long → wide pivot이 관측 항목을 컬럼으로 올바르게 펴는지 검증

### end-to-end (MinIO 로컬)

```bash
make up   # 또는 ops/compose/docker-compose.yml
cd collector && uv run python main.py --source bike_station_realtime --window-start <최근 5분 경계>
```

MinIO 콘솔에서 `bronze/` · `silver/` · `_manifest/` 3개 객체가 생겼는지, manifest의 `counts`가 실제 행 수와 맞는지 확인한다.

### 재개 검증

silver 쓰기 직전에 예외를 주입해 실행한다. manifest가 `stage=validated`, `status=FAILED`로 남는지 확인한 뒤 재실행해서, 로그에 `stage=fetched`가 **없고** bronze 재사용으로 완료되는지 확인한다.

### 목표 달성 검증

두 번째 서울 열린데이터광장 소스(예: 문화행사)를 **YAML 파일 하나만 추가**해서 동작시킨다. 공통 코드에 한 줄도 손대지 않고 성공하면 설계 목표가 충족된 것이다.
