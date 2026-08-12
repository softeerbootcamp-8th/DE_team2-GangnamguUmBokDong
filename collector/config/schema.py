"""SourceConfig / ColumnSpec / Policies pydantic 모델.

구현 예정: docs/collector/implementation-issues.md #2
설계 근거: docs/collector/implementation-plan.md 4절 (config 스키마)

## 이 모듈의 역할

YAML 한 개가 곧 소스 한 개다. 이 모듈은 그 YAML이 가질 수 있는 모양을 못박는다.
어댑터 · 검증 엔진 · pipeline은 dict가 아니라 **검증이 끝난 이 모델**을 받으므로,
config 오타가 실행 도중 `KeyError`로 튀어나오지 않는다.

## 모델 구성

- `SourceConfig` (최상위)
  - `source_id` · `description`
  - `adapter` — 어댑터 레지스트리 키(`seoul_openapi` · `kma_apihub`)
  - `adapter_params` — 어댑터가 해석하는 자유 dict. **스키마를 고정하지 않는다.**
    어댑터마다 필요한 키가 다르고, 여기를 고정하면 새 소스가 공통 코드를 건드리게 된다.
  - `schedule` · `storage` · `quality` · `policies` · `columns`
- `Schedule` — `interval`(`5m` · `10m` · `3h` · `1d`). `window_end` 계산과 문서화용이고
  실제 스케줄링은 Airflow가 한다. 문자열을 `timedelta`로 바꾸는 validator를 여기 두면
  main이 파싱을 다시 하지 않는다.
- `Storage` — `bronze_format`(원본 그대로 + gzip) · `silver_format`(parquet) ·
  `partition`(예: `[dt, hh]` → `dt=YYYY-MM-DD/hh=HH`)
- `Quality` — `max_drop_ratio`(0~1. 초과하면 PARTIAL이 아니라 **FAILED**) ·
  `allow_empty`(행 0건 허용 여부. 행사 소스만 true)
- `Policies` — 4분면 기본값 `required_missing` · `required_outlier` ·
  `optional_missing` · `optional_outlier`(필수), `row`(선택, null이면 행 정책 스킵),
  `row_params`(선택, 행 정책에 넘길 인자 dict).
  정책 값은 전부 **함수 이름 문자열**이다. 함수를 직접 참조하지 않는 대가로 loader가
  이름 존재와 `row_params` 형태를 검증한다.
- `ColumnSpec` — `types` · `required` · `range{min,max}` · `enum` ·
  `on_missing` · `on_outlier`(이 컬럼에 한해 4분면 기본값을 덮어쓴다) ·
  `fill_default`가 쓸 기본값.

## 판정에서 이 필드들이 갖는 의미

- `types`는 "이 타입으로 해석 가능해야 한다"는 뜻이다. 서울 API는 숫자도 문자열로
  내려주므로 해석에 성공하면 **캐스팅된 값이 silver에 들어간다**(정규화 겸용).
  실패하면 `TYPE_ERROR`가 된다.
- `on_outlier`는 **`OUTLIER`에만 적용된다.** `TYPE_ERROR`는 4분면 기본값을 쓴다
  (계획서 5절). 값을 교정하는 정책을 캐스팅 실패 값에 적용할 수 없기 때문이다.
- `range`와 `enum`은 보통 배타적으로 쓴다(`PTY`는 enum, `T1H`는 range). 둘을 동시에
  선언하는 것을 허용할지는 #2에서 정한다.
- `row_params`의 **내용**은 이 모델이 검증하지 않는다. 정책마다 필요한 인자가 다르므로
  `dict`로 받아 두고, loader가 해당 정책에 등록된 params 모델로 검증한다.

## 주의

- 선언되지 않은 키는 거부한다(`extra="forbid"`). 이 모델의 목적은 config 오타를 수집
  시작 전에 죽이는 것이다.
- `required` 기본값을 false로 두면 미선언 컬럼이 4분면의 `optional_*`로 디스패치된다.
  기본값 선택이 검증 동작을 바꾸므로 명시적으로 정하고 문서에 남긴다.
- 모델은 불변(frozen)으로 둔다. config는 실행 중에 바뀌지 않는다.
"""
