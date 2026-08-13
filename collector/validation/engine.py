"""행 순회 → 판정 → 정책 디스패치 → 결과 집계.

구현 예정: docs/collector/implementation-issues.md #5
설계 근거: docs/collector/implementation-plan.md 5절 (검증 엔진)

## 이 모듈의 역할

어댑터가 넘긴 `list[dict]`를 받아 **silver에 들어갈 행 · quarantine으로 갈 행 · manifest
에 남을 집계**를 만든다. 소스 이름을 알지 못하고, 판단 기준은 전부 config에서 온다.

계약 타입은 `validation/types.py`, 정책 조회는 `validation/registry.py`에서 가져온다.

## 판정 순서 (컬럼 하나당 3단계)

    원시값 → ① 결측 판정(None · "" · 센티널)
           → ② 타입 해석(`types` 목록으로 캐스팅 시도, 실패하면 TYPE_ERROR)
           → ③ 범위 · enum 판정

②에서 해석에 성공한 **캐스팅된 값**이 silver에 들어간다. 서울 API가 숫자를 문자열로
주므로 이 단계가 정규화를 겸한다. 판정 결과는
`Issue(column, kind, required, raw_value, spec)`이다.

## 정책 디스패치

- 4분면 — `(required 여부) × (missing / outlier)`으로 정책 이름을 고른다.
- 컬럼에 `on_missing` · `on_outlier`가 선언돼 있으면 **그쪽이 4분면 기본값을 이긴다.**
- **`TYPE_ERROR`는 outlier 계열로 보낸다.** 즉 `required_outlier` · `optional_outlier`를
  쓴다. 단 **컬럼별 `on_outlier` 오버라이드는 적용하지 않고 4분면 기본값만 쓴다.**
  오버라이드는 "타입은 맞지만 범위를 벗어난 값"을 겨냥한 설정이고, `clip_to_range`처럼
  값을 교정하는 정책은 캐스팅되지 않은 값에 적용할 수 없기 때문이다.
- 정책 2단계 — ① 컬럼 정책이 값을 교정한 뒤 ② 행 정책이 최종 판정을 내린다.
  `policies.row`가 null이면 ② 단계를 스킵한다.
- 행 정책 호출 시 config의 `policies.row_params`를 검증된 모델로 넘긴다. params가 없는
  정책에도 같은 4인자로 호출한다(정책별 분기를 두지 않는다).
- `FAIL_BATCH`를 받으면 순회를 즉시 중단하고 배치 실패로 올린다.

## 행 단위 산출물

- `_row_status` — 값이 하나라도 교정됐으면 `"repaired"`, 아니면 `"ok"`.
  **silver에 추가되는 메타 컬럼은 이것 하나뿐이다.**
- quarantine 레코드 — 폐기된 행에 `_issues`(column · kind · required · action)와
  `_row_index`를 붙여 JSONL 한 줄로 남긴다.

        {"_issues":[{"column":"stationId","kind":"missing","required":true,
          "action":"drop_row"}], "_row_index":417, "stationId":null, ...}

## 집계 (manifest가 그대로 받아쓰는 형태)

`counts{fetched, kept, repaired, dropped}` · `drop_ratio` ·
`column_issues{컬럼: {missing, outlier, type_error}}` · `policy_actions{정책명: 횟수}`

집계 키를 manifest 스키마와 맞춰 둔다. pipeline이 변환 없이 넘길 수 있어야 한다.
**`type_error`는 집계에서 별도 항목으로 유지한다** — 디스패치는 outlier와 합치지만
집계는 구분해야 원인을 추적할 수 있다.

**`counts.expected` · `missing` · `completeness`는 이 모듈이 만들지 않는다.** 수집
단계에서 나오는 값이라 pipeline이 채운다. 이 모듈이 보는 `fetched`는 "받은 행 수"이고
그것이 "받았어야 할 행 수"인지는 알지 못한다. `drop_ratio`의 분모를 `fetched`로 두는
설계도 여기서 나온다 — 검증 엔진은 수집이 완전했는지 모르는 채로 폐기율만 판정한다.

## 주의

- 행 단위 dict 순회로 구현한다(pandas 벡터화 아님).
- 행당 로그는 0줄이다. 행 상세는 quarantine 파일이 담당한다.
- 원본 행을 제자리에서 변형하지 말고 새 dict를 만든다. `raw_value`를 quarantine에
  남겨야 하므로 원시값이 살아 있어야 한다.

검증(계획서 12절): (필수/선택) × (결측/이상치/타입오류) **6조합**을 정책별 기대 동작과
함께 확인한다. 타입오류 2조합은 컬럼 오버라이드가 있어도 4분면 기본값이 적용되는지를
함께 본다.
"""
