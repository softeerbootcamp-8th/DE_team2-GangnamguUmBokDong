# collector 구현 작업 단위

> 설계 근거는 [ADR 0001](../adr/0001-collector-module-design.md), 구현 상세는 [구현 계획](./implementation-plan.md)을 참고한다. 이 문서는 그 계획을 **어떤 이슈·브랜치 단위로 쪼갤지**를 다룬다.

## 브랜치 규칙

[CONTRIBUTING](../../.github/CONTRIBUTING.md#브랜치-이름-규칙) 요약이다.

- `타입/설명` 형식, 소문자 + 하이픈(kebab-case)
- 타입: `feature` · `fix` · `refactor` · `docs` · `test` · `chore`
- 작업 브랜치는 **항상 최신 `develop`에서** 딴다
- 하나의 이슈는 하나의 브랜치에서 처리하고, PR을 거쳐 병합한다

---

## 작업 목록

| # | 브랜치 | 내용 | 주요 산출 | 선행 |
| --- | --- | --- | --- | --- |
| 1 | `chore/collector-deps` | `httpx`·`pyyaml`·`pydantic`·`pyarrow`·`boto3` 의존성 추가와 디렉토리 스켈레톤 | `pyproject.toml` | — |
| 2 | `feature/collector-config-loader` | config 스키마, YAML 로드, 정책 이름·`row_params` 검증, SHA-256 해시 | `config/schema.py`<br>`config/loader.py`<br>*(후속)* `tests/conftest.py`의 `ColumnSpecStub`을 실제 `ColumnSpec`으로 교체 | 1·3 |
| 3 | `feature/collector-policy-registry` | 계약 타입, `@policy`·`@row_policy` 데코레이터(params 모델 등록), 공통 정책 함수 10종 | `validation/types.py`<br>`validation/registry.py`<br>`validation/policies.py` | 1 |
| 4 | `feature/collector-storage-manifest` | 경로 규칙(**조각 키 기반**), bronze 조각 쓰기·읽기·정리, **`_retry_queue` 마커 I/O**, MinIO 입출력, 상태 어휘와 manifest 스키마(**완결도 필드**) | `storage.py`<br>`manifest.py` | 1 |
| 5 | `feature/collector-validation-engine` | 판정 3단계, 4분면 정책 디스패치, 행 정책, 결과 집계 | `validation/engine.py` | 2·3 |
| 6 | `feature/collector-seoul-adapter` | 어댑터 프로토콜(**`FetchResult` yield**), **실패 3범주·라운드 재시도·`fetch_budget`**, 서울 어댑터(`RESULT.CODE` 검사·페이지네이션) | `adapters/base.py`<br>`adapters/seoul_openapi.py` | 2 |
| 7 | `feature/collector-pipeline` | 조각 즉시 저장 루프를 포함한 fetch→bronze→validate→silver 오케스트레이션, **재개 분기 4가지**, **게이트 2종과 완결도 집계** | `pipeline.py` | 4·5·6 |
| 8 | `feature/collector-cli-logging` | CLI 인자 파싱(**`--backfill`**), 구조화 로그 고정 필드 주입 | `main.py`<br>`logging_setup.py` | 7 |
| 9 | `feature/collector-kma-adapter` | 기상청 어댑터 — 격자 반복 호출과 long→wide pivot | `adapters/kma_apihub.py` | 6 |
| 10 | `feature/collector-source-configs` | 소스 YAML 7종 작성과 end-to-end 검증 | `sources/*.yaml` | 8·9 |
| 11 | `feature/collector-backfill-dag` | 백필 DAG — `_retry_queue` LIST → `--backfill` 호출, 만료 처리. **주기와 소스별 `max_age` 확정** | `airflow/dags/…` | 10 |


- [x] 1
- [ ] 2
- [x] 3
- [x] 4
- [ ] 5
- [ ] 6
- [ ] 7
- [ ] 8
- [ ] 9
- [ ] 10
- [ ] 11


[구현 계획 11절](./implementation-plan.md#11-구현-순서)의 9단계를 11개로 나눴다. 의존성 추가를 앞으로 뺐고(#2·#3·#4를 돌리려면 먼저 병합돼야 한다), "기상청 어댑터 + YAML 7종"은 성격이 달라 #9·#10으로 분리했다. #11은 실제 마커가 쌓여야 검증할 수 있어 #10 뒤에 둔다.

**부분 실패 허용과 백필**([ADR 0004](../adr/0004-partial-fetch-and-backfill.md))은 새 이슈를 만들지 않고 #4·#6·#7·#8에 흡수했다. 계약 변경이라 나중에 얹는 것이 아니라 처음부터 그 모양으로 만드는 편이 싸다. 특히 **조각 키는 S3 경로에 새겨지므로 나중에 바꾸면 이미 쌓인 bronze를 마이그레이션해야 한다.** 외부 잡인 #11만 분리했다.

---

## 진행 순서

선행 관계상 동시에 진행할 수 있는 묶음이다.

| 라운드 | 동시 진행 가능 |
| --- | --- |
| 1 | #1 |
| 2 | #3 · #4 |
| 3 | #2 |
| 4 | #5 · #6 |
| 5 | #7 · #9 |
| 6 | #8 |
| 7 | #10 |
| 8 | #11 |

```
#1 deps
 ├─ #4 storage ──────────────────────────────┐
 └─ #3 policy ─ #2 config ─┬─ #5 engine ─────┼─ #7 pipeline ─ #8 cli ─┐
                           └─ #6 seoul ──────┘                        ├─ #10 configs ─ #11 backfill
                                       └─ #9 kma ─────────────────────┘
```

#1 병합 직후 #3·#4를 나눠 잡고, #3이 끝나면 #2를 잡는 것이 가장 빠르다. #2~#5는 네트워크와 S3 없이 순수 단위 테스트로 끝난다.

---

## 완료 기준

각 이슈는 [구현 계획 12절](./implementation-plan.md#12-검증-방법)의 해당 단위 테스트를 통과하면 완료로 본다.

**#10이 설계 목표의 검증 지점이다.** 두 번째 서울 열린데이터광장 소스를 YAML 파일 하나만 추가해 공통 코드에 한 줄도 손대지 않고 동작시키면, "소스가 늘어도 공통 코드는 바뀌지 않는다"는 목표가 충족된 것으로 본다.
