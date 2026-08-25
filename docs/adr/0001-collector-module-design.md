# ADR-0001: Collector는 원본 보존과 설정 기반 검증을 한 실행에서 수행한다

- 상태: 채택
- 결정일: 2026-08-12
- 작성자: Data Engineering 2팀
- 대체 대상: 없음
- 대체한 ADR: 없음

## 배경

실시간 API는 수집 시점을 놓치면 같은 데이터를 다시 받기 어렵지만, 스키마와 품질 정책은 계속 바뀔 수 있다. 또한 서울 열린데이터광장과 기상청의 응답 구조가 서로 달라, 소스별 분기를 파이프라인에 직접 추가하면 수집원이 늘어날수록 공통 코드가 복잡해진다.

Airflow 재시도는 태스크를 처음부터 실행하므로 이미 받은 원본을 보존하지 않으면 최초 window를 잃을 수 있다. 품질 오류가 있는 행을 폐기하면서 근거를 남기지 않으면 정책의 적절성도 검증할 수 없다.

## 결정

Collector는 다음 경계를 유지한다.

1. API 응답 원본을 Bronze에 먼저 저장하고, 같은 실행에서 정규화·검증한 Silver를 만든다.
2. `(source_id, window_start)`를 논리적 실행 키로 사용하고 manifest에 단계와 품질 집계를 기록한다. 재시도에서는 저장된 Bronze를 재사용한다.
3. 소스의 스키마, 품질 임계치, 정책과 adapter parameter는 `collector/sources/*.yaml`에 선언한다. 설정은 수집 전에 검증한다.
4. Adapter는 API 제공처 단위로 구현하고 `fetch`와 `normalize`를 분리한다. `fetch`는 원본 획득만, `normalize`는 네트워크 없는 행 변환만 담당한다.
5. 컬럼 정책과 행 정책은 이름 기반 registry에 등록한다. 컬럼 정책으로 값을 교정한 뒤 행 정책으로 유지·폐기를 결정한다.
6. Silver에는 `_row_status`로 `ok` 또는 `repaired`를 표시하고, 폐기된 행과 사유는 Quarantine에 보존한다.
7. 검증 엔진은 실제 공유 소비자가 생기기 전까지 `collector/validation/`에 둔다.

## 근거

- Bronze를 보존하면 정책 변경이나 저장 실패가 발생해도 외부 API를 다시 호출하지 않고 재처리할 수 있다.
- YAML 설정과 registry를 사용하면 새 소스를 추가할 때 공통 검증 엔진을 수정하지 않아도 된다.
- API 제공처 단위 Adapter는 같은 인증·페이지네이션 규약을 중복 구현하지 않게 한다.
- 값 교정과 행 폐기를 분리하면 컬럼 단위 sentinel 처리와 복수 컬럼 관계 검증을 함께 지원할 수 있다.
- manifest와 Quarantine은 별도 DB 연결 없이 실행 상태, 품질 집계와 문제 행을 추적하게 한다.

## 결과

원본 재현성과 재시도 멱등성을 확보하고, 소스 추가와 품질 정책 변경의 범위를 설정 파일과 정책 함수로 제한한다. 대신 Bronze와 Silver를 모두 저장하고 manifest·Quarantine을 관리해야 하며, 현재 행 단위 검증은 데이터 규모가 커지면 성능을 다시 평가해야 한다.

부분 조각 저장과 streaming 경계는 [ADR-0003](0003-bronze-streaming-and-scaling-boundaries.md)이 구체화한다. 누락 조각 재시도, `max_missing_ratio`와 backfill은 [ADR-0004](0004-partial-fetch-and-backfill.md)가 이 결정을 확장하며, 원본 보존·설정 기반 검증·논리 실행 키 원칙은 그대로 유지한다.

현재 구현은 Silver를 content-addressed immutable object로 기록하고 source authority manifest가 유효 revision을 가리키도록 발전했다. 이는 이 ADR의 재현성과 멱등성 원칙을 강화한 후속 구현이며, 상세 계약은 Collector 코드와 별도 후속 ADR에서 관리한다.

## 관련 자료

- `collector/pipeline.py`
- `collector/config/schema.py`
- `collector/adapters/base.py`
- `collector/validation/`
- `collector/manifest.py`
- `collector/storage.py`
- [ADR-0003](0003-bronze-streaming-and-scaling-boundaries.md)
- [ADR-0004](0004-partial-fetch-and-backfill.md)
