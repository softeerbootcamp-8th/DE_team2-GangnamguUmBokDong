# ADR-0003: Bronze는 API 응답 조각을 도착 즉시 저장한다

- 상태: 채택
- 결정일: 2026-08-12
- 작성자: Data Engineering 2팀
- 대체 대상: 없음
- 대체한 ADR: 없음

## 배경

한 source window를 수집하려면 페이지나 기상 격자별로 API를 여러 번 호출한다. 모든 응답을 메모리에 모은 뒤 한 번만 저장하면 중간 실패 때 이미 받은 원본까지 잃고, 실시간 API의 과거 window는 다시 확보하지 못할 수 있다.

Adapter가 S3 저장까지 담당하면 외부 API 규약과 저장소 책임이 결합되고 단위 테스트에도 S3가 필요해진다. 반대로 Silver를 조각마다 만들면 window 전체의 품질 임계치를 판단하기 전에 불완전한 결과가 게시될 수 있다.

## 결정

1. Adapter의 `fetch`는 요청 단위 `FetchResult`를 순회 가능한 형태로 반환한다.
2. Pipeline은 성공 응답을 받는 즉시 요청 식별 key로 Bronze 조각을 저장한다.
3. Adapter는 저장소를 모르며, `normalize`는 네트워크를 사용하지 않고 전체 조각을 행 목록으로 변환한다.
4. Pipeline은 window 전체를 정규화·검증한 뒤 Silver Parquet 하나를 만든다. 품질 게이트를 통과하기 전에는 Silver를 쓰지 않는다.
5. 일반 재수집과 backfill correction은 기존 원본을 비우지 않고 새 immutable Hot
   Bronze revision에 쓴다. 재개는 manifest가 가리키는 revision과 조각만 읽어 이전
   실행의 잔여 조각이 섞이지 않게 한다.
6. 현재 window 크기에서는 조각 payload와 정규화 행을 메모리에 유지한다. 규모가 커지면 Bronze 재읽기 또는 별도 처리 경계를 다시 결정한다.
7. 검증된 날짜의 모든 Hot revision은 원본 gzip bytes를 보존하는 날짜 단위 Cold
   Bronze로 묶고, Hot 객체만 30일 뒤 만료한다.

## 근거

- 즉시 저장은 실패 시 원본 손실 범위를 요청 한 건으로 제한한다.
- 요청 parameter에서 만든 안정적인 key는 병렬 fetch의 완료 순서와 무관하며 특정 누락 조각을 다시 요청할 수 있게 한다.
- 저장 책임을 Pipeline에 두면 Adapter를 HTTP 입력만으로 테스트할 수 있다.
- Silver를 window 단위로 유지하면 `max_drop_ratio`를 쓰기 전에 판정하고 하류의 파일 조각 병합을 피할 수 있다.

## 결과

Bronze 객체 수와 manifest 관리 비용은 늘지만, 중간 실패 후에도 받은 원본을 보존하고 재처리할 수 있다. 정규화와 검증은 여전히 window 전체를 메모리에 올리므로 source 크기가 커질 때 재평가가 필요하다.

초기에는 조각 순번과 첫 실패 즉시 중단을 사용했지만, 안정적인 요청 key·재시도 라운드·부분 수집 게이트로 대체됐다. 현재 동작은 [ADR-0004](0004-partial-fetch-and-backfill.md)가 확장한다.

## 관련 자료

- `collector/adapters/base.py`
- `collector/pipeline.py`
- `collector/storage.py`
- `collector/tests/test_adapters_base.py`
- `collector/tests/test_storage.py`
- [ADR-0004](0004-partial-fetch-and-backfill.md)
