## PR 유형

어떤 변경 사항이 있나요?

- [ ] 새로운 기능 추가
- [x] 버그 수정
- [ ] 사용자 UI 디자인 변경
- [ ] 코드에 영향을 주지 않는 변경사항(오타 수정, 탭 사이즈 변경, 변수명 변경)
- [ ] 코드 리팩토링
- [x] 주석 추가 및 수정
- [x] 코드 수정
- [ ] 문서 수정
- [x] 테스트 추가, 테스트 리팩토링
- [ ] 파일 혹은 폴더명 수정
- [ ] 파일 혹은 폴더 삭제


## 개요
<!-- 이 PR이 무엇을, 왜 하는지 1~3줄로 요약 -->
### Collector pipeline 단계별 실행시간 로그 추가
population_realtime 수집 과정에서 HTTP 호출 이후 장시간 로그가 출력되지 않는 현상을 분석할 수 있도록 collector/pipeline.py에 주요 단계별 타이밍 로그를 추가했다.

## 작업 내용 
<!-- 작업 내용을 작성 -->
각 단계는 기존 logger를 통해 INFO 레벨로 기록하며, 실행시간 측정에는 시스템 시각 변경의 영향을 받지 않는 time.monotonic()을 사용한다.

로그 형식은 다음과 같이 통일했다.

    stage_started stage=fetch
    stage_completed stage=fetch elapsed_seconds=1.234


측정 대상은 다음과 같다.
- fetch: API 데이터 수집 및 fetch callback 수행
- bronze: Bronze 초기화·조회·조각 처리 및 artifact 구성
- normalize: 수집 원본 정규화
- validation: 데이터 검증 및 품질 정책 적용
- silver_write: Silver Parquet 저장
- manifest_write: 실행 결과 manifest 저장
- collector_run: 전체 collector 실행시간
Bronze 조각 저장은 기존과 동일하게 fetch callback에서 즉시 수행된다. 따라서 bronze 구간 안에서 fetch 구간이 측정되며, HTTP 응답 이후 Bronze 저장에서 blocking이 발생하면 fetch 완료 로그가 출력되지 않아 문제 구간을 좁힐 수 있다.

로그에는 row 데이터나 API 응답 내용을 포함하지 않는다. 기존 수집, 재시도, 백필, Bronze 재사용 및 오류 처리 동작은 변경하지 않았다.

### 테스트 보완
정상 수집 경로에서 다음 사항을 검증하도록 pipeline 로그 테스트를 보완했다.
- 각 주요 단계의 stage_started 로그 출력
- 각 주요 단계의 stage_completed 로그 출력
- 완료 로그에 elapsed_seconds 포함
- 전체 collector_run 실행시간 출력
- 기존 실패 및 SKIPPED 분기의 로그 계약 유지

---

## 관련 이슈 
<!-- 연관된 이슈 번호를 적습니다. (예: `#123`) -->

---


## 테스트
<!-- 어떻게 검증했는지. 재현 방법이나 테스트 케이스 -->
다음 테스트와 정적 검사를 실행했다.
- Pipeline 관련 테스트: 26 passed
- Collector 전체 테스트: 318 passed, 8 skipped
- Ruff 검사 통과
- git diff --check 통과
---

## PR 체크리스트
<!-- 아래 항목은 **리뷰어가 코드를 확인하면서 체크**하는 항목입니다. -->

PR이 다음 요구 사항을 충족하는지 확인하세요.

- [ ] 코드가 정상적으로 빌드/실행되는지 확인했습니다.
- [ ] 기존 기능에 영향을 주지 않는지 확인했습니다.
- [ ] 커밋 메시지 컨벤션에 맞게 작성했습니다. [Commit message convention](#) 참고 (Ctrl + 클릭하세요.)
- [ ] 변경 사항에 대한 테스트를 했습니다.
