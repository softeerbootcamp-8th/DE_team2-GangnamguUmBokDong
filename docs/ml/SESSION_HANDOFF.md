# 세션 인계 문서 — 다음 세션 시작용

아래 내용을 새 세션 첫 메시지로 붙여넣으세요.

---

## 프로젝트 배경

`ml/` 디렉터리(따릉이 대여/반납 수요 예측). **폴더 구조가 이번 세션에 재편됨** —
`ml/` 아래 정확히 5개 폴더: `common/`, `feature_engine/`, `training/`, `inference/`,
`data/`(그대로 유지, 실배포는 S3). 각 폴더의 `README.md`(실행 방법)/`DESIGN.md`
(설계 배경)를 먼저 읽을 것. 전체 개요는 [README.md](README.md).

**의사결정 히스토리는 `ml/history.md`에 시간순으로 정리돼 있음 — 반드시 먼저 읽을 것.**
특히 14~16번 항목이 최근 세션들 작업 내용.

## 최근 세션들에서 완료한 것 (전부 테스트 통과 확인됨)

1. **하이퍼파라미터 프로필 시스템** — `common/common_config.py`가 `ML_PROFILE`
   환경변수로 `common/profiles/{이름}.json`을 읽는 로더로 재작성.
2. **Spark(`feature_engine/spark/`) 5분 tick 그리드 포팅** — pandas와 대칭 맞춤,
   sparse target 조회, dtype 다운캐스트.
3. **타임존 KST(Asia/Seoul) 확정 + 근본 버그 수정** — `F.unix_timestamp()`/
   `F.timestamp_seconds()`의 timestamp_ntz/tz-aware 비대칭 문제를
   `_unix_seconds_ntz()`/`_seconds_to_ntz()`(session-tz 무관 헬퍼)로 해결.
4. **폴더 구조 재편** — `src`/`feature_engine`/`scripts` 단일 패키지를
   `common`/`feature_engine`/`training`/`inference` 5개 폴더로 분리(인스턴스별
   독립 배포 가능하게). 상세는 history.md 16번 항목.

**검증**: `.venv/bin/python -m pytest common/tests feature_engine/tests/dev_features_rental_censoring.py feature_engine/tests/dev_completion_curve_integration.py training/tests inference/tests -q`
(45개), `.venv-spark/bin/python -m pytest feature_engine/tests/test_spark_*.py -q`(12개)
전부 통과.

## 다음 세션에서 할 수 있는 것 (우선순위 없음 — 필요할 때 참고)

1. **실데이터로 Spark 파이프라인 전체 재검증**: `data/processed_v2/targets_2025.parquet`/
   `return_targets_2025.parquet`(pandas가 이미 sparse tick 포맷으로 만들어둔 실제
   2025년 데이터)이 `feature_engine/spark/config.py`의 기본 경로와 같은 곳을
   가리키므로, `feature_engine.spark.build_targets`로 다시 만들 필요 없이 바로
   `feature_engine.spark.run_pipeline`을 실데이터로 돌려볼 수 있다. 이전(시간 단위
   그리드 시절) E2E 실행 기록은 tick 그리드로 재실행하면 행 수가 12배 늘어나므로
   리소스/시간이 달라질 것 — 로컬 머신 RAM(18GB) 한계를 먼저 고려(history.md
   11번의 청크 처리 교훈 참고).
2. **재학습 + 지표 재검증**: 5분 tick 그리드로 바뀐 뒤 실제 재학습을 한 번도
   안 해봤다 — README.md의 "결과 요약" 표가 여전히 시간 단위 그리드 시절
   수치다. `training.train_rental_model`/`train_return_model`을 실데이터로
   돌려서 deviance/coverage가 tick 그리드에서도 비슷한 수준인지 확인 필요.
3. **월별 성능 모니터링의 "고정 baseline → rolling baseline" 개선**은 아직
   미착수(history.md 9번 항목 마지막 문단) — 계절이 서서히 바뀌는 건 흡수하고
   급격한 이탈만 잡도록 재설계.
4. **LightGBM 분산 학습 인프라 검증**: 코드는 준비됐지만(history.md 17번,
   [ADR-0001](adr/0001-lightgbm-distributed-training.md)) 실제 워커 머신이
   없어 다중 머신 End-to-End 검증을 못 했다 — 인프라가 서면
   `LGB_MACHINES`에 실제 `host:port`를 넣고 검증할 것.

## 주의사항 (이전 세션들에 겪은 함정들, 계속 유효함)

- 이 Mac은 RAM 18GB뿐 — 268M행 전체를 pandas DataFrame 하나로 올리면 OS가
  SIGKILL로 죽인다. 배치 처리(station 25개씩) 또는 표본 추출로 우회할 것.
- `pd.merge_asof`는 대규모 데이터에서 병목이 될 수 있음 — `np.searchsorted` 기반으로 대체.
- Spark 테스트/스크립트는 반드시 `.venv-spark`(Python 3.11)로 실행. 메인
  `.venv`(Python 3.14)로 실행하면 pyspark 직렬화가 깨져서 이상한 에러가 남.
- **타임존은 KST(Asia/Seoul)로 통일** — Spark 세션을 새로 만드는 코드/테스트는
  `TZ=Asia/Seoul` env(SparkSession 생성 **전**)와
  `spark.sql.session.timeZone=Asia/Seoul`을 반드시 같이 설정할 것. 초 단위
  정수 ↔ 타임스탬프 왕복은 `feature_engine/spark/rolling_window_features.py`의
  `_unix_seconds_ntz()`/`_seconds_to_ntz()`를 쓸 것(직접 `F.unix_timestamp()`/
  `F.timestamp_seconds()` 쓰면 세션 타임존에 따라 조용히 틀어짐).
- **공유 모듈을 쪼갤 때 monkeypatch 대상 확인**: `from x import NAME`(bound-name)
  대신 `from . import x` + `x.NAME`(attribute 접근)을 써야 다른 모듈이
  `x.NAME = ...`으로 override했을 때 실제로 반영된다 — `common/trip_events.py`가
  이 함정에 걸렸었다(history.md 16번 항목 참고).
- **폴더 구조 재편 후 새 코드를 추가할 때**: 어느 폴더에 넣을지 애매하면
  "이 로직을 feature_engine/training/inference 중 2개 이상이 정확히 같은 값으로
  써야 하는가?"를 먼저 물어볼 것 — 그렇다면 `common/`, 아니면 해당 폴더에만.
