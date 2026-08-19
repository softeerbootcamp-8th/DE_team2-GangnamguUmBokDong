# 2025년 최초 챔피언 생성 가이드

> 이 문서의 과거 로컬 `processed_v2` 입력, 단일 multi-horizon 테이블,
> `TRAIN_START/END`, `*_SAMPLE_FRAC`, LightGBM 분산 실행 절차는 현재 구현과 맞지 않아
> 제거했다. 실행 계약의 기준 문서는
> [feature_engine README](../../ml/feature_engine/README.md)와
> [training README](../../ml/training/README.md)다.

목표 데이터 계약은 과거/월별 학습의 확정 사실 데이터는 `archive/{source}/dt=...`
에서 읽고, 온라인 5분 추론만 최신 `silver/`를 읽는 것이다. collector의 CSV/API
bootstrap도 과거 원천을 archive에 적재한다.

현재 `feature_engine.spark.silver_source`는 historical fact(트립/재고/날씨/인구)를
아직 Silver에서만 읽는다. 따라서 **archive reader와 2025 archive backfill이 통합되기
전에는 아래 명령을 production 최초 챔피언 생성 절차로 실행할 수 없다.** 이 연결은
다음 통합 작업의 필수 blocker다. 최신 `station_master_enriched`만 historical snapshot이
없는 current dimension으로 Silver에서 읽는 계약을 유지한다.

## 전제 조건

- 2025 CSV/API 원천이 source별 `archive/` partition에 모두 적재돼 있어야 한다.
- feature engine historical reader가 archive schema를 현재 feature schema로 변환해야
  한다. 트립/재고/날씨/인구 fact에 `silver/` fallback을 두면 안 된다.
- 최신 `silver/station_master_enriched`는 current station dimension 입력으로 사용할
  수 있다.
- 위 archive 경로로 작은 날짜 구간의 feature/target parity 검증이 먼저 통과해야 한다.

## 전제 충족 후 실행

각 패키지 환경을 먼저 준비한다.

```bash
cd ml
(cd feature_engine && uv sync)
(cd training && uv sync)
(cd inference && uv sync)
```

archive에 필요한 2025 원천 파티션이 적재되고 archive reader 통합이 끝난 뒤 다음
순서로 실행한다.

```bash
cd ml
export TRAIN_WINDOW_START=2025-01-01
export TRAIN_WINDOW_END=2025-12-31

./feature_engine/.venv/bin/python -m feature_engine.spark.run_pipeline
./feature_engine/.venv/bin/python -m feature_engine.spark.build_multi_horizon_features
./training/.venv/bin/python -m training.train_rental_model --promote-if-no-champion
./training/.venv/bin/python -m training.train_return_model --promote-if-no-champion

unset TRAIN_WINDOW_START TRAIN_WINDOW_END
```

두 window 변수는 반드시 쌍으로 지정한다. feature 생성과 학습이 같은 값을 받아야
하며, 종료일은 inclusive다. Spark 단계는 2026년 이후 시계열이 archive에 있어도
2025년 범위 밖 행을 제외하고, `[T,T+60분)` 라벨이 완결되지 않는 마지막 55분도
학습 테이블에서 제외해야 한다. `station_master_enriched`만 historical snapshot이
보장되지 않는 current dimension이라 최신 Silver snapshot을 사용한다.

`--promote-if-no-champion`은 해당 모델의 챔피언 포인터가 없을 때만 정상 promotion
검증을 거쳐 최초 포인터를 만든다. 대여가 성공하고 반납이 실패하면 반납 명령만 다시
실행할 수 있다. 기존 챔피언이 있으면 덮어쓰지 않고 학습 전에 실패한다.

초기 실행 뒤 두 window 변수를 반드시 해제한다. 월별 재학습 오케스트레이터도 자식
프로세스에서 이 변수를 제거해 최신 rolling window를 강제하지만, 장기 배포 환경에
초기값을 남겨두지 않는 것이 명확하다.

## 메모리 부족 시 지원되는 축소 방법

현재 실제 로더가 지원하는 첫 번째 비상 dial은 train 날짜의 결정적 축소다.

```bash
TRAIN_DAY_DIVISOR=2 ./training/.venv/bin/python -m training.train_rental_model --promote-if-no-champion
```

그래도 부족한 개발 검증에서만 `MAX_TRAIN_HORIZON=6`처럼 최대 horizon을 줄일 수
있다. 날짜 축소는 계절/요일 표본을 줄이고, horizon 축소는 먼 예측 구간의 품질을
검증하지 못하게 하므로 둘 다 전체 설정보다 품질 위험이 있다.

`TRAIN_SAMPLE_FRAC`/`VALID_SAMPLE_FRAC`/`TEST_SAMPLE_FRAC`는 구현되지 않은 과거
설정이라 사용하면 즉시 오류가 난다. multi-horizon anchor 간격을 늘리는 옵션도
학습 시각 분포와 `minute` feature의 분포를 바꾸므로 “서빙 정밀도와 무관한” 축소가
아니다. 운영 최초 모델의 기본 절차로 권장하지 않는다.

현재 lazy loader도 완전히 메모리 상수는 아니다. feature 행렬은 날짜 청크로 읽지만,
각 split의 사전 스캔에서 `label + date (+ exposure)`를 하나의 pandas DataFrame으로
읽는다. 전체 5분/12 horizon 규모에서 이 1차원 계열들이 메모리 한계를 넘으면 날짜별
prepass/메타데이터 집계로 재설계해야 한다. 이번 구현의 남은 명시적 한계다.

## 과거 자원 실측(참고 전용)

아래 값은 현재 5분 full-year 계약 이전, 2025년 11월 일부와 과거 anchor 축소 실험에서
얻은 수치다. 현행 용량 산정값이 아니라 상대적인 증가 폭을 이해하는 참고 자료다.

| 과거 anchor 밀도 | 한 달 학습 행 수 | 당시 peak RAM | 당시 대여 모델 학습 시간 |
|---|---:|---:|---:|
| 정각(60분) | 약 2,197만 | 약 3GB(비정밀) | 수 분 |
| 20분 | 약 6,592만 | 약 10.14GB | 약 60분 |
| 5분 | 약 2.6억 이상 | 18GB 로컬 머신에서 OOM | 미완료 |

실제 2025 전체 실행 전에는 대상 머신에서 작은 날짜 범위로 smoke test하고, Spark
shuffle/스토리지와 학습 peak RSS를 관찰한다. 현재 training은 분산 LightGBM worker
구성을 완성한 상태가 아니므로, 환경변수 몇 개만 켜서 분산 실행할 수 있다고 가정하면
안 된다.
