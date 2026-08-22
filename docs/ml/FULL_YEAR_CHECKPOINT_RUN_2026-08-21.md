# 2025 전체 모델 checkpoint 학습 실행 결과 (2026-08-21)

## 결론

최신 `develop`의 adaptive anchor/horizon 최적화를 적용하면 2025년 rental train은
323,200,260행까지 줄지만, 32GiB WSL2에서 LightGBM 첫 boosting round를 완료하기
전에 시스템 가용 메모리 3GiB 안전선에 도달했다. 공유 Docker와 다른 병렬 작업을
보호하기 위해 rental을 실패 처리했고 return은 시작하지 않았다. 운영 후보 모델
artifact는 산출되지 않았고 serving release pointer도 변경하지 않았다.

실행 코드는 `develop` commit `8bd7e3a`에서 분리한
`feature/ml/training-checkpoint-resume` worktree에서 작성했다. 대상 데이터는 기존
Docker MinIO volume의 `issue163-full-year` bucket을 그대로 사용했다.

## 고정 데이터·모델 계약

- window: `2025-01-01..2025-12-31`
- train/valid/test: `323,200,260 / 24,816,397 / 23,424,950`행
- train days: 317일 (`TRAIN_DAY_DIVISOR=1`)
- horizons: `1,2,3,4,5,6,9,12`, `MAX_TRAIN_HORIZON=12`
- adaptive anchors: enabled, peak tick 20분
- profile: `full-year-memory-safe`
- rounds: 800, checkpoint interval 25
- archive ID: `2026-08-21-full-year-checkpoint-v1`
- system memory guard: available memory 3GiB

## 실행 증거

| 시도 | 변경 | 종료 지점 | process-tree RSS peak | guard 시 가용 메모리 | 결과 manifest |
|---|---|---|---:|---:|---|
| v1 | 8 threads, cache 512MiB, native valid 상주 | train+valid 구성 뒤 boosting 진입 | 21.99GiB | 2.56GiB | `data/issue163-full-year/resource/train-rental-full-year-checkpoint-v1.json` |
| v2 | native valid 지연 | train 구성 뒤 boosting 진입 | 23.23GiB | 2.64GiB | `data/issue163-full-year/resource/train-rental-full-year-checkpoint-v1-defer-valid-v2.json` |
| v3 | native valid 지연, 4 threads, cache 128MiB | train 구성 뒤 boosting 진입 | 23.96GiB | 2.56GiB | `data/issue163-full-year/resource/train-rental-full-year-checkpoint-v1-low-cache-v3.json` |

각 manifest의 `status`는 `resource_limit`, wrapper 종료 코드는 75이고 대상 process는
SIGTERM으로 정리됐다. v3에서는 process-tree swap 0.34GiB, system swap peak 4.90GiB도
관측됐다. 원본 로그와 진행 로그는 같은 basename으로
`data/issue163-full-year/logs/`에 보존했다.

## checkpoint와 부분 산출물

구현된 checkpoint는 Poisson/Q10/Q50/Q90 phase마다 Booster를 먼저 업로드한 뒤
`state.json`을 원자적으로 갱신한다. 동일한 데이터·profile·파라미터·코드
fingerprint일 때 마지막 정상 round부터 재개하며 early-stopping 상태도 이어받는다.
실제 LightGBM 강제 중단/재개 테스트와 MinIO smoke에서 동작을 검증했다.

이번 전체 실행은 세 번 모두 첫 round가 끝나기 전 종료됐으므로 round checkpoint가
생기지 않았다. archive에는 선행 단계가 쓴 `rental_station_categories.json`만 있고,
Booster·metrics·conformal·profile 또는 checkpoint state는 없다. Dataset 구성 단계
자체는 재개 대상이 아니므로 다음 머신에서는 데이터를 다시 구성해야 한다.

## 다음 실행 조건

데이터 일수나 horizon을 줄이면 1년 운영 모델 계약을 바꾸므로 이번 목표의 성공으로
간주하지 않는다. 동일 계약을 완주하려면 최소 64GiB RAM 머신에서 rental/return을
순차 실행하거나, 현재 미구현인 분산/out-of-core boosting 경로가 필요하다. 64GiB에서도
동일 3GiB guard, resource manifest, 고정 archive ID와 checkpoint를 유지해야 한다.

후속 swap/cgroup 실측에서 동일 데이터 계약의 학습 logical footprint는 rental
34.843GiB, return 34.144GiB, 현재 Docker stack을 포함한 WSL 전체는 최대
48.806GiB로 확인됐다. 상세 방법과 용량 판정은
`docs/ml/FULL_YEAR_MEMORY_PROBE_2026-08-22.md`를 참고한다.
