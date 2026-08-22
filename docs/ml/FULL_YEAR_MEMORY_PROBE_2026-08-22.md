# 2025 전체 모델 메모리 실측 (2026-08-22)

## 결론

2025 전체 운영 데이터 계약을 유지한 LightGBM 학습의 최대 동시 logical footprint는
rental **34.843GiB**, return **34.144GiB**였다. 현재 Docker stack을 포함한 WSL
전체 동시 RAM+swap 사용량은 최대 **48.806GiB**였다. 따라서 학습 전용 머신은
40GiB가 실측상 최소 실용선이고, 현재 서비스를 함께 실행하는 구성은 48GiB가
안전 여유 없는 관측 하한이다. 운영 권장 사양은 **64GiB RAM**이다.

## 측정 계약

- 기준 코드: `develop` commit `8bd7e3a` + checkpoint/resource probe worktree
- bucket: 기존 MinIO volume의 `issue163-full-year`
- window: `2025-01-01..2025-12-31`
- train/valid/test: `323,200,260 / 24,816,397 / 23,424,950`행
- train days: 317일, divisor 1
- horizons: `1,2,3,4,5,6,9,12`
- adaptive anchors: enabled, peak tick 20분
- profile: `full-year-memory-safe`
- LightGBM: 8 threads, histogram pool 512MiB, native valid Dataset 상주
- Docker cgroup: RAM hard limit 21GiB, reservation 18GiB, swap limit 40GiB
- host swap: 기존 WSL 8GiB + 임시 swapfile 40GiB
- calibration rounds: objective별 5 rounds, checkpoint interval 1

`resource-profile-v2`는 0.5초마다 process-tree RSS/PSS/SwapPSS, cgroup
`memory.current`/`memory.swap.current`, system RAM+swap과 memory PSI를 같은
snapshot에서 기록한다. cgroup OOM event가 없고 네 objective와 valid/test 평가가
모두 끝난 실행만 결과로 사용했다.

## 결과

| 모델 | 결과 | cgroup RAM+swap peak | process PSS+SwapPSS peak | WSL 전체 RAM+swap peak | 시간 |
|---|---|---:|---:|---:|---:|
| rental | succeeded | 34.843GiB | 30.239GiB | 48.806GiB | 15분 38초 |
| return | succeeded | 34.144GiB | 28.897GiB | 48.137GiB | 11분 13초 |

rental peak snapshot은 RAM 21.000GiB와 swap 13.843GiB가 동시에 사용된
`2026-08-21T17:18:24Z`이다. return은 RAM 21GiB와 swap 13.144GiB를 사용했다.
두 실행 모두 `oom=0`, `oom_kill=0`, `oom_group_kill=0`이다. `memory.events.max`는
cgroup RAM limit에서 swap reclaim이 일어날 때 증가한 것이며 실패가 아니다.

rental의 memory PSI avg10 peak는 some 16.60%, full 15.69%였고 누적 full stall은
24.80초였다. return은 some 13.88%, full 12.49%, 누적 full stall 17.45초였다.
이는 swap으로 완주할 수 있지만 물리 RAM보다 느려진다는 직접 증거다.

증거 파일:

- `data/issue163-full-year/resource/memory-probe-rental-r5-retry2.json`
- `data/issue163-full-year/resource/memory-probe-return-r5.json`
- `data/issue163-full-year/logs/memory-probe-rental-r5-retry2.log`
- `data/issue163-full-year/logs/memory-probe-return-r5.log`

첫 rental 시도는 격리 컨테이너의 MLflow Host header 403으로 Dataset 구성 전에
종료됐고 `memory-probe-rental-r5.json`에 별도 보존했다. 메모리 결과에는 포함하지
않는다.

## 5 rounds로 800 rounds 메모리를 판정할 수 있는 이유

행 수에 비례하는 native Dataset, label/init score, gradient/hessian과 histogram
작업 버퍼는 첫 boosting round에서 할당되고 이후 round가 같은 buffer를 재사용한다.
실제로 rental/return 모두 첫 objective의 첫 round에서 최대치를 기록했고, 이어진
Q10/Q50/Q90와 전체 평가가 이를 넘지 않았다.

5-round 최종 Booster는 모델당 약 85~109KiB였다. tree text 증가량을 800 rounds로
선형 외삽해도 모델당 약 15~18MiB이며, 한 phase 학습 중 활성 Booster 하나의 증가분은
34.8GiB peak의 0.06% 미만이다. 따라서 800 rounds가 필요한 RAM 등급을 35GiB에서
48/64GiB 등급으로 바꾸지 않는다.

단, 이 결과는 800 rounds 전체 wall time이나 byte 단위 절대 최대값을 측정한 것은
아니다. byte 단위 최대를 주장하려면 800-round rental/return을 전부 완주해야 하지만,
swap calibration 속도를 단순 비례하면 수십 시간이 필요하다. 머신 사양 결정에는
이번 동시 footprint 실측을 사용한다.

## 용량 판정

- 학습 process만 격리 실행: 관측 peak 34.843GiB. 최소 36GiB 이상이 필요하고,
  allocator·kernel·변동 여유를 포함한 실용 하한은 40GiB다.
- 현재 Docker stack과 병행: WSL 전체 관측 peak 48.806GiB. 48GiB는 여유가 없어
  권장하지 않는다.
- 운영 권장: 64GiB RAM. 관측 전체 peak 대비 약 31% 여유가 있어 Docker, page cache,
  allocator 변동과 checkpoint 업로드를 수용한다.
- 현재 32GiB를 계속 사용: 학습 cgroup swap peak가 13.843GiB이고 기존 WSL swap도
  사용되므로 총 swap 24GiB 이상이 필요하다. 완주는 가능하지만 PSI stall과 SSD I/O로
  학습 시간이 크게 늘어난다.

calibration archive는
`models/archive/dt=2026-08-22-memory-probe-r5-v1/full-year-memory-safe/`이며
5-round 모델이므로 운영 모델이 아니다. serving release pointer는 변경하지 않았다.
