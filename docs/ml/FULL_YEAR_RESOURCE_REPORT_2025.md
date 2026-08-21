# 2025 전체 모델 실행·AWS 자원 보고서

## 결론

2026-08-21 기준 `develop`의 `44b4bff42a4db93cb176f704a18f450fade92469`에서
2025 Archive 적재, `g20/r20/a20` feature/target, fallback profile, 모델 학습,
pair serving release, 5분 추론 순서로 격리 실행했다.

- 2025 전체 feature와 profile은 완성됐다.
- 샘플링 없는 전체 학습은 train 569,134,731행, valid 55,624,083행을 native
  LightGBM Dataset으로 만든 뒤 학습 작업 메모리가 붙는 시점에 31.34GiB WSL의
  안전 한계를 넘었다. rental/return 모두 같은 지점에서 중단됐다.
- 그러므로 #163의 운영 후보 모델은 아직 생성되지 않았으며 #163 완료로 표시하면
  안 된다. #178의 production CLI와 pair-atomic 연결 경로는 구현·검증됐지만,
  실제 #163 산출물 게시라는 최종 조건은 64GiB 학습 머신에서 다시 실행해야 한다.
- 로컬 검증용 `d8/h1/20 rounds` 페어는 별도
  `dt=2026-08-21-local-smoke` archive에 만들었다. 이 페어는 연결 검증 전용이며
  champion 또는 운영 성능 후보가 아니다.

실행 중인 WSL 설정은 RAM 33,654,714,368 bytes(31.34GiB), swap 8GiB였다.
36GB로 재시작하지 않았다. 4GB를 더 주는 것보다 Windows 호스트를 보호하면서
32GB급 실패 증거를 남기는 편이 안전하고, 아래 결과상 36GB도 운영 여유가 없다.

## 원천 데이터 완전성

| 원천 | 확인 범위 | 결과 | S3 크기 |
|---|---|---:|---:|
| 대여 이력 | 2024-11-27~2026-01-07 context 포함 | 407/407일 | 1,223,104,698B |
| 대여소 현황 | 2025-01-01~12-31 | 365/365일 | 168,676,723B |
| 날씨 | 2024-12-31~2025-12-31 | 366/366일 | 1,796,341B |
| 생활인구 | 2025-01-01~12-31 | 365/365일 | 6,888,255,082B |

대여소 현황의 2025-01-09/10과 날씨의 2024-12-31은 원본에 실제 행이 없어
스키마가 맞는 0행 partition으로 물질화했다. 값을 발명하거나 최신 Silver로
대체하지 않았다. 최신 station master는 2,752행이며 station mapping은
2,682행(97.456%)이다.

## 단계별 실측

모든 RSS는 wrapper가 자식 JVM을 포함한 process tree를 주기적으로 합산한 값이다.
system peak에는 WSL의 다른 프로세스와 page cache도 포함된다.

| 단계 | 상태 | 시간 | process-tree peak RSS | system used peak | scratch peak |
|---|---|---:|---:|---:|---:|
| ZIP stage(393개) | 성공 | 82.33초 | 0.03GiB | 2.96GiB | 30.09GiB |
| rental bootstrap | 성공 | 775.11초 | 2.85GiB | 9.85GiB | 0.00GiB |
| station bootstrap | 성공 | 473.96초 | 0.86GiB | 8.22GiB | 0.00GiB |
| population backfill | 성공 | 713.08초 | 0.64GiB | 7.37GiB | 0.00GiB |
| base feature pipeline | 성공 | 833.76초 | 9.63GiB | 15.88GiB | 8.75GiB |
| multi-horizon feature | 성공 | 1,778.78초 | 9.09GiB | 14.80GiB | 86.08GiB |
| station fallback profile | 성공 | 22.28초 | 8.91GiB | 14.25GiB | 0.01GiB |
| population fallback profile | 성공 | 10.35초 | 8.25GiB | 14.75GiB | 0.00GiB |
| release 게시(smoke pair) | 성공 | 7.89초 | 3.06GiB | 7.88GiB | 0.00GiB |
| 5분 배치 추론 | 성공 | 12.70초 | 4.10GiB | 9.29GiB | 0.00GiB |

첫 multi-horizon write는 `partitionBy(date)`가 입력 partition마다 작은 파일을
써서 rental만 46,190개 미완성 object와 약 10GB가 생겼다. 이를 중단하고 date로
먼저 repartition하도록 수정했다. 최종 rental/return mart는 각각 데이터 파일
363개와 `_SUCCESS` 하나이며 크기는 8,957,553,175B / 8,873,073,594B다.
1월 9/10은 station source가 실제 0행이라 grid도 없으므로 363일이 정상이다.
전체 feature prefix는 19,744,507,296B다.

## 전체 학습 메모리 한계

전체 계약은 `TRAIN_DAY_DIVISOR=1`, horizon 1..12, train/valid/test 날짜 수
245/24/24다. rental/return 모두 train 569,134,731행, valid 55,624,083행이다.

| 실행 | 개선 내용 | 종료 시점 | 시간 | process RSS peak | system used peak | system swap peak | scratch peak |
|---|---|---|---:|---:|---:|---:|---:|
| rental 1 | 기존 일괄 label prepass | train prepass 뒤 guard | 58.15초 | 23.64GiB | 28.97GiB | 1.16GiB | 0.00GiB |
| rental 2 | 날짜별 disk-backed prepass | train+valid overlap에서 guard | 203.71초 | 24.50GiB | 28.47GiB | 4.47GiB | 12.72GiB |
| rental 3 | native 복사 뒤 Python 참조 해제 | LightGBM 학습 진입에서 guard | 210.78초 | 24.26GiB | 28.44GiB | 4.61GiB | 12.72GiB |
| return | exposure 없는 동일 경로 | LightGBM 학습 진입에서 guard | 182.57초 | 25.18GiB | 29.28GiB | 2.57GiB | 4.24GiB |
| return memory-safe | col-wise, histogram 512MB, max_bin 63 | LightGBM 학습 진입에서 guard | 180.63초 | 23.75GiB | 28.38GiB | 3.39GiB | 4.24GiB |
| return deferred-valid | 위 설정 + native valid 지연 | LightGBM 학습 진입에서 guard | 167.57초 | 24.55GiB | 28.73GiB | 2.06GiB | 4.24GiB |
| return quantized | 위 설정 + quantized gradient | LightGBM 학습 진입에서 guard | 166.55초 | 25.74GiB | 29.75GiB | 2.31GiB | 4.24GiB |

가용 system memory 3GiB 하한에서 해당 process group만 SIGTERM으로 종료했다.
즉 위 수치는 실제 최종 peak가 아니라 **32GB급에서 관측 가능한 하한**이다.
LightGBM round가 진행되기 전에 종료됐기 때문에 전체 학습 시간도 이 머신에서
외삽해 완료 시간으로 단정할 수 없다. 첫 64GiB 실행은 rental/return을 반드시
순차로 돌리고 같은 probe를 유지해야 한다.

LightGBM 공식 OOM 권고인 `histogram_pool_size`, `force_col_wise`, `max_bin`과
4.x의 quantized gradient까지 별도 프로필로 비교했으며, train 전체를 유지하고
native valid만 학습 뒤로 미루는 경로도 시험했다. 모두 첫 boosting round 직후
보호선을 넘었으므로 36GB WSL 재시작을 추가로 시도할 근거보다 64GiB EC2 요청
근거가 강하다. deferred-valid 경로는 valid 날짜/행을 버리지 않고 학습 뒤 전체
streaming 평가·conformal에 사용하지만 early stopping 대신 고정 round를 쓴다.

## Serving-release 검증

로컬 검증 페어는 2025년 12개월을 포함하되 `d8/h1/20 rounds`로 줄여 각각
4,678,194 train행, 2,339,655 valid행을 사용했다. rental은 39.50초/2.33GiB,
return은 34.91초/2.26GiB에 끝났고 16개 model artifact의 총 크기는
2,532,577B다.

수동 production CLI가 exact rental/return archive, station profile, Spark station
master를 받아 canonical crosswalk를 만들고 pair release를 원자 게시했다.
격리 버킷 결과는 generation 0, release manifest SHA-256
`3220965e0a16d192820aff4be0dd367ca45aecf8b89fd6563a7bef752c6c5569`다.

새 process가 `serving-release/current.json`을 한 번 읽어 고정한 artifact로
2025-12-31 08:05를 추론했다. 공통 model support 2,749개 중 최신 master 필드가
유효한 2,681개를 대상으로 12 horizon, 총 32,172행을 11.71초에 만들었다.
실패는 0건이고 모든 예측은 finite이며 `P10 <= P50 <= P90`였다. `minute=7`은
5분 serving tick 계약에 따라 거부됐다. 제외된 68개는 capacity 또는 grid_id가
결측인 station으로, 운영 serving plan도 active·유효 master·두 model support의
교집합을 써야 한다.

## AWS 요청안

### 상시 EC2

허용된 `t4g.large`는 2 vCPU/8GiB의 Arm Graviton2 burstable 인스턴스다. 추론
단독 peak 4.10GiB와 rental bootstrap peak 2.85GiB를 단순 합산해도 6.95GiB라
OS, Airflow scheduler/worker, Docker, collector 동시 실행 여유가 거의 없다.
따라서 다음 중 하나가 필요하다.

1. 우선 `t4g.large` 한 대에서 collector와 inference가 겹치지 않도록 제한하고,
   backfill/compaction 포함 동시 soak test를 수행한다.
2. 동시 실행이 필수라면 `t4g.xlarge` 16GiB 또는 상시 EC2 두 대를 요청한다.

로컬 실측은 x86_64였고 t4g는 arm64이므로 PR CI에 네이티브
`ubuntu-24.04-arm` gate를 추가했다. Airflow/collector/inference/training을 포함한
uv lock 해석과 model runtime 테스트, Airflow/MLflow image의 `linux/arm64` build를
통과해야 배포할 수 있다. 다만 CI는 t4g의 8GiB/CPU credit 조건을 재현하지 않으므로
실제 인스턴스 soak는 별도로 남는다. T4g의 CPU credit 특성상 장시간 compaction을
상시 서버에서 돌리는 것도 분리하는 편이 안전하다.

운영 t4g에서는 amd64-only인 로컬 `postgis/postgis:16-3.5` 컨테이너를 실행하지
않고 할당된 `db.t4g.medium` RDS에 접속한다. ECR에는 CI가 검증한
`linux/arm64` Airflow/MLflow 이미지를 게시하며, 로컬 Compose 전체를 그대로 EC2에
복제하는 방식은 지원하지 않는다.

AWS 공식 사양:

- <https://aws.amazon.com/ec2/instance-types/t4/>
- <https://docs.aws.amazon.com/ec2/latest/instancetypes/gp.html>

### 학습 EC2

`r6g.2xlarge`(8 vCPU/64GiB, Arm Graviton2, EBS-only) 1대를 필요할 때만 켜는 안을
요청한다. 상시 `t4g.large`와 같은 Graviton2 아키텍처에서 학습·재로드·추론까지
검증해 x86 전용 경로를 만들지 않기 위함이다. 메모리 증설 근거는 32GB급에서 두
모델 모두 LightGBM round 시작 전에 system
29.28GiB와 swap을 소진했고, 관측 peak가 최종 peak의 하한이라는 점이다.
최소 100GiB gp3 scratch를 붙이고 rental/return을 순차 실행한다. 첫 실행에서도
동일한 3GiB guard를 유지하며 64GiB를 넘으면 그 manifest로 다음 증설을 판단한다.

`r6g.2xlarge`가 대상 리전에 없거나 승인 정책상 최신 세대만 가능하면 같은 Arm64
계약의 `r7g.2xlarge`(8 vCPU/64GiB)를 대안으로 요청한다. x86 `r6i.2xlarge`는
Graviton 계열을 사용할 수 없다는 별도 사유가 확인될 때만 차선이다.

AWS 공식 사양:

- <https://aws.amazon.com/ec2/instance-types/r6g/>
- <https://aws.amazon.com/ec2/instance-types/r7g/>
- <https://docs.aws.amazon.com/ec2/latest/instancetypes/mo.html>

현재 LightGBM 설정은 CPU 학습이며 허용된 AWS 타입에도 GPU가 없다. 로컬 GPU
학습을 별도 구현하면 AWS 운영 재현성과 다른 경로가 되므로 이번 자원안의 해결책이
아니다.

### Feature engineering용 EMR

`m4.large`는 노드당 2 vCPU/8GiB이고 EBS-only다. 로컬 Spark peak 9.63GiB가
단일 노드 8GiB보다 크지만 분산 Spark peak와 직접 1:1 비교할 수는 없으므로,
1 primary + 4 core의 5노드 안을 먼저 검증할 수 있다. 단, JVM/daemon overhead를
뺀 executor memory로 설정하고 단계별 동시 task 수를 제한해야 한다.

multi-horizon의 로컬 scratch peak가 86.08GiB였고 EMR `*.large` 기본 EBS는
노드당 32GiB이므로 core 노드에는 별도 gp3를 붙여 총 scratch 300GiB 이상을
확보하는 안을 권한다. 날짜당 파일 하나로 줄인 변경 덕분에 최종 출력 자체는 약
17.8GB지만 shuffle 중간 공간이 더 크다.

AWS 공식 사양:

- <https://aws.amazon.com/ec2/instance-types/general-purpose/#M4>
- <https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-storage.html>

## 구현·회귀 검증

원격 `develop`을 다시 fetch한 결과 작업 기준과 `origin/develop`은 모두
`44b4bff42a4db93cb176f704a18f450fade92469`이며 ahead/behind는 `0/0`이었다.
현재 변경에 대해 다음 검증이 통과했다.

| 범위 | 결과 |
|---|---:|
| collector bootstrap | 765 passed, 9 skipped |
| Spark multi-horizon | 10 passed |
| training + `ml_core` | 239 passed |
| realtime inference | 127 passed, 1 skipped |
| resource probe | 4 passed |
| Python compile + `git diff --check` | 성공 |

중단된 full-year MLflow run은 resource manifest와 일치하는 7건을 `KILLED`로
정리했고 성공한 축소 smoke 2건은 `FINISHED`로 보존했다. 작업 전용
`issue163-full-year` MinIO/Postgres/MLflow 컨테이너만 중지했으며 볼륨과 원천,
feature, profile, 모델 smoke archive, resource manifest는 삭제하지 않았다.

## 64GiB 실행 판정 기준

첫 Arm64 Graviton 64GiB 실행은 아래 조건을 동시에 만족해야 완료로 인정한다.

1. `TRAIN_WINDOW_START/END=2025-01-01/2025-12-31`, `TRAIN_DAY_DIVISOR=1`,
   `MAX_TRAIN_HORIZON=12`, g20/r20/a20이며 rental/return을 순차 실행한다.
2. 각 모델의 Poisson, Q10/Q50/Q90, station categories, conformal correction,
   metrics, effective profile 총 8개 artifact가 같은 immutable archive prefix에 있다.
3. 새 process 재로드에서 두 profile과 source fingerprint가 일치하고 model support가
   일치한다.
4. pair publication 성공 출력의 generation, manifest URI/SHA, station crosswalk
   source fingerprint를 보존한다.
5. 새 release를 고정해 `:00`과 `:05`에서 전체 station×12 horizon 추론이 성공하고,
   finite·quantile order·fallback 실패 0건을 확인하며 `minute=7`은 거부한다.
6. 두 학습 manifest 모두 `status=succeeded`이고 wall time, process-tree peak RSS,
   system memory/swap peak, scratch peak가 기록돼야 한다.

64GiB에서도 3GiB memory guard가 발동하면 그 결과를 실패로 숨기지 않고 동일
manifest를 다음 인스턴스 증설 근거로 쓴다. 로컬에서 가장 낮은 RSS를 보인
memory-safe 설정은 `force_col_wise=true`, `histogram_pool_size=512`, `max_bin=63`이며,
grid/window/horizon/날짜를 줄이지 않는다. `LGB_DEFER_VALID_DATASET=true`는 native
valid 상주가 다시 병목일 때만 별도 프로필로 사용하고, 이 경우 early stopping 없이
요청한 800 rounds를 고정 실행했다는 사실을 metrics와 manifest에 남긴다.

## 남은 작업

1. 승인된 64GiB Arm64 EC2에서 `d1/h12`, 기본 800 rounds로 rental/return을
   순차 완주하고 metrics/resource manifest를 보존한다.
2. 전체 artifact 16개를 새 process에서 재로드하고 profile/source fingerprint를
   확인한다.
3. 그 **전체 모델 페어**에 대해서만 수동 serving-release CLI를 실행하고 :00/:05
   12-horizon smoke를 반복한다. 로컬 smoke pointer를 운영으로 복사하지 않는다.
4. 상시 `t4g.large`에서 backfill·compaction·collector·Airflow·inference 동시
   soak를 수행해 8GiB 유지 또는 16GiB 요청을 최종 결정한다.
5. EMR 5노드에서 executor/driver peak와 실제 EBS shuffle 사용량을 다시 계측한다.
