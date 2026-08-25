# 2025 전체 모델 실행·AWS 자원 보고서

> **보관 실행 보고서:** 2026-08-21 `develop@44b4bff`의 full-year 준비·축소 smoke·
> 자원 측정 결과다. 아래 AWS instance 요청안과 남은 작업은 당시 판단이며 현재 인프라
> 사양 또는 배포 상태를 나타내지 않는다. 학습 memory 등급은 후속
> [FULL_YEAR_MEMORY_PROBE_2026-08-22.md](FULL_YEAR_MEMORY_PROBE_2026-08-22.md), 현재 실행
> 절차는 [FULL_YEAR_TRAINING_GUIDE.md](FULL_YEAR_TRAINING_GUIDE.md)를 우선한다.

## 보고서 해석

| 이 보고서가 남기는 증거 | 현재 상태로 재사용할 수 없는 주장 |
| --- | --- |
| 당시 Archive·feature mart 준비와 행 수 | 현재 S3 partition의 완전성·행 수 |
| 32GiB 환경의 safety guard 중단 지점 | 현재 코드의 full-year peak와 최소 RAM |
| 축소 pair의 게시·전체 station 추론 성공 | 축소 pair가 현재 champion이라는 주장 |
| 당시 x86 local 자원과 Arm64 CI 호환성 | 현재 AWS 가격·가용 instance·실행 성능 |
| Resource manifest에 기록한 당시 phase 결과 | 현재 MLflow run 또는 serving pointer 상태 |

`d8/h1`, 월별 12일 표본, 5-round calibration 같은 축소 모델은 연결·자원 검증용이며
운영 품질 후보가 아니다. 당시 bucket·volume·manifest가 현재도 존재한다고 가정하지
않는다.

## 한눈에 보는 진행 상태

2026-08-21 기준 `develop@44b4bff`에서 실행했다. 결론은 두 가지다.

- **정식 전체 모델:** 데이터 준비는 끝났지만, 학습을 시작해 첫 번째 나무 하나를
  만들기 전에 메모리 안전선에 걸렸다.
- **AWS 연결용 임시 모델:** 2025년 각 월에서 학습일을 표본으로 뽑아 대여·반납
  모델을 만들었고, 게시와 전체 대여소 5분 추론까지 성공했다.

| 경로 | 원천 적재 | feature 생성 | 모델 학습 | pair 게시 | 5분 추론 |
|---|---:|---:|---:|---:|---:|
| 정식 전체 모델(샘플링 없음) | 완료 | 완료 | **메모리 중단** | 미실행 | 미실행 |
| AWS 연결용 임시 모델 | 재사용 | 재사용 | **완료** | **완료** | **완료** |

정식 전체 모델이 멈춘 위치는 아래와 같다.

`원천 적재 완료 → feature 생성 완료 → 5.69억 train행 로드 완료 → LightGBM 학습 진입 → 첫 round 완료 전 중단`

임시 모델은 아래 전 과정을 끝냈다.

`2025년 월별 학습일 12일 선택 → 대여·반납 학습 완료 → generation 1 게시 → 2,681개 대여소 × 12시간 추론 성공`

### 지금 확정된 결론

- 데이터·feature·fallback·serving 연결 코드는 동작한다.
- 31.34GiB RAM에서 호스트 안전 여유 3GiB를 지키며 실행한 12개월 학습은
  rental/return 모두 완주하지 못했다.
- 설정된 swap 8GiB 중 최대 4.61GiB만 사용했다. 약 3.39GiB가 남아 있었으므로
  **swap을 다 써서 중단된 것이 아니다.** WSL 가용 RAM을 3GiB 남기는 안전장치가
  먼저 학습 프로세스를 종료했다.
- 64GiB는 성공 확정치가 아니라 32GiB 다음 단계의 첫 검증 요청 사양이다.
- 임시 pair는 AWS 연결 시험에 쓸 수 있지만 운영 품질의 champion으로 확정하지
  않는다. #163과 #178은 샘플링 없는 전체 모델을 검증할 때까지 열어 둔다.

배포 연결만 검증한 `d8/h1/20 rounds` 모델은
`dt=2026-08-21-local-smoke`에 격리했다. 이 모델은 운영 품질 후보가 아니다.

### 측정 환경

- WSL RAM: 31.34GiB
- WSL swap: 8.00GiB
- 학습 중단 조건: system 가용 RAM이 3.00GiB 미만
- 측정 아키텍처: x86_64
- ARM64: 네이티브 CI에서 의존성·테스트·image build만 확인

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

## 단계별 시간·자원 사용량

- `작업 peak`: 해당 명령과 자식 프로세스가 RAM에 올린 최대량
- `WSL peak`: 다른 프로세스와 page cache까지 포함한 WSL 전체 최대 사용량
- `임시 디스크`: 작업 중 최대로 늘어난 scratch 사용량

| 단계 | 상태 | 시간 | 작업 peak | WSL peak | 임시 디스크 peak |
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
| 임시 rental 학습 | 성공 | 172.50초 | 5.61GiB | 15.05GiB | 0.63GiB |
| 임시 return 학습 | 성공 | 128.27초 | 5.81GiB | 14.69GiB | 0.21GiB |
| 임시 pair 게시 | 성공 | 7.78초 | 3.05GiB | 11.90GiB | 0.00GiB |
| 임시 pair 5분 추론 | 성공 | 12.93초 | 3.92GiB | 12.39GiB | 0.00GiB |

첫 multi-horizon write는 `partitionBy(date)`가 입력 partition마다 작은 파일을
써서 rental만 46,190개 미완성 object와 약 10GB가 생겼다. 이를 중단하고 date로
먼저 repartition하도록 수정했다. 최종 rental/return mart는 각각 데이터 파일
363개와 `_SUCCESS` 하나이며 크기는 8,957,553,175B / 8,873,073,594B다.
1월 9/10은 station source가 실제 0행이라 grid도 없으므로 363일이 정상이다.
전체 feature prefix는 19,744,507,296B다.

## 12개월 전체 학습은 어디서 중단됐나

### 실행 규모

- 날짜 샘플링 없음: `TRAIN_DAY_DIVISOR=1`
- 예측 범위 축소 없음: horizon 1~12
- 날짜: train 245일, valid 24일, test 24일
- 모델별 행 수: train 569,134,731행, valid 55,624,083행

### 중단 위치

학습은 다음 순서로 진행된다.

1. 날짜별 Parquet에서 label과 feature를 읽는다.
2. LightGBM용 train Dataset을 만든다.
3. 기본 모드는 valid Dataset도 만든다. 저메모리 모드는 이 단계를 학습 뒤로 미룬다.
4. LightGBM boosting round를 실행한다.
5. valid/test 평가와 모델 8개 artifact 저장을 완료한다.

기본 경로는 3번까지 완료했고, valid 지연 경로는 의도적으로 3번을 건너뛰었다.
두 경로 모두 **4번의 첫 round가 끝나기 전에** RAM 안전선에 도달했다. 따라서
전체 학습 시간과 최종 peak는 아직 측정하지 못했다.

### 시도별 결과

| 실행 | 개선 내용 | 종료 시점 | 시간 | process RSS peak | system used peak | system swap peak | scratch peak |
|---|---|---|---:|---:|---:|---:|---:|
| rental 1 | 기존 일괄 label prepass | train prepass 뒤 guard | 58.15초 | 23.64GiB | 28.97GiB | 1.16GiB | 0.00GiB |
| rental 2 | 날짜별 disk-backed prepass | train+valid overlap에서 guard | 203.71초 | 24.50GiB | 28.47GiB | 4.47GiB | 12.72GiB |
| rental 3 | native 복사 뒤 Python 참조 해제 | LightGBM 학습 진입에서 guard | 210.78초 | 24.26GiB | 28.44GiB | 4.61GiB | 12.72GiB |
| return | exposure 없는 동일 경로 | LightGBM 학습 진입에서 guard | 182.57초 | 25.18GiB | 29.28GiB | 2.57GiB | 4.24GiB |
| return memory-safe | col-wise, histogram 512MB, max_bin 63 | LightGBM 학습 진입에서 guard | 180.63초 | 23.75GiB | 28.38GiB | 3.39GiB | 4.24GiB |
| return deferred-valid | 위 설정 + native valid 지연 | LightGBM 학습 진입에서 guard | 167.57초 | 24.55GiB | 28.73GiB | 2.06GiB | 4.24GiB |
| return quantized | 위 설정 + quantized gradient | LightGBM 학습 진입에서 guard | 166.55초 | 25.74GiB | 29.75GiB | 2.31GiB | 4.24GiB |

### RAM과 swap에서 확인한 범위

- RAM 안전선에 가장 먼저 도달한 것이 종료 원인이다.
- swap은 최대 4.61GiB를 사용했다. 설정된 8GiB를 전부 사용하지 않았다.
- 표의 peak는 완주 peak가 아니라 중단 시점까지 관측한 **하한**이다.
- `force_col_wise`, histogram 512MiB, `max_bin=63`, quantized gradient, valid 지연을
  각각 시험했지만 전체 데이터에서는 첫 round를 완료하지 못했다.
- 따라서 “안정적으로 필요한 RAM은 32GiB보다 크다”까지만 확인됐다. 정확한 최소
  RAM과 전체 학습 시간은 큰 swap 진단 또는 더 큰 RAM에서 완주해야 알 수 있다.

## Serving-release 검증

### AWS 연결용 임시 모델

당장 AWS에 올려 end-to-end 연결을 확인할 모델은 2025년 12개월을 모두 포함하되
각 월의 학습일 1일씩, 총 12일을 결정적으로 골랐다. horizon은 1~12를 유지하고
학습 반복만 20 rounds로 제한했다.

| 항목 | 값 |
|---|---:|
| train | 28,081,827행(정식 전체 train의 약 4.93%) |
| valid | 27,544,044행 |
| test | 28,084,062행 |
| rental | 170.88초, Poisson deviance 2.1628, RMSE 2.3693 |
| return | 126.57초, Poisson deviance 2.2613, RMSE 2.4152 |

대여·반납 artifact 16개를 같은 archive prefix에 저장하고 `generation 1`로 원자
게시했다. release manifest SHA-256은
`4afa876d32bf316e70ab04b65cfd5701cdfff8b88dbac3218452afd66cd49b55`다.
새 process에서 이를 다시 읽어 2025-12-31 08:05 기준 2,681개 대여소 × 12시간,
총 32,172행을 11.92초에 추론했다. 대여소별 실패 0건, 유한값과 분위수 순서가
모두 정상이었고 `minute=7`도 계약대로 거부됐다.

이 모델은 **AWS 연결용 임시 모델**이다. 학습 표본이 정식 전체의 4.93%이고
20 rounds만 사용했으므로 운영 품질의 champion이라고 판단하지 않는다.

### 이전 연결 smoke

초기 배포 코드 검증에는 더 작은 `d8/h1/20 rounds` pair를 사용했다. 각각
4,678,194 train행, 2,339,655 valid행이며 rental은 39.50초/2.33GiB, return은
34.91초/2.26GiB에 끝났다. 이 pair는 이제 임시 모델 generation 1로 교체됐으며
재현 기록으로만 남긴다.

수동 production CLI가 exact rental/return archive, station profile, Spark station
master를 받아 canonical crosswalk를 만들고 pair release를 원자 게시했다.
당시 격리 버킷 결과는 generation 0, release manifest SHA-256
`3220965e0a16d192820aff4be0dd367ca45aecf8b89fd6563a7bef752c6c5569`다.

새 process가 `serving-release/current.json`을 한 번 읽어 고정한 artifact로
2025-12-31 08:05를 추론했다. 공통 model support 2,749개 중 최신 master 필드가
유효한 2,681개를 대상으로 12 horizon, 총 32,172행을 11.71초에 만들었다.
실패는 0건이고 모든 예측은 finite이며 `P10 <= P50 <= P90`였다. `minute=7`은
5분 serving tick 계약에 따라 거부됐다. 제외된 68개는 capacity 또는 grid_id가
결측인 station으로, 운영 serving plan도 active·유효 master·두 model support의
교집합을 써야 한다.

## 당시 AWS 요청안

### 상시 EC2

허용된 `t4g.large`는 2 vCPU/8GiB의 Arm Graviton2 burstable 인스턴스다. 2026-08-21
x86_64 로컬에서 Airflow scheduler/webserver/dag-processor, MLflow, API, Web과
로컬 RDS/S3 대역을 모두 띄운 상태로 2,681개 대여소×12 horizon 추론을 실제로
겹쳤다. 13.08초 동안 추론 process-tree peak는 3.87GiB, system used peak는
12.33GiB였고 swap은 사용하지 않았다.

실행 직전 EC2 배치 대상 컨테이너만의 working set도 약 4.31GiB였다. 여기에 추론
3.87GiB를 더하면 8.18GiB로, OS와 Docker 여유를 넣기 전부터 `t4g.large`의 8GiB를
넘는다. 로컬 Postgres/MinIO는 이 4.31GiB에서 제외했으므로 운영에서 RDS/S3를
외부화해도 결론은 바뀌지 않는다. 따라서 **Airflow·MLflow·API·Web·collector·추론을
한 대에 상시 배치하는 초기안에는 `t4g.large`를 요청하지 않는다.** `t4g.xlarge`
16GiB 한 대 또는 역할을 나눈 `t4g.large` 두 대가 필요하다. 8GiB 한 대만 허용되면
MLflow/Web을 상시 프로세스에서 제외하고 collector·compaction·inference가 겹치지
않도록 직렬화해야 하며, 이는 초기안과 다른 축소 운영안이다.

위 값은 용량 결정을 위한 x86_64 로컬 실측이다. 아키텍처 호환성은 네이티브
`ubuntu-24.04-arm` CI에서 uv lock, Airflow/collector/inference/training 테스트,
Airflow/MLflow `linux/arm64` image build까지 성공했다. 실제 t4g의 CPU credit과
Arm64 실행 시간은 배정 후 확인할 항목이지, 8GiB 초과 판정을 뒤집는 근거로 쓰지
않는다. 운영 t4g는 amd64-only 로컬 PostGIS container 대신 할당된
`db.t4g.medium` RDS에 접속한다.

AWS 공식 사양:

- <https://aws.amazon.com/ec2/instance-types/t4/>
- <https://docs.aws.amazon.com/ec2/latest/instancetypes/gp.html>

### 학습 EC2

`r6g.2xlarge`(8 vCPU/64GiB, Arm Graviton2, EBS-only)는 성공이 확정된 최소 사양이
아니라 첫 검증 요청 사양이다. 32GiB급에서 두 모델 모두 LightGBM 첫 round 전에
system used 29.75GiB, swap 4.61GiB까지 관측됐다. 3GiB RAM 안전 여유를 지키는
32GiB 환경이 부족하다는 것은 확인됐지만, swap 8GiB가 고갈된 것은 아니며 보호
종료 이후의 최종 peak는 모른다. R6g의 32GiB 다음 표준 크기가 64GiB이고 상시
t4g와 같은 Graviton2에서 학습·재로드·추론을 검증할 수 있어 먼저 요청한다.
최소 100GiB gp3 scratch를 붙이고 rental/return을 순차 실행한다. 첫 실행에서도
동일한 3GiB guard를 유지하며, 성공해야만 64GiB를 확정하고 guard가 작동하면 그
manifest로 다음 증설을 판단한다.

여기서 100GiB gp3는 feature/label 임시 파일용 scratch이며 swap 권고가 아니다.
다만 최소 RAM 범위를 먼저 좁혀야 한다면 32GiB RAM에 큰 swap을 붙인 별도 진단
실험은 의미가 있다. 완주 시 peak system memory와 peak swap을 함께 기록하면 64GiB
요청을 보강하거나 128GiB로 바로 올릴 근거가 된다. 이 합은 page cache와 inactive
page를 포함해 필요한 물리 RAM과 정확히 1:1은 아니므로 보수적으로 해석한다.
5.69억 행 LightGBM의 무작위 접근이 disk paging으로 바뀌면 수일 이상 걸리거나
WSL/Windows가 응답 불능이 될 수 있으므로 swap 완주는 운영 성공으로 인정하지 않고,
실행 시간 제한·swap 사용 상한·진행 로그를 둔 자원 진단으로만 수행한다.

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
당시 변경에 대해 다음 검증이 통과했다.

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

## 당시 64GiB 실행 판정 기준

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

## 당시 남은 작업

1. 승인된 64GiB Arm64 EC2에서 `d1/h12`, 기본 800 rounds로 rental/return을
   순차 완주하고 metrics/resource manifest를 보존한다.
2. 전체 artifact 16개를 새 process에서 재로드하고 profile/source fingerprint를
   확인한다.
3. 그 **전체 모델 페어**에 대해서만 수동 serving-release CLI를 실행하고 :00/:05
   12-horizon smoke를 반복한다. 로컬 smoke pointer를 운영으로 복사하지 않는다.
4. 배정된 `t4g.large`에서는 MLflow/Web을 제외하고 작업을 직렬화한 축소 운영안만
   확인한다. 초기 all-in-one 구성은 로컬 peak가 8GiB를 넘었으므로 16GiB 또는
   두 대 분리를 요청한다.
5. EMR 5노드에서 executor/driver peak와 실제 EBS shuffle 사용량을 다시 계측한다.
