# 따릉이 재배치 우선순위 대시보드

> 따릉이 대여 이력과 실시간 인구 및 날씨 데이터를 기반으로 대여소별 수요·재고를 예측하고, 재배치 우선순위와 이동 경로를 제공하는 운영 대시보드

**소프티어 부트캠프 8기 Data Engineering 2팀 최종 프로젝트**

---

## 목차

1. [프로젝트 개요](#1-프로젝트-개요)
2. [솔루션](#2-솔루션)
3. [기대효과](#3-기대효과)
4. [데이터 파이프라인](#4-데이터-파이프라인)
5. [데이터셋](#5-데이터셋)
6. [시스템 아키텍처](#6-시스템-아키텍처)
7. [기술 스택](#7-기술-스택)
8. [Airflow 워크플로](#8-airflow-워크플로)
9. [로컬 실행](#9-로컬-실행)
10. [테스트](#10-테스트)
11. [디렉터리 구조](#11-디렉터리-구조)
12. [문서](#12-문서)
13. [팀원](#13-팀원)

---

## 1. 프로젝트 개요

### 배경 및 문제점

> **어떤 대여소는 0대, 어떤 대여소는 117대—자전거는 있지만 필요한 곳에 없습니다.**

따릉이는 서울 전역 2,774개 대여소로 성장했지만, 시간대와 지역에 따른 수요 쏠림으로 품절과 과다 거치가 동시에 발생합니다. 2025년에는 자전거가 한 대도 없는 대여소가 94곳으로 확인됐습니다. [[뉴시스, 2024](https://www.newsis.com/view/NISX20241108_0002951902)] [[뉴시스, 2025](https://mobile.newsis.com/view_amp.html?ar_id=NISX20250709_0003245347)]

### 따릉이 재배치 현황

> **배송 인력 130명이 자전거 4만 5,000대의 균형을 맞추고 있습니다.**

담당자는 24시간 대여소 현황을 확인하며 자전거를 직접 싣고 옮깁니다. 그러나 제한된 인력으로는 실시간 요청에 즉시 대응하기 어렵고, 일부 대여소의 재배치는 2~3일 간격으로 이루어집니다. [[경향신문, 2017](https://v.daum.net/v/L5wOWYLZO0)] [[미디어오늘, 2020](https://www.mediatoday.co.kr/news/articleView.html?idxno=206065)] [[뉴시스, 2025](https://mobile.newsis.com/view_amp.html?ar_id=NISX20250709_0003245347)]

### 기존 방식의 한계

> **현재 재고를 보고 움직이면, 도착했을 때는 이미 수요가 바뀌어 있습니다.**

- **사후 대응**: 현재 거치율만으로는 몇 시간 뒤의 품절과 포화를 미리 알기 어렵습니다. [[경향신문, 2017](https://v.daum.net/v/L5wOWYLZO0)]
- **수동 판단**: 회수지, 공급지와 방문 순서가 담당자의 경험에 의존합니다.
- **제한된 자원**: 자전거는 늘었지만 배송·정비 인력은 충분히 늘지 못했습니다. [[한국경제, 2023](https://www.hankyung.com/article/2023053115641)]
- **데이터 오차**: 비정상 반납으로 앱의 재고와 실제 위치가 다를 수 있습니다. [[뉴시스, 2024](https://www.newsis.com/view/NISX20241108_0002951902)]

### 전략적 목표

현재 재고를 관찰하는 데 그치지 않고 대여·반납 수요와 향후 재고를 예측하여 **사후 대응 중심의 운영을 예측 기반의 선제적 재배치로 전환**합니다. 이를 통해 담당자가 제한된 차량과 인력을 더 시급한 대여소에 먼저 투입할 수 있도록 의사결정 근거와 실행 가능한 작업 경로를 제공합니다.

### 대상 사용자

서울시 공공자전거 따릉이의 재배치 운영·관리 담당자

## 2. 솔루션

우리 시스템은 실시간 재고와 도시 데이터를 수집하고, 대여·반납 수요를 예측한 뒤, 그 결과를 재배치 우선순위와 작업 경로로 변환합니다.

1. **실시간 운영 데이터 통합**: 따릉이 재고·대여이력과 날씨·생활인구·행사 데이터를 자동으로 수집하고 품질을 검증합니다.
2. **대여·반납 수요 예측**: 대여소별 대여량과 반납량을 각각 예측하고 현재 재고에 반영해 시간대별 예상 재고를 계산합니다.
3. **위험 감지와 우선순위 산정**: 현재 상태, 최근 재고 추세와 예측 결과를 결합해 품절·포화 방향, 위험까지 남은 시간과 0~100의 긴급도 점수를 산출합니다.
4. **실행 가능한 재배치 경로 생성**: 공급·회수가 필요한 수량, 차량 적재량과 이미 배차된 작업을 고려해 방문 순서와 상차·하차 수량을 제안합니다.
5. **하나의 운영 화면 제공**: 지도, 우선순위 목록, 수요·재고 예측 그래프와 작업 경로를 한 화면에서 확인하고 작업 상태를 관리합니다.
6. **지속적인 모델 운영**: 데이터 파이프라인을 5분 단위로 갱신하고, 모델 성능을 추적해 월별 평가 결과에 따라 재학습합니다.

### 제공 기능

- **실시간 현황 조회**: 지도에서 대여소 위치, 현재 자전거 수와 거치 가능 수를 확인합니다.
- **수요·재고 예측**: 대여·반납 모델의 결과를 결합해 향후 재고 변화를 제공합니다.
- **긴급도 산정**: 품절·포화 예상 시점과 부족량으로 작업 우선순위를 계산합니다.
- **재배치 경로 생성**: 상차·하차 수량과 방문 순서를 포함한 작업 경로를 생성합니다.
- **운영 상태 관리**: 작업의 배차, 완료, 취소 및 복원 상태를 관리합니다.
- **모델 수명주기 관리**: MLflow로 실험과 모델을 추적하고 월별 평가 결과에 따라 재학습합니다.

## 3. 기대효과

| 관점 | 기대효과 |
| --- | --- |
| **시민** | 품절과 반납 공간 부족 가능성을 낮춰 원하는 장소에서 자전거를 이용할 가능성을 높입니다. |
| **운영 담당자** | 경험에만 의존하던 판단을 데이터 기반 우선순위로 보완하고, 상황 파악부터 작업 결정까지 걸리는 시간을 줄입니다. |
| **재배치 작업** | 시급도, 필요 수량과 이동 경로를 함께 제공해 제한된 차량과 인력을 중요한 작업에 집중할 수 있습니다. |
| **운영 조직** | 작업 상태와 예측 근거를 일관된 데이터로 관리하여 재배치 정책을 평가하고 개선할 기반을 마련합니다. |
| **시스템 운영** | 자동 수집·검증·추론·모니터링을 통해 반복 업무를 줄이고 데이터와 모델의 최신성을 유지합니다. |

궁극적으로는 자전거의 공간적 불균형을 완화하여 따릉이의 이용 가능성을 높이고, 같은 운영 자원으로 더 효과적인 재배치를 수행하는 것을 기대합니다.

## 4. 데이터 파이프라인

### 데이터 흐름

![dataflow](./dataflow.png)

| 계층 | 역할 |
| --- | --- |
| **Bronze** | 외부 API 응답 원본과 수집 manifest를 보존합니다. |
| **Silver** | 스키마·품질 규칙을 통과한 정규화 데이터를 저장합니다. |
| **Archive** | 학습과 재처리에 사용할 일 단위 이력을 구성합니다. |
| **Gold** | 대여소, 예측, 긴급도, 작업 경로 등 서비스 조회용 데이터를 PostGIS에 게시합니다. |

## 5. 데이터셋

| 데이터셋 | 제공처 | 활용 |
| --- | --- | --- |
| [공공자전거 대여이력](https://data.seoul.go.kr/dataList/OA-15182/F/1/datasetView.do) | 서울 열린데이터광장 | 대여·반납 학습 및 실시간 이력 보강 |
| [공공자전거 실시간 대여정보](https://data.seoul.go.kr/dataList/OA-15493/A/1/datasetView.do) | 서울 열린데이터광장 | 대여소별 현재 재고 |
| [서울 실시간 인구 데이터](https://data.seoul.go.kr/dataList/OA-21778/F/1/datasetView.do) | 서울 열린데이터광장 | 실시간 생활인구 특성 |
| [자치구별 생활인구(250m)](https://data.seoul.go.kr/dataList/OA-23019/S/1/datasetView.do) | 서울 열린데이터광장 | 격자별 생활인구 기준값 |
| [기상청 API 허브](https://apihub.kma.go.kr/) | 기상청 | 초단기 실황·예보 및 단기예보 |
| [서울시 문화행사 정보](https://data.seoul.go.kr/dataList/OA-15486/S/1/datasetView.do?tab=A) | 서울 열린데이터광장 | 운영 화면의 주변 행사 정보 |
| [서울시 체육시설 공연행사 정보](https://data.seoul.go.kr/dataList/OA-15497/A/1/datasetView.do) | 서울 열린데이터광장 | 운영 화면의 공연·행사 정보 |

외부 API를 직접 수집하려면 서울 열린데이터광장의 `SEOUL_OPENAPI_KEY`와 기상청 API 허브의 `KMA_APIHUB_KEY`가 필요합니다.

## 6. 시스템 아키텍처

![따릉이 재배치 우선순위 시스템 아키텍처](./architecture.png)

Airflow가 수집, 정규화, 생활인구 보정, 추론, Gold 게시와 재배치 경로 생성을 오케스트레이션합니다. 운영 환경에서는 S3와 RDS for PostgreSQL/PostGIS를 데이터 계층으로 사용하며, 특징 생성은 일회성 EMR Classic 클러스터에서, 모델 학습은 학습용 EC2에서 수행합니다. 상시 애플리케이션 EC2는 Airflow, MLflow, FastAPI와 웹 서비스를 실행합니다.


### 기술 스택

| 영역 | 기술 |
| --- | --- |
| 오케스트레이션 | Apache Airflow 3, Docker Compose |
| 수집·가공 | Python, PyArrow, Pandas, Pydantic |
| 머신러닝 | LightGBM, Apache Spark, MLflow |
| 저장소 | Amazon S3, MinIO, PostgreSQL 16, PostGIS 3.4 |
| 백엔드 | FastAPI |
| 프론트엔드 | React, TypeScript, Vite, Leaflet |
| AWS 인프라 | EC2, EMR Classic, RDS, S3, IAM, VPC |
| IaC·개발 도구 | Terraform, uv, Ruff, Pytest, Vitest |

## 8. Airflow 워크플로

모든 스케줄은 `Asia/Seoul` 기준이며 `catchup=False`로 동작합니다.

| DAG | 스케줄 | 역할 |
| --- | --- | --- |
| `realtime_tick` | 매 5분 | 실시간 데이터 수집 → 정규화 → 추론 → Gold 게시 → 긴급도·경로 생성 |
| `daily_population_and_events` | 매일 03:00 | 생활인구 수집·보정 및 문화·공연 행사 게시 |
| `station_master` | 매일 03:04 | 대여소 기준정보 수집 및 서빙용 마스터 생성 |
| `daily_compaction` | 매일 04:30 | D-6 대여이력 재수집 및 Silver 일 단위 Archive 압축 |
| `monthly_retrain_rental` | 매월 1일 03:00 | 대여 모델 평가 및 조건부 재학습 |
| `monthly_retrain_return` | 매월 1일 06:00 | 반납 모델 평가 및 조건부 재학습 |

`realtime_tick`은 단일 DAG로 5분마다 실행됩니다. 날씨 collector별 freshness gate가 마지막 성공 수집 시각을 확인해, 날씨가 필요하지 않은 tick에는 불필요한 외부 API 호출을 생략합니다.

## 9. 로컬 실행

### 사전 요구사항

- Docker Desktop과 Docker Compose
- `make`
- Python 프로젝트를 직접 실행할 경우 [uv](https://docs.astral.sh/uv/)
- 프론트엔드를 단독 실행할 경우 Node.js 20 이상

### 전체 스택 시작

```bash
git clone <repository-url>
cd DE_team2-GangnamguUmBokDong
make bootstrap
```

최초 실행 시 `make bootstrap`은 `.env.example`을 `.env`로 복사합니다. 필수 API 키가 비어 있으면 서비스를 기동하지 않고 중단하므로, 생성된 `.env`에 `SEOUL_OPENAPI_KEY`와 `KMA_APIHUB_KEY`를 입력한 뒤 `make bootstrap`을 다시 실행하세요.

| 서비스 | 기본 주소 |
| --- | --- |
| 대시보드 | http://localhost:5173 |
| FastAPI | http://localhost:8000 |
| Airflow | http://localhost:8081 |
| MLflow | http://localhost:5000 |
| MinIO Console | http://localhost:9001 |
| PostgreSQL | `localhost:5433` |

Airflow 기본 사용자명은 `.env`의 `AIRFLOW_ADMIN_USER`이며, 최초 생성된 비밀번호는 웹서버 로그에서 확인할 수 있습니다.

```bash
make ps       # 서비스 상태
make logs     # 전체 로그
make down     # 서비스 종료
make up       # 기존 환경 재기동
```

Apple Silicon에서는 Makefile이 PostGIS용 `linux/amd64` override를 자동 적용합니다. 세부 설정과 기존 볼륨 주의사항은 [개발 환경 가이드](docs/getting-started.md)를 참고하세요.

### 개별 Python 프로젝트 설치

이 저장소는 하나의 uv workspace가 아니라 컴포넌트별 독립 프로젝트로 구성됩니다.

```bash
make sync-all

# 또는 필요한 프로젝트만 설치
cd collector
uv sync
uv run python main.py --help
```

## 10. 테스트

```bash
make lint       # 전체 Python 프로젝트 Ruff 검사
make test       # 로컬 테스트 모음
make test-ci    # CI와 동일한 테스트 모음
make e2e-smoke  # 실행 중인 로컬 스택의 실시간 E2E smoke test
```

프론트엔드만 검증하려면 `apps/web`에서 `npm ci`, `npm test`, `npm run build`를 실행합니다.

## 11. 디렉터리 구조

```text
.
├── airflow/            # DAG, 공통 설정, 태스크 빌더와 콜백
├── apps/
│   ├── api/            # Gold PostGIS 조회용 FastAPI
│   └── web/            # React 운영 대시보드
├── collector/          # 외부 API 수집, 검증, Bronze/Silver 저장
├── normalizer/         # 실시간 인구·대여소 데이터 정규화
├── nowcaster/          # 생활인구 추정과 백필
├── ml/
│   ├── feature_engine/ # Spark 기반 학습 특징 생성
│   ├── inference/      # 대여·반납 수요 추론
│   └── training/       # 모델 학습·평가·챔피언 게시
├── loader/             # Silver/예측 결과의 Gold 게시
├── rebalance/          # 긴급도 계산과 재배치 경로 생성
├── libs/               # 공통 도메인 및 ML 계약 라이브러리
├── ops/                # Compose, 배포, DB 초기화와 운영 스크립트
├── terraform/          # AWS 인프라 IaC
├── docs/               # 설계, 운영, ADR과 데이터 계약 문서
├── models/             # 로컬 개발용 초기 모델 산출물
├── Makefile            # 개발·검증·배포 명령 진입점
└── .env.example        # 로컬 환경변수 예시
```

## 12. 문서

- [개발 환경 및 로컬 실행](docs/getting-started.md)
- [Airflow 데이터 흐름](docs/airflow/explain.md)
- [Gold publication contract](docs/gold/publication-contract-v1.md)
- [Gold PostGIS ERD](docs/gold/target-erd.md)
- [MLflow 설정](docs/ml/MLFLOW_SETUP.md)
- [프론트엔드 구성](apps/web/README.md)
- [아키텍처 결정 기록](docs/adr/)

## 13. 팀원

<table>
  <tr>
    <td align="center"><a href="https://github.com/zerossin"><img src="https://github.com/zerossin.png" width="120" alt="김민수"/><br /><b>김민수</b></a></td>
    <td align="center"><a href="https://github.com/dragonjin520"><img src="https://github.com/dragonjin520.png" width="120" alt="김용진(Albert)"/><br /><b>김용진(Albert)</b></a></td>
    <td align="center"><a href="https://github.com/R2TURN0"><img src="https://github.com/R2TURN0.png" width="120" alt="문종민"/><br /><b>문종민</b></a></td>
    <td align="center"><a href="https://github.com/vysryoo"><img src="https://github.com/vysryoo.png" width="120" alt="유용선"/><br /><b>유용선</b></a></td>
  </tr>
</table>
