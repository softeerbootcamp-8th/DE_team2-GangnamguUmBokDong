# GitHub Actions CI와 배포 방식

> 상태: CI 구현 완료, CD 방식 미결정<br>
> 코드 확인일: 2026-08-24

이 문서는 현재 구현된 GitHub Actions CI와 운영 배포 경로를 구분하고, 자동 배포를 추가할 때 선택해야 할 방식을 정리한다. 특정 EC2 ID나 IP처럼 바뀌는 값은 문서에 고정하지 않고 Terraform output을 사용한다.

## 현재 구현 상태

```text
pull request / push (develop, main)
        ↓
GitHub-hosted runner
        ├─ Unit Tests
        ├─ Integration Tests
        ├─ Arm64 Compatibility
        └─ Quality: 앞의 세 job 성공 확인

main 배포
        ↓
자동 workflow 없음 → 운영자가 app EC2에서 Makefile deploy-* 실행
```

`.github/workflows/`에는 `ci.yml`만 있다. `deploy.yml`이나 GitHub Actions에서 EC2·EMR·RDS를 변경하는 job은 현재 구현되어 있지 않다.

## 구현된 CI

### 실행 조건

- `develop`, `main` 대상 pull request
- `develop`, `main` branch push
- 같은 workflow와 ref의 이전 실행은 새 실행이 시작되면 취소
- repository permission은 `contents: read`

### Job 계약

| Job | Runner | 실제 작업 |
| --- | --- | --- |
| `unit-tests` | `ubuntu-latest` | uv 설치 → `make sync-ci-unit` → `make test-ci-unit` |
| `integration-tests` | `ubuntu-latest` | `.env.example` 복사 → `make up` → `make test-ci-integration` → 항상 `make down` |
| `arm64-compatibility` | `ubuntu-24.04-arm` | 주요 uv project sync와 테스트, Airflow·MLflow ARM64 image build |
| `quality` | `ubuntu-latest` | 앞의 세 job 결과가 모두 `success`인지 확인 |

`quality`는 별도 lint를 실행하지 않는다. UI 표시 이름은 `Lint and Test`지만 실제 역할은 필수 job 결과를 하나의 branch protection check로 모으는 것이다.

통합 테스트가 Docker root 소유 파일을 만들 수 있어 테스트 전에 workspace 소유권을 runner 사용자로 되돌린다. 인프라는 실패 여부와 관계없이 `if: always()` 단계에서 종료한다.

## 현재 수동 배포 경로

운영 Compose는 app EC2에서 실행되도록 설계되어 있다.

| 명령 | 역할 |
| --- | --- |
| `make deploy-env` | S3 설정을 받아 `/opt/app/.env` 생성 |
| `make deploy-db-bootstrap` | RDS의 DB 3개 생성과 Gold/PostGIS baseline 적용 |
| `make deploy-db-check` | DB 계약 확인 |
| `make deploy-resync` | 서비스별 uv lock 기준 환경 재동기화 |
| `make deploy-up` | 운영 Compose build 및 기동 |
| `make deploy-restart` | 실행 중 서비스 재시작 |
| `make deploy-smoke` | 배포 후 API·pipeline smoke 검증 |

운영 frontend도 S3 정적 호스팅이 아니다. `Dockerfile.web`에서 build한 정적 bundle을 app EC2의 nginx 컨테이너가 제공한다. RDS는 VPC 내부에서 접근하며, 파이프라인 객체와 설정은 S3를 사용한다.

현재 계정에서는 SSM Session Manager와 SendCommand가 조직 정책으로 거부되어 실제 관리 경로는 SSH다. app EC2의 22번 포트는 `admin_cidrs`만 허용하며 `make allow-my-ip`가 현재 접속 IP를 Terraform 변수에 반영한다. train EC2는 app EC2를 bastion으로 사용하는 `make ssh-train` 경로를 사용한다.

Terraform에는 EC2 instance profile의 `AmazonSSMManagedInstanceCore` 등 SSM을 전제로 한 일부 구성이 남아 있지만, 이는 현재 SSM 명령이 동작한다는 뜻이 아니다.

## 코드 기준 위치

- CI: `.github/workflows/ci.yml`
- CI·배포 명령: `Makefile`
- 운영 Compose: `ops/compose/docker-compose.prod.yml`
- 환경파일 생성: `ops/deploy/render_env.sh`
- SSH 보안 그룹 갱신: `ops/deploy/merge_admin_cidrs.py`
- EC2·IAM·네트워크: `terraform/compute_*.tf`, `terraform/iam.tf`, `terraform/network.tf`
