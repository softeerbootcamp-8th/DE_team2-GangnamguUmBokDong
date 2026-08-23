# GitHub Actions CD 연동 — 의사결정 자료

작성일: 2026-08-23. 결정 전 자료라 아직 아무 파일도 만들거나 고치지 않았다(이 문서만 추가).
현재 `terraform/`은 다른 작업자가 다른 브랜치에서 작업 중이라, terraform 리소스 변경이
필요한 부분은 코드로 바로 넣지 않고 이 문서에 스니펫으로만 남긴다.

## 1. 현재 상태 요약

### 인프라 (terraform state 기준)

| 역할 | 리소스 | 비고 |
|---|---|---|
| 상시 앱(Airflow·collector 등) | EC2 `i-077c84802978f077b` (t4g.large, ARM) | public IP `54.116.106.151`, EIP 고정 |
| 학습/평가 | EC2 `i-087b8e30654d2d62d` (train) | **아직 이 CD 논의 범위 아님** — 학습용 인스턴스가 따로 준비 안 됨 |
| 피처마트 | EMR (transient 클러스터) | `emr-features` 타겟이 매번 클러스터를 새로 띄우고 `--auto-terminate`로 끝냄. "상시 배포 대상"이 아니라 "코드를 S3에 미리 올려두면 다음 실행이 그 코드를 씀" |
| 서빙용 Gold 데이터 | RDS `gng-ubd-db` (postgres 16) | `publicly_accessible: false` — VPC 내부(=app/train EC2)에서만 접근 가능 |
| 나머지 데이터 | S3 `gng-ubd-s3-bucket` | 설정(`config/prod.env`, `config/secrets.env`)도 여기서 옴 |

### 이미 있는 배포 도구 (Makefile)

`deploy-*` 타겟들이 이미 잘 갖춰져 있다 — **거의 전부 "app EC2 안에서 실행"을 전제로 짜여 있다**:

- `deploy-env` — S3 config를 내려받아 `/opt/app/.env` 생성
- `deploy-db-bootstrap` / `deploy-db-check` — RDS에 직접 붙어야 함(VPC 내부 필요)
- `deploy-up` / `deploy-resync` / `deploy-restart` / `deploy-smoke` — `docker-compose.prod.yml` 기동/재기동/헬스체크
- `deploy-seed-models` — 로컬 `models/`를 S3로 sync
- `emr-package` / `emr-features` — 로컬 PC나 아무 머신에서나 실행 가능(AWS CLI + terraform output만 있으면 됨, VPC 무관)

즉 **EC2 관련 배포는 "EC2 내부에서 실행"이 이미 전제된 설계**고, EMR 쪽은 "아무 데서나 실행 가능"이다.

### frontend(apps/web)는 S3가 아니라 EC2 컨테이너다

`ops/compose/Dockerfile.web` 확인 결과 — 빌드타임에 정적 번들을 만들어 **nginx 컨테이너로 EC2 위에서 서빙**한다(`docker-compose.prod.yml`의 `web` 서비스). S3 정적 호스팅이 아니다.

> **확인 필요**: 이번 요청에서 "기타 나머지 데이터(프론트 포함)는 S3"라고 하셨는데, 코드상 프론트*앱*은 EC2 컨테이너로 떠 있다. 프론트가 다루는 *데이터*(파이프라인 산출물)가 S3에 있다는 뜻으로 이해했다 — 만약 정말로 프론트를 S3 정적 호스팅으로 옮기고 싶다는 의미였다면 별도 논의가 필요하다(CloudFront/버킷 정책 등 새 인프라 필요).

## 2. 핵심 제약사항 — 왜 이게 어려운 문제인가

`terraform/variables.tf`와 `Makefile`에 명시적으로 적혀 있다:

> SSM이 이 계정에서 전면 거부되어(StartSession·SendCommand·DescribeInstanceInformation 모두) SSH가 유일한 접속 수단이다.

그리고 SSH도 아무나 못 붙는다 — `admin_cidrs` 보안그룹으로 **특정 IP만 화이트리스트**해서 열어준다(팀원이 IP 바뀔 때마다 `make allow-my-ip`로 본인 IP를 추가). MLflow/Airflow UI를 인터넷에 노출하지 않으려고 일부러 이렇게 좁혀놨다.

**문제**: GitHub Actions의 기본(호스티드) 러너는 실행할 때마다 IP가 랜덤하게 바뀐다. 지금 방식의 "고정 IP 화이트리스트"와 구조적으로 안 맞는다. 그래서 이번에 세 가지 방식 중 골라야 한다(SSH 기반 A/B, SSM 기반 C).

## 3. CD 실행 방식 — 옵션 비교

### 옵션 A: app EC2에 self-hosted runner 설치 (추천)

GitHub Actions runner 에이전트를 app EC2 안에 서비스로 설치해서, "배포"라는 job 자체를 **EC2 안에서 직접 실행**시킨다. GitHub 쪽에서 보면 그냥 "이 리포의 self-hosted runner 하나가 잡을 수행"하는 것뿐이라, GitHub → EC2로 들어오는 네트워크 연결이 전혀 없다(반대로 EC2 → GitHub로 나가는 아웃바운드 폴링만 있음). 그래서 SG/IP 화이트리스트 문제 자체가 사라진다.

**장점**
- GitHub Secrets에 **AWS 키도, SSH 개인키도 저장할 필요 없음** — 배포 job은 EC2에 이미 붙어있는 instance profile(`gng-ubd-app` role)을 그대로 씀
- `admin_cidrs` 화이트리스트를 건드릴 필요 없음(임시로 열었다 닫았다 안 해도 됨)
- 기존 `deploy-*` Makefile 타겟이 "EC2 안에서 실행"을 전제로 이미 짜여 있어서 마찰 없이 그대로 씀
- 설정이 한 번만 필요(등록 스크립트 1회 실행) — 이후엔 서비스로 상주

**단점**
- EC2 하나가 "앱 서버"이면서 동시에 "CI 실행 환경"도 겸함 — 배포 스크립트에 문제가 있으면(예: 무한루프) 그 EC2 리소스를 잡아먹음. 다만 이 프로젝트 규모(데모/소규모 팀)에서는 흔히 쓰는 절충
- runner 프로세스 자체의 유지보수(업데이트, 재등록)를 누군가 챙겨야 함
- **최초 설치는 EC2에 SSH로 직접 들어가서 수동으로 해야 함**(지금 SSH 접속 가능한 사람이 1회 작업)

**필요한 GitHub 설정**
- Secrets/Variables: **없음** (배포 job 자체는)
- 리포 Settings → Actions → Runners → "New self-hosted runner"에서 나오는 등록 토큰은 최초 1회용(약 1시간 후 만료)이라 GitHub Secret으로 저장 안 해도 됨

**EC2에서 1회 실행할 설치 절차 (예시)**
```bash
# app EC2에 SSH로 접속한 상태에서
mkdir -p /opt/actions-runner && cd /opt/actions-runner
curl -o actions-runner.tar.gz -L https://github.com/actions/runner/releases/download/vX.Y.Z/actions-runner-linux-arm64-X.Y.Z.tar.gz
tar xzf actions-runner.tar.gz

# GitHub 리포 Settings > Actions > Runners > New self-hosted runner 에서 토큰 발급받아 실행
./config.sh --url https://github.com/<org>/<repo> --token <등록토큰> \
  --labels gng-ubd-app --name gng-ubd-app-runner --work _work

sudo ./svc.sh install
sudo ./svc.sh start
```
(arm64용 러너 tarball 링크인지 확인 필요 — t4g가 ARM이므로)

---

### 옵션 B: GitHub 호스티드 러너 + 매 배포마다 SG 임시 오픈

기존 `allow-ip`/`revoke-ip` 패턴을 CI에서 그대로 재현한다. 배포 job이:
1. AWS 자격증명으로 러너의 임시 공인 IP를 알아냄
2. `terraform apply -target=aws_security_group.app`로 그 IP를 `admin_cidrs`에 추가
3. SSH로 접속해 `deploy-*` 실행
4. 끝나면 다시 `terraform apply`로 그 IP 제거

**장점**
- EC2에 아무것도 새로 설치할 필요 없음(순수 GitHub 쪽 설정만으로 끝)

**단점**
- **AWS 자격증명 + SSH 개인키를 GitHub Secrets에 저장해야 함** — 유출 시 인프라 전체가 위험
- CI에 **terraform apply 권한**을 줘야 함 — 지금 terraform은 다른 팀원이 작업 중인데, CI가 매 배포마다 `admin_cidrs` state를 건드리면 그 팀원의 작업과 충돌할 위험이 큼(같은 state 파일을 두 곳에서 동시에 apply)
- 병렬로 두 배포가 겹치면 SG 규칙이 꼬일 수 있음(락 필요)
- 이 계정이 SSM을 정책적으로 막아둔 것 자체가 "외부에서의 원격 접근을 최소화하자"는 의도로 보이는데, 이 옵션은 그 의도와 반대 방향

**필요한 GitHub Secrets**
| 이름 | 용도 |
|---|---|
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (또는 OIDC role) | terraform apply + EC2 IP 조회 |
| `SSH_PRIVATE_KEY` | `gng-ubd-admin.pem` 내용 |
| `EC2_HOST` | app EC2 public IP (또는 매번 `terraform output`으로 조회) |

**필요한 GitHub Variables**
| 이름 | 값 |
|---|---|
| `AWS_REGION` | `ap-northeast-2` |

---

### 옵션 C: 계정 SCP를 바꿔서 SSM 허용 (추천 후보)

`terraform/*.tf` 주석을 다시 훑어보니, **이 프로젝트 쪽 설정은 이미 SSM을 쓸 준비가 전부 되어 있다**:

- `compute_app.tf` — app EC2 role에 `AmazonSSMManagedInstanceCore` 이미 부착, SG 아웃바운드도 "SSM 엔드포인트(443)와 S3에 나가야 한다"고 이미 열어둠
- `network.tf` — SSM Session Manager를 전제로 한 아웃바운드 규칙이 이미 있음
- 유일하게 막는 건 **AWS Organizations SCP**(Service Control Policy) 하나 — `ssm:StartSession`/`ssm:SendCommand`/`ssm:DescribeInstanceInformation`을 계정 레벨에서 전면 거부

즉 이 리포의 terraform은 "원래 SSM으로 접속할 생각으로 설계"돼 있었는데, SCP가 막아서 SSH로 우회한 흔적으로 보인다. **SCP만 풀리면 이 프로젝트 쪽에서는 추가로 고칠 게 거의 없다.**

SCP가 풀리면 CD는 이렇게 짤 수 있다 — GitHub 호스티드 러너에서 AWS 자격증명(OIDC 권장)만으로 바로 배포:

```yaml
- uses: aws-actions/configure-aws-credentials@v4
  with:
    role-to-assume: ${{ secrets.AWS_ROLE_TO_ASSUME }}
    aws-region: ap-northeast-2

- name: Deploy via SSM
  run: |
    CMD_ID=$(aws ssm send-command \
      --instance-ids "${{ vars.APP_INSTANCE_ID }}" \
      --document-name "AWS-RunShellScript" \
      --output-s3-bucket-name "${{ vars.S3_BUCKET }}" \
      --output-s3-key-prefix "ci-logs/" \
      --parameters 'commands=["cd /opt/app && git fetch origin main && git reset --hard origin/main && make deploy-env && make deploy-resync && make deploy-up && make deploy-smoke"]' \
      --query "Command.CommandId" --output text)
    aws ssm wait command-executed --command-id "$CMD_ID" --instance-id "${{ vars.APP_INSTANCE_ID }}"
    aws ssm get-command-invocation --command-id "$CMD_ID" --instance-id "${{ vars.APP_INSTANCE_ID }}"
```

**장점**
- self-hosted runner 설치·유지보수 불필요(옵션 A 대비) — GitHub 호스티드 러너로 끝
- SSH 개인키를 아예 안 씀 — 지금처럼 `admin_cidrs`를 IP 바뀔 때마다 갱신할 필요도 없어짐(원한다면 나중에 SG에서 22번 포트 자체를 없애 공격 표면을 더 줄일 수도 있음)
- AWS 표준 방식이라 CloudTrail에 실행 커맨드가 그대로 로그로 남음(SSH보다 감사 추적이 쉬움)
- 이미 준비된 IAM/네트워크 설정을 그대로 재사용 — 이 리포 terraform은 추가로 거의 안 건드려도 됨

**단점**
- **SCP는 이 리포의 terraform으로 못 바꾼다** — AWS Organizations 관리 계정(대개 이 프로젝트 팀과는 다른 조직/계정)에서 별도로 풀어야 한다. "정책 변경해도 된다"는 확인은 받았지만, 실제로 누가·어디서·언제 풀 수 있는지는 이 회의에서 별도로 확인 필요 (다른 인프라 담당자/조직 관리자 승인 절차가 있을 수 있음)
- SCP를 푸는 순간 SSM이 계정 전체(이 프로젝트뿐 아니라 다른 프로젝트/리소스까지)에 대해 풀리는 것이라, 원래 그걸 막아둔 이유(보안팀 정책?)를 먼저 파악해두는 게 안전함
- `send-command`는 표준출력 48KB 제한이 있어 `--output-s3-bucket-name`으로 로그를 S3에 스트리밍하도록 같이 설정해야 함(위 예시에 반영)

**필요한 GitHub 설정**
| 종류 | 이름 | 값 |
|---|---|---|
| Secret | `AWS_ROLE_TO_ASSUME` | OIDC로 assume할 IAM role ARN (`ssm:SendCommand`·`ssm:GetCommandInvocation`을 app 인스턴스 ARN으로 좁힌 정책) |
| Variable | `AWS_REGION` | `ap-northeast-2` |
| Variable | `APP_INSTANCE_ID` | `i-077c84802978f077b` |
| Variable | `S3_BUCKET` | `gng-ubd-s3-bucket` |

옵션 A와 마찬가지로 OIDC role을 위한 terraform(`aws_iam_openid_connect_provider` + role)이 필요하지만, 이번엔 EMR job과 **같은 OIDC provider를 공유**하고 role만 나눠도 된다.

---

### 공통: EMR 피처마트 코드 갱신 job

EMR은 상시 서버가 아니라서 "배포"라는 개념이 약간 다르다 — `emr-package`로 `libs/core`, `libs/ml_core`, `ml/feature_engine`을 tar로 묶어 S3(`emr/pyfiles.tar.gz`)에 올려두면, 다음에 Airflow가 `emr-features`로 클러스터를 띄울 때 그 코드를 그대로 씀. 이건 **VPC 접근이 필요 없고 S3 쓰기 권한만 있으면 되므로**, 옵션 A/B와 무관하게 **GitHub 호스티드 러너에서 그대로 실행 가능**하다.

권장: OIDC 방식(`aws-actions/configure-aws-credentials` + `role-to-assume`)으로 장기 AWS 키를 GitHub에 안 두는 것. 이러려면 terraform에 아래와 같은 리소스가 추가로 필요하다(다른 팀원과 조율 후 반영):

```hcl
# 예시 스니펫 — 실제 반영 전 팀 논의 필요
resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

resource "aws_iam_role" "github_actions_emr_package" {
  name = "gng-ubd-github-actions-emr-package"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Federated = aws_iam_openid_connect_provider.github.arn }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = { "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com" }
        StringLike   = { "token.actions.githubusercontent.com:sub" = "repo:<org>/<repo>:ref:refs/heads/main" }
      }
    }]
  })
}
# S3 emr/ prefix에 대한 PutObject만 허용하는 정책을 별도로 붙임
```

**필요한 GitHub 설정 (이 job만)**
| 종류 | 이름 | 값 |
|---|---|---|
| Secret | `AWS_ROLE_TO_ASSUME` | 위 IAM role ARN (OIDC 방식) — 또는 대안으로 static 키 2개 |
| Variable | `AWS_REGION` | `ap-northeast-2` |
| Variable | `S3_BUCKET` | `gng-ubd-s3-bucket` |

## 4. 배포 대상별 정리

| 대상 | CD에서 할 일 | 실행 위치 | 비고 |
|---|---|---|---|
| EC2(Airflow·collector·apps/web·apps/api 등) | `git pull` → `deploy-env` → `deploy-resync` → `deploy-up` → `deploy-smoke` | app EC2 (옵션 A: self-hosted runner / 옵션 B: SSH) | 이미 Makefile에 다 있음, 새로 짤 로직 거의 없음 |
| EMR(피처마트) | `emr-package`로 코드 S3에 재업로드 | 아무 곳(호스티드 러너 가능) | "배포"라기보다 "다음 실행이 쓸 코드 갱신" |
| RDS(Gold 서빙) | 별도 CD 불필요 | — | app EC2가 이미 붙어서 씀. 스키마 변경 시에만 `deploy-db-bootstrap`/`check` 수동 실행 |
| S3(데이터/설정) | 별도 CD 불필요 | — | 데이터는 파이프라인이 직접 씀, 설정은 `deploy-secrets`로 이미 관리 중 |
| 학습/평가(train EC2) | **범위 밖** | — | 아직 전용 인스턴스/배포 방식 미정 — 다음 논의 |

## 5. 다음 단계 (회의에서 정할 것)

1. 옵션 A/B/C 중 선택
   - **SCP를 실제로 언제 풀 수 있는지가 관건**: 바로 풀 수 있으면 **C 추천**(장기적으로 가장 깔끔 — self-hosted runner 유지보수도 없고 SSH도 아예 없앨 수 있음)
   - SCP 승인에 시간이 걸리거나 이번 스프린트 내로 확답이 안 되면, 그 사이엔 **A로 먼저 진행**하고 SCP가 풀리면 C로 옮겨가는 것도 방법(A→C 전환은 GitHub 쪽 workflow만 갈아끼우면 되고 EC2 쪽엔 영향 없음)
   - B는 terraform 동시 작업 충돌 위험 때문에 우선순위 낮음
2. "프론트 포함 S3" 발언이 정말 프론트 앱을 S3로 옮기자는 뜻인지 확인
3. EMR 코드 갱신 job의 인증 방식(OIDC vs static 키) 결정 — OIDC면 terraform 반영을 다른 작업자와 조율
4. main 브랜치 push 시 자동 배포로 할지, 수동 트리거(`workflow_dispatch`)로 할지
5. 학습/평가 인스턴스는 이번 스코프에서 제외하고 별도 후속 작업으로 남길지 확인

여기까지 결정되면 실제 `.github/workflows/deploy.yml`과(필요시) terraform 반영을 진행하겠습니다.
