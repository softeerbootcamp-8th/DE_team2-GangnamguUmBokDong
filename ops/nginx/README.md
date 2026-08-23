# 대시보드 접근 제어 (nginx Basic Auth)

회원가입 없이, 서울시 담당자 등 소수 계정만 미리 심어두는 방식이다. `default.conf`가
`server {}` 레벨에 `auth_basic`을 걸어 정적 자산과 `/api/` 프록시를 전부 잠근다 —
API를 직접 두드려도 로그인 없이는 401이다.

## 동작 방식

- 브라우저가 `Authorization: Basic base64(id:pw)` 헤더를 보내고, nginx가
  `.htpasswd` 파일의 해시(bcrypt)와 대조한다. 서버는 세션을 따로 관리하지 않는다.
- **자격증명 유지 기간은 브라우저가 결정한다.** 한 번 입력하면 브라우저가 메모리에
  캐시해뒀다가 같은 origin으로 가는 모든 요청에 자동으로 다시 실어 보낸다. 이건
  "탭"이 아니라 "브라우저 프로세스" 단위로 유지되는 경우가 많다 — 탭 하나를 닫아도
  같은 브라우저의 다른 탭·창이 남아 있으면 안 풀린다. 서버가 쿠키처럼 만료 시간을
  강제할 방법이 없다. 정확히 "탭 닫으면 즉시 로그아웃"이 필요하면 Basic Auth로는
  불가능하고 쿠키/세션 기반 로그인(API 레벨)으로 가야 한다.
- 로그인이라는 별도 이벤트가 없어서(매 요청마다 인증), `access_log`에
  `$remote_addr`(IP) + `$remote_user`(인증 성공 시 nginx가 채움)를 같이 남겨
  로그인 로그를 대신한다. 실패한 시도도 어떤 아이디로 시도했는지, 어느 IP인지 남는다.

## 계정 추가/삭제

로컬에서 파일 하나 만들어서 EC2에 반영하는 방식이다. `.htpasswd`는 **git에 올리지
않는다**(`.gitignore` 처리됨) — 비밀번호 해시라도 저장소에 남으면 오프라인 크래킹
대상이 된다.

### 최초 설정 (또는 계정 전체를 다시 쓰고 싶을 때)

1. `config/secrets.env`(S3, `SEOUL_OPENAPI_KEY`/`KMA_APIHUB_KEY`와 같은 객체)에
   아래 두 줄을 추가한다:
   ```
   NGINX_BASIC_AUTH_USER=<아이디>
   NGINX_BASIC_AUTH_PASSWORD=<비밀번호>
   ```
2. app EC2에서:
   ```bash
   make deploy-env          # secrets.env를 다시 내려받아 /opt/app/.env 갱신
   make deploy-nginx-auth   # /opt/app/.env로 .htpasswd 생성
   make deploy-up           # (최초 1회) 또는 web만 재시작:
   # docker compose --env-file /opt/app/.env -f ops/compose/docker-compose.prod.yml up -d --force-recreate web
   ```

### 계정을 하나 더 추가 (기존 계정은 유지)

app EC2에서 직접 htpasswd 파일에 append한다(`-c` 옵션은 파일을 통째로 새로
만들어 기존 계정을 지우므로 절대 쓰지 않는다):

```bash
htpasswd -B /opt/app/ops/nginx/.htpasswd <새 아이디>
docker compose --env-file /opt/app/.env -f ops/compose/docker-compose.prod.yml \
  up -d --force-recreate web
```

### 계정 삭제

```bash
htpasswd -D /opt/app/ops/nginx/.htpasswd <지울 아이디>
docker compose --env-file /opt/app/.env -f ops/compose/docker-compose.prod.yml \
  up -d --force-recreate web
```

`web` 컨테이너를 재생성해야 nginx가 새 `.htpasswd`를 다시 읽는다(파일은 볼륨
마운트라 이미지 재빌드는 필요 없다).

## 전제조건: HTTPS

지금 대시보드는 도메인·인증서 없이 EC2 public IP로 평문 HTTP 접속이다. Basic
Auth는 자격증명을 base64(=사실상 평문)로 매 요청마다 보내므로, 로그인 기능을
쓰는 이상 HTTPS(자체 서명 인증서든 도메인+Let's Encrypt든)를 별도로 검토해야
한다. 이건 이번 변경 범위 밖이라 여기서는 처리하지 않았다.
