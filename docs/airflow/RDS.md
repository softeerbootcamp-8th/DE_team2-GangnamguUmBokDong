# AWS RDS PostgreSQL 구축 및 실행 가이드

AWS 배포에서는 로컬 Compose의 `postgres` 컨테이너를 Amazon RDS for PostgreSQL로
대체한다. 하나의 RDS 인스턴스에 애플리케이션, Airflow, MLflow용 데이터베이스를
분리하고, 애플리케이션 DB에는 PostGIS와 Gold 기준 스키마를 적용한다.

## 권장 구성

- Region: 서울(`ap-northeast-2`)
- Engine: Amazon RDS for PostgreSQL 16
- 개발 instance: `db.t4g.small` 또는 `db.t4g.medium`
- Storage: `gp3` 50~100 GiB, storage autoscaling 활성화
- Network: 두 개 이상의 private subnet으로 구성한 DB subnet group
- Public access: 비활성화
- Credentials: AWS Secrets Manager에서 master password 관리
- Encryption: KMS 저장 암호화 활성화

운영 환경에서는 Multi-AZ, 7~14일 이상의 자동 백업, 삭제 방지, Performance Insights와
Enhanced Monitoring을 추가한다. Instance와 storage 크기는 부하 측정 결과에 따라
조정한다.

현재 로컬 환경은 `postgis/postgis:16-3.5` 이미지를 사용한다. RDS에서는 Docker
이미지의 PostGIS 버전을 그대로 고를 수 없으므로, 선택한 PostgreSQL minor version에서
제공하는 버전을 확인한다.

```sql
SELECT *
FROM pg_available_extension_versions
WHERE name = 'postgis';
```

## 네트워크와 보안 그룹

RDS는 애플리케이션과 같은 VPC의 private subnet에 생성한다. RDS security group의
inbound에는 공인 IP나 `0.0.0.0/0` 대신 다음 실행 환경의 security group을 source로
지정하여 TCP `5432`만 허용한다.

- Airflow scheduler와 API server
- 애플리케이션 API
- MLflow server
- 초기화 또는 migration을 실행하는 ECS task/EC2 instance

개발자 접속은 VPN, bastion host 또는 AWS Systems Manager Session Manager의 port
forwarding을 사용한다.

## RDS 인스턴스 생성

AWS Console의 **RDS → Databases → Create database**에서 다음 순서로 생성한다.

1. `Standard create`와 PostgreSQL 16 계열을 선택한다.
2. DB identifier를 지정한다. 예: `gangnam-bike-prod`.
3. Master username을 지정한다. 예: `dbadmin`.
4. `Manage master credentials in AWS Secrets Manager`를 활성화한다.
5. VPC, DB subnet group, RDS security group을 선택한다.
6. Public access를 `No`로 설정한다.
7. Storage, backup, encryption, monitoring 설정을 확인하고 생성한다.

애플리케이션은 master 계정을 상시 사용하지 않는다. Master 계정은 최초 database,
PostGIS extension, role과 schema를 준비할 때만 사용한다.

## 데이터베이스와 사용자 준비

현재 저장소는 다음 세 database를 사용한다.

| Database | 용도 | 실행 사용자 예시 |
| --- | --- | --- |
| `app` | Gold 및 서비스 데이터 | `app_user` |
| `airflow` | Airflow metadata | `airflow_user` |
| `mlflow` | MLflow backend metadata | `mlflow_user` |

RDS와 통신 가능한 환경에서 master 계정으로 접속한다.

```bash
psql \
  "host=<RDS_ENDPOINT> port=5432 dbname=postgres user=dbadmin sslmode=require"
```

Database를 생성한다. 이는 로컬 최초 기동 때
`ops/postgres/init/001_create_databases.sh`가 담당하던 과정이다.

```sql
CREATE DATABASE app;
CREATE DATABASE airflow;
CREATE DATABASE mlflow;
```

실행 사용자를 분리하고 연결 권한을 부여한다.

```sql
CREATE ROLE app_user LOGIN PASSWORD '<APP_PASSWORD>';
CREATE ROLE airflow_user LOGIN PASSWORD '<AIRFLOW_PASSWORD>';
CREATE ROLE mlflow_user LOGIN PASSWORD '<MLFLOW_PASSWORD>';

GRANT CONNECT ON DATABASE app TO app_user;
GRANT CONNECT ON DATABASE airflow TO airflow_user;
GRANT CONNECT ON DATABASE mlflow TO mlflow_user;
```

비밀번호는 Git, `.env`, Docker image 또는 ECS task definition에 평문으로 넣지 않는다.
Secrets Manager에 저장하고 실행 환경의 IAM role에 필요한 secret 읽기 권한만 부여한다.

## PostGIS 및 Gold 스키마 초기화

PostGIS extension 설치에는 RDS의 높은 권한이 필요하다. 선택한 engine version에서
PostGIS 제공 여부를 확인한 뒤, 새로 만든 빈 `app` database에 Gold 기준 스키마를 한
번만 적용한다.

```bash
psql \
  "host=<RDS_ENDPOINT> port=5432 dbname=app user=dbadmin sslmode=require" \
  -v ON_ERROR_STOP=1 \
  -f docs/gold/target-schema.sql
```

`docs/gold/target-schema.sql`은 내부에서 PostGIS를 생성한다.

```sql
CREATE EXTENSION IF NOT EXISTS postgis;
```

적용 결과를 확인한다.

```sql
SELECT PostGIS_Version();
SELECT extname, extversion
FROM pg_extension
WHERE extname = 'postgis';
```

저장소의 schema contract 검사도 실행한다.

```bash
PGHOST=<RDS_ENDPOINT> \
PGPORT=5432 \
PGUSER=dbadmin \
PGDATABASE=app \
PGSSLMODE=require \
POSTGRES_USER=dbadmin \
POSTGRES_APP_DB=app \
bash ops/postgres/check_gold_schema.sh
```

`target-schema.sql`은 migration이 아니라 **빈 database용 최초 baseline**이다. 데이터가
있는 운영 DB에 반복 실행하지 않는다. 이후 변경은 버전이 부여된 migration으로
관리해야 한다.

초기화가 끝나면 `app_user`에 필요한 schema와 object의 소유권 또는 최소 권한을
부여한다. 실제 권한은 Gold loader의 DDL/DML 범위에 맞춰 확정하며 애플리케이션
사용자에게 `rds_superuser`를 부여하지 않는다.

## 애플리케이션 연결 정보

AWS 실행 환경에는 다음 형태의 연결 정보를 주입한다.

```dotenv
DATABASE_URL=postgresql://app_user:<PASSWORD>@<RDS_ENDPOINT>:5432/app?sslmode=require
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow_user:<PASSWORD>@<RDS_ENDPOINT>:5432/airflow?sslmode=require
MLFLOW_BACKEND_STORE_URI=postgresql://mlflow_user:<PASSWORD>@<RDS_ENDPOINT>:5432/mlflow?sslmode=require
```

RDS PostgreSQL 15 이상은 기본적으로 SSL 연결을 강제한다. 최소한
`sslmode=require`를 사용하고, 운영에서는 RDS CA bundle을 신뢰 저장소에 넣은 뒤
`sslmode=verify-full`로 hostname과 인증서를 검증한다.

Airflow는 `airflow-init`을 한 번 실행해 metadata migration을 마친 뒤 API server와
scheduler를 시작한다. MLflow는 `mlflow` database를 backend store로 지정한다.

## Compose 또는 ECS 배포 시 변경점

현재 `ops/compose/docker-compose.yml`에는 다음 로컬 PostgreSQL 결합이 있다.

- `postgres` 서비스와 volume
- hostname `postgres`
- `postgres-schema-check` 서비스
- 각 서비스의 `depends_on: postgres`
- 연결 문자열의 `postgres:5432`

따라서 AWS에서 기존 `make up`을 그대로 실행하지 않는다. AWS 전용 Compose override나
ECS task definition에서 다음과 같이 변경한다.

1. `postgres` 서비스와 로컬 Postgres volume을 제거한다.
2. `depends_on: postgres`를 제거한다.
3. 하드코딩된 DB URL 대신 Secrets Manager에서 연결 정보를 주입한다.
4. `postgres-schema-check`를 배포 전에 한 번 실행하는 init/migration task로 바꾼다.
5. Init task 성공 후 Airflow, MLflow와 API 서비스를 시작한다.

RDS는 `docker compose up`이나 `make up`으로 실행하는 프로세스가 아니다. RDS 상태가
`Available`이고 network와 schema 준비가 끝나면 애플리케이션이 endpoint로 접속한다.

## 최초 배포 순서

1. VPC, private subnet, DB subnet group과 security group을 생성한다.
2. RDS PostgreSQL instance를 생성한다.
3. `app`, `airflow`, `mlflow` database와 실행 role을 생성한다.
4. `app`에 PostGIS와 `docs/gold/target-schema.sql`을 적용한다.
5. Gold schema contract 검사를 실행한다.
6. Airflow metadata migration을 실행한다.
7. MLflow server를 시작한다.
8. Airflow API server, scheduler와 애플리케이션 API를 시작한다.
9. 승인된 dispatch center와 weather grid seed를 명시적으로 실행한다.
10. CloudWatch alarm, backup과 복구 절차를 점검한다.

운영 전에는 snapshot 복구를 실제로 시험한다. CPU, free storage, database connections,
freeable memory, read/write latency에 CloudWatch alarm을 설정한다. Connection 수가 빠르게
늘면 connection pool 조정 또는 RDS Proxy 도입을 검토한다.

초기에는 한 RDS instance에 세 database를 구성해도 된다. Airflow와 MLflow의 metadata
부하가 `app`의 실시간 처리에 영향을 주기 시작하면 애플리케이션용 RDS와 platform
metadata용 RDS를 분리한다.

## AWS 공식 참고 자료

- [RDS VPC security group으로 접근 제어](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.RDSSecurityGroups.html)
- [RDS PostgreSQL에서 PostGIS 관리](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.PostGIS.html)
- [RDS PostgreSQL SSL 연결](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.SSL.html)
- [RDS와 AWS Secrets Manager 연동](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-secrets-manager.html)