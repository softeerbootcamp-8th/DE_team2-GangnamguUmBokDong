PROJECTS := collector apps/api airflow ml/inference ml/training ml/feature_engine libs/core libs/ml_core normalizer nowcaster loader rebalance
LOCAL_TEST_PROJECTS := collector apps/api ml/inference ml/training ml/feature_engine libs/core libs/ml_core normalizer nowcaster loader rebalance
CI_TEST_PROJECTS := collector apps/api ml/inference libs/core libs/ml_core normalizer nowcaster loader rebalance
CI_UNIT_PROJECTS := collector apps/api ml/inference libs/core libs/ml_core normalizer nowcaster rebalance
CI_INTEGRATION_PROJECTS := loader

PLATFORM_COMPOSE := $(shell bash ops/compose/platform_args.sh)
COMPOSE = docker compose $(if $(wildcard .env),--env-file .env,) -f ops/compose/docker-compose.yml $(PLATFORM_COMPOSE)

.PHONY: sync-all sync-ci-unit lint test-gold-bootstrap test-gold-transition-available test test-ci test-ci-unit test-ci-integration bootstrap up down logs ps migrate-route-cancellation migrate-route-dismiss-restore seed bootstrap-gold-seeds seed-e2e e2e-preflight e2e-smoke

E2E_LOGICAL_DTTM ?= $(shell TZ=Asia/Seoul date '+%Y-%m-%dT%H:%M:00+09:00' | awk -F: '{ printf "%s:%02d:00+09:00\n", $$1, int($$2 / 5) * 5 }')
E2E_STATION_SOURCE_DTTM ?= $(shell python3 ops/e2e_time.py station-source '$(E2E_LOGICAL_DTTM)')
GOLD_DISPATCH_CENTER_EFFECTIVE_DTTM := 2026-08-19T03:15:38Z
GOLD_WEATHER_GRID_SEED_VERSION ?= local-dev-weather-grid-v1
GOLD_WEATHER_GRID_EFFECTIVE_DTTM ?= 2026-08-19T03:15:38Z

sync-all:
	@for p in $(PROJECTS); do \
		echo "==> $$p"; \
		(cd $$p && uv sync) || exit 1; \
	done

sync-ci-unit:
	@for p in $(CI_UNIT_PROJECTS); do \
		echo "==> $$p"; \
		(cd $$p && uv sync --frozen) || exit 1; \
	done

lint:
	@for p in $(PROJECTS); do \
		echo "==> $$p"; \
		(cd $$p && uvx ruff check .) || exit 1; \
	done

test-gold-bootstrap:
	bash ops/postgres/tests/test_bootstrap.sh

test-gold-transition-available:
	bash ops/gold/tests/run_transition_validation.sh

test: test-gold-bootstrap
	@for p in $(LOCAL_TEST_PROJECTS); do \
		echo "==> $$p"; \
		(cd $$p && uv run --with pytest pytest -q); \
		code=$$?; \
		if [ $$code -ne 0 ] && [ $$code -ne 5 ]; then exit $$code; fi; \
	done
	@echo "==> airflow"
	@$(COMPOSE) exec -T airflow-scheduler \
		sh -lc 'cd /workspace/airflow && uv run pytest -q'

test-ci:
	@for p in $(CI_TEST_PROJECTS); do \
		echo "==> $$p"; \
		(cd $$p && uv run --with pytest pytest -q); \
		code=$$?; \
		if [ $$code -ne 0 ] && [ $$code -ne 5 ]; then exit $$code; fi; \
	done
	@echo "==> airflow"
	@$(COMPOSE) exec -T airflow-scheduler \
		sh -lc 'cd /workspace/airflow && uv run pytest -q'

test-ci-unit: test-gold-bootstrap
	@for p in $(CI_UNIT_PROJECTS); do \
		echo "==> $$p"; \
		(cd $$p && uv run --with pytest pytest -q); \
		code=$$?; \
		if [ $$code -ne 0 ] && [ $$code -ne 5 ]; then exit $$code; fi; \
	done

test-ci-integration:
	@for p in $(CI_INTEGRATION_PROJECTS); do \
		echo "==> $$p"; \
		(cd $$p && uv run --with pytest pytest -q); \
		code=$$?; \
		if [ $$code -ne 0 ] && [ $$code -ne 5 ]; then exit $$code; fi; \
	done
	@echo "==> airflow"
	@$(COMPOSE) exec -T airflow-scheduler \
		sh -lc 'cd /workspace/airflow && uv run pytest -q'
		
bootstrap:
	./ops/bootstrap/bootstrap.sh

up:
	@$(COMPOSE) up -d --build postgres postgres-schema-check || { \
		$(COMPOSE) logs --no-color postgres postgres-schema-check; \
		exit 1; \
	}
	@$(COMPOSE) wait postgres-schema-check >/dev/null 2>&1 || true
	@schema_check_id="$$( $(COMPOSE) ps --all --quiet postgres-schema-check)"; \
	if [ -z "$$schema_check_id" ]; then \
		echo "[gold-postgis] schema-check 컨테이너를 찾을 수 없습니다." >&2; \
		exit 1; \
	fi; \
	if ! schema_check_state="$$(docker inspect --format '{{.State.Status}}:{{.State.ExitCode}}' "$$schema_check_id")"; then \
		echo "[gold-postgis] schema-check 컨테이너 상태를 읽을 수 없습니다." >&2; \
		exit 1; \
	fi; \
	if [ "$$schema_check_state" != "exited:0" ]; then \
		echo "[gold-postgis] schema-check 실패: $$schema_check_state" >&2; \
		$(COMPOSE) logs --no-color postgres postgres-schema-check; \
		exit 1; \
	fi
	@$(COMPOSE) up -d --build || { \
		$(COMPOSE) logs --no-color postgres postgres-schema-check airflow-init; \
		exit 1; \
	}

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

migrate-route-cancellation:
	@$(COMPOSE) exec -T postgres \
		psql -v ON_ERROR_STOP=1 -U "$${POSTGRES_USER:-postgres}" -d "$${POSTGRES_APP_DB:-app}" \
		< ops/postgres/migrations/130_add_route_cancellation.sql

migrate-route-dismiss-restore:
	@$(COMPOSE) exec -T postgres \
		psql -v ON_ERROR_STOP=1 -U "$${POSTGRES_USER:-postgres}" -d "$${POSTGRES_APP_DB:-app}" \
		< ops/postgres/migrations/131_add_route_dismiss_and_restore.sql

seed:
	@echo "[gold-postgis] make seed는 weather grid seed_version/effective_dttm SSOT 확정 전이라 비활성화되었습니다." >&2
	@echo "[gold-postgis] 승인된 값으로 loader/gold_cli.py의 seed:dispatch_center, seed:weather_grid를 명시적으로 실행하세요." >&2
	@false

# 신규 로컬 Gold DB에서 최초 1회 실행한다. make up에 자동 연결하지 않는다.
# AWS에서는 승인된 GOLD_WEATHER_GRID_SEED_VERSION/EFFECTIVE_DTTM을 명시한다.
bootstrap-gold-seeds:
	@test -n "$(GOLD_WEATHER_GRID_SEED_VERSION)" || { \
		echo "[gold-postgis] GOLD_WEATHER_GRID_SEED_VERSION이 필요합니다." >&2; \
		exit 2; \
	}
	@test -n "$(GOLD_WEATHER_GRID_EFFECTIVE_DTTM)" || { \
		echo "[gold-postgis] GOLD_WEATHER_GRID_EFFECTIVE_DTTM이 필요합니다." >&2; \
		exit 2; \
	}
	@echo "[gold-postgis] dispatch_center seed 게시"
	@$(COMPOSE) exec -T airflow-scheduler sh -lc \
		'cd /workspace/loader && env -u VIRTUAL_ENV UV_PROJECT_ENVIRONMENT=/opt/venvs/modules/loader uv run --frozen python gold_cli.py --publication seed:dispatch_center --window-start "$(GOLD_DISPATCH_CENTER_EFFECTIVE_DTTM)"'
	@echo "[gold-postgis] weather_grid seed 게시: version=$(GOLD_WEATHER_GRID_SEED_VERSION) effective=$(GOLD_WEATHER_GRID_EFFECTIVE_DTTM)"
	@$(COMPOSE) exec -T airflow-scheduler sh -lc \
		'cd /workspace/loader && env -u VIRTUAL_ENV GOLD_WEATHER_GRID_SEED_VERSION="$(GOLD_WEATHER_GRID_SEED_VERSION)" UV_PROJECT_ENVIRONMENT=/opt/venvs/modules/loader uv run --frozen python gold_cli.py --publication seed:weather_grid --window-start "$(GOLD_WEATHER_GRID_EFFECTIVE_DTTM)"'
	@echo "[gold-postgis] Gold seed bootstrap 완료"

seed-e2e:
	@test -n "$(E2E_STATION_SOURCE_DTTM)" || { \
		echo "[e2e] station source 시각 계산에 실패했습니다: $(E2E_LOGICAL_DTTM)" >&2; \
		exit 2; \
	}
	@$(COMPOSE) exec -T airflow-scheduler sh -lc \
		'cd /workspace/collector && env -u VIRTUAL_ENV UV_PROJECT_ENVIRONMENT=/opt/venvs/modules/collector uv run --frozen python main.py --source bike_station_realtime --window-start "$(E2E_STATION_SOURCE_DTTM)"'
	@$(COMPOSE) exec -T airflow-scheduler sh -lc \
		'cd /workspace/loader && env -u VIRTUAL_ENV LOCAL_E2E_ALLOW_FIXTURE=1 uv run --frozen python local_e2e.py seed --logical-dttm "$(E2E_LOGICAL_DTTM)"'

e2e-preflight:
	@$(COMPOSE) exec -T airflow-scheduler sh -lc \
		'cd /workspace/loader && env -u VIRTUAL_ENV LOCAL_E2E_ALLOW_FIXTURE=1 uv run --frozen python local_e2e.py check --logical-dttm "$(E2E_LOGICAL_DTTM)"'

e2e-smoke:
	@python3 ops/e2e_smoke.py

# ===========================================================================
# 운영 배포 (AWS)
#
# 실행 위치가 둘로 나뉜다:
#   deploy-*  → 상시 EC2 안에서 실행 (SSM 세션으로 접속한 뒤)
#   그 외      → 로컬에서 실행 (terraform 출력과 aws CLI 사용)
#
# 로컬 타겟은 MFA 세션이 필요하다. AWS_PROFILE=mfa를 지정하거나 export 해둘 것.
# ===========================================================================

TF                 = terraform -chdir=terraform
PROD_ENV          ?= /opt/app/.env
PROD_COMPOSE       = docker compose --env-file $(PROD_ENV) -f ops/compose/docker-compose.prod.yml

# psql 컨테이너는 debian 기반을 쓴다 — alpine에는 bash가 없는데 우리 스크립트가
# bash 문법(`[[ ]]`, 배열, here-string)을 쓴다. postgres:16은 multi-arch라 Graviton에서도 돈다.
PSQL_IMAGE        ?= postgres:16

# SSM 대신 SSH를 쓴다(계정 정책상 SSM 전면 거부). 키페어는 terraform이 만들지 않고
# `aws ec2 create-key-pair`로 AWS가 직접 발급한다(terraform/variables.tf의
# ssh_key_name 참고) — 그 결과물이 이 개인키 파일이다.
SSH_KEY           ?= ~/.ssh/gng-ubd-admin.pem

EMR_STAGE          = .emr-stage
EMR_RELEASE       ?= emr-7.9.0
EMR_INSTANCE_TYPE ?= m5.xlarge
EMR_INSTANCE_COUNT?= 3

.PHONY: deploy-env deploy-secrets deploy-db-bootstrap deploy-db-check deploy-seed-models \
        deploy-up deploy-down deploy-ps deploy-logs deploy-restart deploy-resync deploy-smoke \
        train-start train-stop train-status tunnel-airflow tunnel-mlflow \
        ssh-app ssh-train allow-my-ip \
        emr-package emr-features emr-status

# --- 상시 EC2 안에서 실행 ---

# S3(SSE-KMS)의 설정 객체를 내려받아 /opt/app/.env를 만든다. 설정을 바꾼 뒤 다시 실행한다.
deploy-env:
	@S3_BUCKET=$${S3_BUCKET:-$$(grep -m1 '^S3_BUCKET=' $(PROD_ENV) 2>/dev/null | cut -d= -f2)}; \
	if [ -z "$$S3_BUCKET" ]; then \
		echo "S3_BUCKET을 알 수 없습니다. S3_BUCKET=<버킷> make deploy-env 로 실행하세요." >&2; exit 1; \
	fi; \
	S3_BUCKET="$$S3_BUCKET" bash ops/deploy/render_env.sh

# 최초 1회. DB 3개 생성 + Gold PostGIS baseline 적용. PostGIS 3.4가 없으면 exit 78로 멈춘다.
deploy-db-bootstrap:
	@set -a; . $(PROD_ENV); set +a; \
	docker run --rm \
	  -e PGHOST -e PGPORT -e PGPASSWORD -e PGSSLMODE \
	  -e POSTGRES_USER -e POSTGRES_APP_DB -e POSTGRES_AIRFLOW_DB -e POSTGRES_MLFLOW_DB \
	  -e GOLD_SCHEMA_FILE=/opt/gold/target-schema.sql \
	  -v "$(PWD)/ops/postgres:/opt/scripts:ro" \
	  -v "$(PWD)/docs/gold/target-schema.sql:/opt/gold/target-schema.sql:ro" \
	  $(PSQL_IMAGE) bash /opt/scripts/bootstrap_rds.sh

# 로컬 compose의 postgres-schema-check와 같은 스크립트를 RDS 대상으로 재사용한다.
# 관계 10 / 함수 18 / 트리거 35 / GiST 3 / ACL을 전부 확인한다.
deploy-db-check:
	@set -a; . $(PROD_ENV); set +a; \
	docker run --rm \
	  -e PGHOST -e PGPORT -e PGPASSWORD -e PGSSLMODE \
	  -e POSTGRES_USER -e POSTGRES_APP_DB -e POSTGRES_AIRFLOW_DB \
	  -e GOLD_SCHEMA_CHECK_FILE=/opt/scripts/check_gold_schema.sql \
	  -v "$(PWD)/ops/postgres:/opt/scripts:ro" \
	  $(PSQL_IMAGE) bash /opt/scripts/check_gold_schema.sh

# 최초 1회. 로컬 compose의 minio-init이 하던 일을 대신한다 — 빠뜨리면 ml/inference가
# champion 포인터를 못 찾아 조용히 실패한다.
deploy-seed-models:
	@set -a; . $(PROD_ENV); set +a; \
	aws s3 sync models/ "s3://$$S3_BUCKET/$${MODELS_PREFIX:-models}/"

deploy-up:
	$(PROD_COMPOSE) up -d --build

deploy-down:
	$(PROD_COMPOSE) down

deploy-ps:
	$(PROD_COMPOSE) ps

deploy-logs:
	$(PROD_COMPOSE) logs -f --tail=200

deploy-restart:
	$(PROD_COMPOSE) restart

# bind mount 방식이라 코드는 git pull만으로 반영되지만 **의존성은 아니다**.
# uv.lock이 바뀐 커밋을 받았으면 반드시 실행할 것.
deploy-resync:
	$(PROD_COMPOSE) run --rm airflow-init

deploy-smoke:
	@set -e; \
	echo "[smoke] web";        curl -fsS -o /dev/null localhost/ && echo "  ok"; \
	echo "[smoke] api health"; curl -fsS localhost/api/healthz && echo; \
	echo "[smoke] api ready";  curl -fsS localhost/api/readyz  && echo; \
	echo "[smoke] stations";   curl -fsS localhost/api/stations | head -c 200; echo; \
	echo "[smoke] airflow";    curl -fsS -o /dev/null -w '  http %{http_code}\n' localhost:8080/

# --- 로컬에서 실행 ---

# SSM은 이 계정에서 전면 거부다(StartSession·SendCommand·DescribeInstanceInformation).
# SSH가 유일한 접속 수단이라 22만 admin_cidrs로 열고, UI는 로컬 포트 포워딩으로 본다.
ssh-app:
	@ssh -i $(SSH_KEY) ec2-user@$$($(TF) output -raw app_public_ip)

# 학습 EC2는 인터넷에 22를 열지 않는다. 상시 EC2를 bastion으로 경유한다.
# ProxyJump는 각 홉 인증을 로컬에서 하므로 개인키를 bastion에 두지 않아도 된다.
ssh-train:
	@ssh -i $(SSH_KEY) -J ec2-user@$$($(TF) output -raw app_public_ip) \
	  ec2-user@$$($(TF) output -raw train_private_ip)

# 8080/5000을 SG에 열지 않고 SSH 터널로 본다. 종료는 Ctrl+C.
tunnel-airflow:
	@echo "http://localhost:8080 에서 Airflow UI (종료: Ctrl+C)"; \
	ssh -i $(SSH_KEY) -N -L 8080:localhost:8080 ec2-user@$$($(TF) output -raw app_public_ip)

tunnel-mlflow:
	@echo "http://localhost:5000 에서 MLflow UI (종료: Ctrl+C)"; \
	ssh -i $(SSH_KEY) -N -L 5000:localhost:5000 ec2-user@$$($(TF) output -raw app_public_ip)

# config/prod.env(terraform 산출물)에는 API 키를 안 넣는다 — 이 타겟이 그 나머지를
# 채운다. 값은 SEOUL_OPENAPI_KEY/KMA_APIHUB_KEY 환경변수로 주거나, 생략하면 로컬
# .env(레포 루트)에서 읽는다. 평문 파일은 업로드 후 즉시 지운다.
deploy-secrets:
	@S3_BUCKET=$${S3_BUCKET:?S3_BUCKET을 알 수 없습니다. S3_BUCKET=<버킷> make deploy-secrets 로 실행하세요.}; \
	SEOUL_OPENAPI_KEY=$${SEOUL_OPENAPI_KEY:-$$(grep -m1 '^SEOUL_OPENAPI_KEY=' .env 2>/dev/null | cut -d= -f2-)}; \
	KMA_APIHUB_KEY=$${KMA_APIHUB_KEY:-$$(grep -m1 '^KMA_APIHUB_KEY=' .env 2>/dev/null | cut -d= -f2-)}; \
	if [ -z "$$SEOUL_OPENAPI_KEY" ] || [ -z "$$KMA_APIHUB_KEY" ]; then \
		echo "SEOUL_OPENAPI_KEY/KMA_APIHUB_KEY를 찾을 수 없습니다. 환경변수로 주거나 .env에 채워두세요." >&2; exit 1; \
	fi; \
	tmp=$$(mktemp); \
	trap 'rm -f "$$tmp"' EXIT; \
	printf 'SEOUL_OPENAPI_KEY=%s\nKMA_APIHUB_KEY=%s\n' "$$SEOUL_OPENAPI_KEY" "$$KMA_APIHUB_KEY" > "$$tmp"; \
	aws s3 cp "$$tmp" "s3://$$S3_BUCKET/config/secrets.env"

# 접속 IP가 바뀌었을 때. 현재 공인 IP로 admin_cidrs를 다시 쓰고 SG 규칙만 갱신한다(10초 내외).
# 자기 공인 IP를 admin_cidrs에 **추가**한다. 덮어쓰면 팀원 한 명이 실행할 때마다
# 나머지 전원이 SSH에서 끊긴다.
#
# -target은 반드시 아래 세 규칙 리소스여야 한다. 규칙은 aws_security_group 안에
# 인라인으로 있지 않고 for_each = toset(var.admin_cidrs)인 별도
# aws_vpc_security_group_ingress_rule이다(terraform/network.tf 참고). 그래서
# -target=aws_security_group.app은 규칙을 하나도 포함하지 않아 terraform이
# "No changes"로 끝나고, -target=aws_security_group.train은 SG 교체를 유발해
# app SG의 5000번 규칙이 그걸 참조하는 탓에 DependencyViolation으로 실패한다
# (2026-08-22 실측, 둘 다 겪었다). 전체 apply도 금지다 — 의도적으로 제외해둔
# 학습 EC2까지 만들어버린다.
allow-my-ip:
	@IP=$$(curl -fsS https://checkip.amazonaws.com | tr -d '\n'); \
	$(MAKE) --no-print-directory allow-ip IP="$$IP"

# 다른 사람의 IP를 대신 열어줄 때 쓴다. 팀원이 AWS 자격증명 없이도 접속할 수 있게
# 하려면 관리자가 이 타깃으로 추가한다.
allow-ip:
	@test -n "$(IP)" || { echo "IP=<공인 IP> 를 지정하세요." >&2; exit 2; }
	@python3 ops/deploy/merge_admin_cidrs.py "$(IP)/32"
	@$(TF) apply -auto-approve \
	  -target='aws_vpc_security_group_ingress_rule.app_ssh' \
	  -target='aws_vpc_security_group_ingress_rule.app_airflow_ui' \
	  -target='aws_vpc_security_group_ingress_rule.app_mlflow_ui'

# 더 이상 접속하지 않는 IP를 목록에서 뺀다. 열어둔 채 방치하지 않기 위한 짝이다.
revoke-ip:
	@test -n "$(IP)" || { echo "IP=<공인 IP> 를 지정하세요." >&2; exit 2; }
	@python3 ops/deploy/merge_admin_cidrs.py --remove "$(IP)/32"
	@$(TF) apply -auto-approve \
	  -target='aws_vpc_security_group_ingress_rule.app_ssh' \
	  -target='aws_vpc_security_group_ingress_rule.app_airflow_ui' \
	  -target='aws_vpc_security_group_ingress_rule.app_mlflow_ui'

train-start:
	@aws ec2 start-instances --instance-ids $$($(TF) output -raw train_instance_id)

# 학습이 끝나면 반드시 실행할 것. 정지 중에는 EBS 요금만 나간다.
train-stop:
	@aws ec2 stop-instances --instance-ids $$($(TF) output -raw train_instance_id)

train-status:
	@aws ec2 describe-instances --instance-ids $$($(TF) output -raw train_instance_id) \
	  --query 'Reservations[0].Instances[0].[InstanceId,InstanceType,State.Name]' --output text

# EMR 노드가 PYTHONPATH로 쓸 레포 패키지를 묶어 올린다.
# core/ml_core/feature_engine 세 패키지 루트가 tar 최상위에 오도록 배치한다.
emr-package:
	@rm -rf $(EMR_STAGE) && mkdir -p $(EMR_STAGE)
	@cp -R libs/core/src/core   $(EMR_STAGE)/core
	@cp -R libs/ml_core         $(EMR_STAGE)/ml_core
	@cp -R ml/feature_engine    $(EMR_STAGE)/feature_engine
	@find $(EMR_STAGE) \( -name '.venv' -o -name '__pycache__' -o -name '.ruff_cache' \
	    -o -name 'tests' -o -name '.pytest_cache' \) -prune -exec rm -rf {} + 2>/dev/null || true
	@tar -czf $(EMR_STAGE)/pyfiles.tar.gz -C $(EMR_STAGE) core ml_core feature_engine
	@set -a; . $(PROD_ENV) 2>/dev/null || true; set +a; \
	BUCKET=$${S3_BUCKET:-$$($(TF) output -raw s3_bucket)}; \
	aws s3 cp $(EMR_STAGE)/pyfiles.tar.gz "s3://$$BUCKET/emr/pyfiles.tar.gz"; \
	aws s3 cp ops/emr/bootstrap.sh        "s3://$$BUCKET/emr/bootstrap.sh"; \
	echo "업로드 완료: s3://$$BUCKET/emr/"
	@rm -rf $(EMR_STAGE)

# transient 클러스터를 띄워 피처마트 2단계를 돌리고 스스로 종료시킨다.
# --auto-terminate가 없으면 잡이 끝나도 클러스터가 계속 과금된다.
# for-use-with-amazon-emr-managed-policies 태그는 AmazonEMRServicePolicy_v2의 요구사항이다.
emr-features:
	@BUCKET=$$($(TF) output -raw s3_bucket); \
	SUBNET=$$($(TF) output -raw subnet_id); \
	SERVICE_ROLE=$$($(TF) output -raw emr_service_role); \
	PROFILE=$$($(TF) output -raw emr_instance_profile); \
	PY=/usr/bin/python3.11; \
	CONF="--conf spark.pyspark.python=$$PY --conf spark.pyspark.driver.python=$$PY \
	      --conf spark.executorEnv.PYTHONPATH=/opt/gng \
	      --conf spark.yarn.appMasterEnv.PYTHONPATH=/opt/gng"; \
	aws emr create-cluster \
	  --name "$${EMR_NAME:-gng-ubd-features}" \
	  --release-label $(EMR_RELEASE) \
	  --applications Name=Spark \
	  --log-uri "s3://$$BUCKET/emr/logs/" \
	  --service-role "$$SERVICE_ROLE" \
	  --ec2-attributes SubnetId=$$SUBNET,InstanceProfile=$$PROFILE \
	  --instance-type $(EMR_INSTANCE_TYPE) --instance-count $(EMR_INSTANCE_COUNT) \
	  --bootstrap-actions Path="s3://$$BUCKET/emr/bootstrap.sh",Args=["$$BUCKET"] \
	  --tags for-use-with-amazon-emr-managed-policies=true \
	  --auto-terminate \
	  --steps \
	    Type=Spark,Name=run_pipeline,ActionOnFailure=TERMINATE_CLUSTER,Args=[--deploy-mode,cluster,$$CONF,/opt/gng/feature_engine/spark/run_pipeline.py] \
	    Type=Spark,Name=multi_horizon,ActionOnFailure=TERMINATE_CLUSTER,Args=[--deploy-mode,cluster,$$CONF,/opt/gng/feature_engine/spark/build_multi_horizon_features.py]

# 잡이 끝난 뒤 클러스터가 정말 사라졌는지 확인한다. 남아 있으면 계속 과금된다.
emr-status:
	@aws emr list-clusters --active --query 'Clusters[].[Id,Name,Status.State]' --output text; \
	echo "(출력이 없으면 활성 클러스터 없음)"
