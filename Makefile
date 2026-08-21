PROJECTS := collector apps/api airflow ml/inference ml/training ml/feature_engine libs/core libs/ml_core normalizer nowcaster loader rebalance
LOCAL_TEST_PROJECTS := collector apps/api ml/inference ml/training ml/feature_engine libs/core libs/ml_core normalizer nowcaster loader rebalance
CI_TEST_PROJECTS := collector apps/api ml/inference libs/core libs/ml_core normalizer nowcaster loader rebalance
CI_UNIT_PROJECTS := collector apps/api ml/inference libs/core libs/ml_core normalizer nowcaster rebalance
CI_INTEGRATION_PROJECTS := loader

PLATFORM_COMPOSE := $(shell bash ops/compose/platform_args.sh)
COMPOSE = docker compose $(if $(wildcard .env),--env-file .env,) -f ops/compose/docker-compose.yml $(PLATFORM_COMPOSE)

.PHONY: sync-all sync-ci-unit lint test-gold-bootstrap test-gold-transition-available test test-ci test-ci-unit test-ci-integration bootstrap up down logs ps seed

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

seed:
	@echo "[gold-postgis] make seed는 weather grid seed_version/effective_dttm SSOT 확정 전이라 비활성화되었습니다." >&2
	@echo "[gold-postgis] 승인된 값으로 loader/gold_cli.py의 seed:dispatch_center, seed:weather_grid를 명시적으로 실행하세요." >&2
	@false

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

EMR_STAGE          = .emr-stage
EMR_RELEASE       ?= emr-7.9.0
EMR_INSTANCE_TYPE ?= m5.xlarge
EMR_INSTANCE_COUNT?= 3

.PHONY: deploy-env deploy-db-bootstrap deploy-db-check deploy-seed-models \
        deploy-up deploy-down deploy-ps deploy-logs deploy-restart deploy-resync deploy-smoke \
        train-start train-stop train-status tunnel-airflow tunnel-mlflow session-app \
        emr-package emr-features emr-status

# --- 상시 EC2 안에서 실행 ---

# S3(SSE-KMS)의 설정 객체를 내려받아 /opt/app/.env를 만든다. 설정을 바꾼 뒤 다시 실행한다.
deploy-env:
	@S3_BUCKET=$${S3_BUCKET:-$$(grep -m1 '^S3_BUCKET=' $(PROD_ENV) 2>/dev/null | cut -d= -f2)}; \
	if [ -z "$$S3_BUCKET" ]; then \
		echo "S3_BUCKET을 알 수 없습니다. S3_BUCKET=<버킷> make deploy-env 로 실행하세요." >&2; exit 1; \
	fi; \
	S3_BUCKET="$$S3_BUCKET" bash ops/deploy/render_env.sh

# 최초 1회. DB 3개 생성 + Gold PostGIS baseline 적용. PostGIS 3.5가 없으면 exit 78로 멈춘다.
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

session-app:
	@aws ssm start-session --target $$($(TF) output -raw app_instance_id)

# UI 포트를 열지 않고 접근한다. SG의 admin_cidrs가 비어 있어도 동작한다.
tunnel-airflow:
	@aws ssm start-session --target $$($(TF) output -raw app_instance_id) \
	  --document-name AWS-StartPortForwardingSession \
	  --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}'

tunnel-mlflow:
	@aws ssm start-session --target $$($(TF) output -raw app_instance_id) \
	  --document-name AWS-StartPortForwardingSession \
	  --parameters '{"portNumber":["5000"],"localPortNumber":["5000"]}'

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
