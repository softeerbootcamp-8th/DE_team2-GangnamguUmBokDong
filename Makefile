PROJECTS := collector apps/api airflow ml/inference ml/training ml/feature_engine libs/core libs/ml_core normalizer nowcaster loader rebalance
LOCAL_TEST_PROJECTS := collector apps/api ml/inference ml/training ml/feature_engine libs/core libs/ml_core normalizer nowcaster loader rebalance
CI_TEST_PROJECTS := collector apps/api ml/inference libs/core libs/ml_core normalizer nowcaster loader rebalance
CI_UNIT_PROJECTS := collector apps/api ml/inference libs/core libs/ml_core normalizer nowcaster rebalance
CI_INTEGRATION_PROJECTS := loader

PLATFORM_COMPOSE := $(shell bash ops/compose/platform_args.sh)
COMPOSE = docker compose $(if $(wildcard .env),--env-file .env,) -f ops/compose/docker-compose.yml $(PLATFORM_COMPOSE)

.PHONY: sync-all sync-ci-unit lint test-gold-bootstrap test-gold-transition-available test test-ci test-ci-unit test-ci-integration bootstrap up down logs ps seed seed-e2e e2e-preflight e2e-smoke

E2E_LOGICAL_DTTM ?= $(shell TZ=Asia/Seoul date '+%Y-%m-%dT%H:%M:00+09:00' | awk -F: '{ printf "%s:%02d:00+09:00\n", $$1, int($$2 / 5) * 5 }')
E2E_STATION_SOURCE_DTTM ?= $(shell python3 ops/e2e_time.py station-source '$(E2E_LOGICAL_DTTM)')

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
