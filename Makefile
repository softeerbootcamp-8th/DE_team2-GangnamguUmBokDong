PROJECTS := collector apps/api airflow ml/inference ml/training ml/feature_engine libs/core libs/ml_core normalizer nowcaster loader rebalance
LOCAL_TEST_PROJECTS := collector apps/api ml/inference ml/training ml/feature_engine libs/core libs/ml_core normalizer nowcaster loader rebalance
CI_TEST_PROJECTS := collector apps/api libs/core libs/ml_core normalizer nowcaster loader rebalance
CI_UNIT_PROJECTS := collector apps/api libs/core libs/ml_core normalizer nowcaster rebalance
CI_INTEGRATION_PROJECTS := loader

COMPOSE = docker compose $(if $(wildcard .env),--env-file .env,) -f ops/compose/docker-compose.yml

.PHONY: sync-all sync-ci-unit lint test test-ci test-ci-unit test-ci-integration bootstrap up down logs ps seed

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

test:
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

test-ci-unit:
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
	@$(COMPOSE) up -d --build || { \
		$(COMPOSE) logs --no-color airflow-init postgres-schema-init; \
		exit 1; \
	}

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

seed:
	cd apps/api && uv run python seed_gold.py
