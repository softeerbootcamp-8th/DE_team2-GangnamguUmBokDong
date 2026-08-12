PROJECTS := collector apps/api airflow ml/predict ml/training ml/feature libs/core

.PHONY: sync-all lint test

sync-all:
	@for p in $(PROJECTS); do \
		echo "==> $$p"; \
		(cd $$p && uv sync) || exit 1; \
	done

lint:
	@for p in $(PROJECTS); do \
		echo "==> $$p"; \
		(cd $$p && uvx ruff check .) || exit 1; \
	done

test:
	@for p in $(PROJECTS); do \
		echo "==> $$p"; \
		(cd $$p && uv run --with pytest pytest -q); \
		code=$$?; \
		if [ $$code -ne 0 ] && [ $$code -ne 5 ]; then exit $$code; fi; \
	done
