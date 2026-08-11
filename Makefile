COMPOSE = docker compose --env-file .env -f ops/compose/docker-compose.yml

.PHONY: bootstrap up down logs ps

bootstrap:
	./ops/bootstrap/bootstrap.sh

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps
