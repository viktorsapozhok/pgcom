# Usage: make COMMAND
#
#  PGCOM build utilities
#
# Commands:
#    help       Show help message.
#    rebuild    Force docker image rebuild by passing --no-cache.
#    build      Build docker image.
#    service    Run container as a background service.
#    login      Run container as a service and attach to it.
#    test       Run tox in container.
#    clean      Remove docker image.
#    prune      Cleanup inactive containers and cache.

DOCKER_COMPOSE = docker-compose -f ./docker-compose.yml
PSQL = psql -h postgresql -p 5432 -U postgres

.PHONY: rebuild build service login clean prune

help:
	@head -13 Makefile

all: build test clean

rebuild:
	$(DOCKER_COMPOSE) build --no-cache pgcom

build:
	$(DOCKER_COMPOSE) build pgcom

service:
	$(DOCKER_COMPOSE) up -V -d postgresql
	sleep 3
	$(DOCKER_COMPOSE) run pgcom $(PSQL) -c "CREATE DATABASE test"
	$(DOCKER_COMPOSE) run pgcom $(PSQL) -d test -c "CREATE SCHEMA model"

login: service
	$(DOCKER_COMPOSE) run pgcom bash

test: service
	$(DOCKER_COMPOSE) run pgcom tox

clean:
	$(DOCKER_COMPOSE) down

prune:
	docker system prune -af
