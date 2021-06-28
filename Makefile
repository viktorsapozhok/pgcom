DOCKER = docker-compose -f ./docker-compose.yml
PSQL = psql -h postgresql -p 5432 -U postgres

.PHONY: help
help:
	@echo 'Usage: make COMMAND'
	@echo
	@echo '  pgcom build utilities'
	@echo
	@echo 'Commands:'
	@echo '  build      Build docker image.'
	@echo '  rebuild    Force docker image rebuild by passing --no-cache.'
	@echo '  service    Run container as a background service.'
	@echo '  login      Run container as a service and attach to it.'
	@echo '  test       Run tox in a container.'
	@echo '  clean      Remove docker image.'


all: build test clean

.PHONY: help
build:
	$(DOCKER) build pgcom

.PHONY: help
rebuild:
	$(DOCKER) build --no-cache pgcom

.PHONY: help
service:
	$(DOCKER) up -V -d postgresql
	sleep 3
	$(DOCKER) run pgcom $(PSQL) -c "CREATE DATABASE test"
	$(DOCKER) run pgcom $(PSQL) -d test -c "CREATE SCHEMA model"

.PHONY: help
login: service
	$(DOCKER) run pgcom bash

.PHONY: help
test: service
	$(DOCKER) run pgcom tox

.PHONY: help
clean:
	$(DOCKER) down
