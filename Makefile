COMPOSE  := docker compose -f docker/docker-compose.yml
PYTHON   := .venv/bin/python
ARGS     ?=

.PHONY: help venv up down reset ps producer consumer dlq test test-unit test-integration

help:
	@echo "make venv       create .venv and install dependencies"
	@echo "make up         start Kafka + Provectus Kafka UI (http://localhost:8080)"
	@echo "make down       stop them (keeps topic data)"
	@echo "make reset      stop, wipe topic data, start fresh"
	@echo "make ps         show container status"
	@echo "make producer   run the producer      e.g. make producer ARGS=\"--count 5\""
	@echo "make consumer   run the consumer      e.g. make consumer ARGS=\"--max-attempts 3\""
	@echo "make dlq        print what is in the Dead Letter Queue"
	@echo "make test       run all tests (integration test needs Kafka up)"
	@echo "make test-unit  run only the tests that need no Kafka"
	@echo "make test-integration  run only the end-to-end test against Kafka"

venv:
	python3 -m venv .venv
	$(PYTHON) -m pip install --quiet --upgrade pip
	$(PYTHON) -m pip install --quiet -r requirements.txt

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v
	$(COMPOSE) up -d

ps:
	$(COMPOSE) ps -a

producer:
	$(PYTHON) -m src.producer $(ARGS)

consumer:
	$(PYTHON) -m src.consumer $(ARGS)

dlq:
	$(PYTHON) -m src.dlq_reader

test:
	$(PYTHON) -m pytest -v

test-unit:
	$(PYTHON) -m pytest -v -m "not integration"

test-integration:
	$(PYTHON) -m pytest -v -m integration
