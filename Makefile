SHELL := /bin/bash
.DEFAULT_GOAL := help

# Load .env (if present) so make targets and scripts share config.
ifneq (,$(wildcard .env))
include .env
export
endif

VENV := .venv
PY := $(VENV)/bin/python

.PHONY: help venv topics list-topics producer consume-test bq-setup consumer dbt-debug dbt-run dbt-test dbt-build dashboard dashboard-prod

PYRUN := PYTHONPATH=src $(PY)
DBT := PYTHONPATH=src $(VENV)/bin/dbt
DBT_DIRS := --project-dir dbt --profiles-dir dbt

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

venv: ## Create venv and install Python deps
	python3 -m venv $(VENV)
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements.txt

topics: ## Create per-ticker topics on Confluent Cloud (idempotent)
	$(PYRUN) -m crypto_pipeline.admin create

list-topics: ## List topics on the Confluent Cloud cluster
	$(PYRUN) -m crypto_pipeline.admin list

producer: ## Run the Binance -> Kafka producer
	PYTHONPATH=src $(PY) -m crypto_pipeline.producer

bq-setup: ## Create the BigQuery raw dataset + ticks table (idempotent)
	PYTHONPATH=src $(PY) -m crypto_pipeline.bq

consumer: ## Run the Kafka -> BigQuery consumer
	PYTHONPATH=src $(PY) -m crypto_pipeline.consumer

dbt-debug: ## Check dbt connection + config
	$(DBT) debug $(DBT_DIRS)

dbt-run: ## Build all dbt models (staging + marts)
	$(DBT) run $(DBT_DIRS)

dbt-test: ## Run dbt data tests
	$(DBT) test $(DBT_DIRS)

dbt-build: ## Run + test dbt models in one pass
	$(DBT) build $(DBT_DIRS)

dashboard: ## Run the Dash dashboard locally (http://localhost:8051)
	PYTHONPATH=src $(PY) -m crypto_pipeline.dashboard.app

dashboard-prod: ## Run the dashboard via gunicorn (production server)
	$(VENV)/bin/gunicorn wsgi:server --bind 0.0.0.0:8051 --workers 2 --threads 4

consume-test: ## Peek messages from a topic to verify (TOPIC=ticks.btcusdt)
	$(PYRUN) -m crypto_pipeline.admin peek $(or $(TOPIC),ticks.btcusdt) 5
