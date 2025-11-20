.PHONY: help install lint format type-check test clean all

help: ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies with Poetry
	poetry install

lint: ## Run ruff linter
	poetry run ruff check .

lint-fix: ## Run ruff linter and auto-fix issues
	poetry run ruff check --fix .

format: ## Format code with ruff
	poetry run ruff format .

format-check: ## Check if code is formatted correctly
	poetry run ruff format --check .

type-check: ## Run pyright type checker
	poetry run pyright

test: ## Run tests with pytest
	poetry run pytest

test-cov: ## Run tests with coverage report
	poetry run pytest --cov=src --cov-report=term-missing --cov-report=html

clean: ## Clean up generated files
	rm -rf .ruff_cache
	rm -rf __pycache__
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -rf htmlcov
	rm -rf .pyright
	find . -type d -name __pycache__ -exec rm -r {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete

all: lint format-check type-check test ## Run lint, format check, type check, and tests

backfill:
	poetry run python src/data/blocks/backfill.py
	poetry run python src/data/relays/backfill.py
	poetry run python src/data/builders/backfill.py
	poetry run python src/data/builders/backfill_extra_builders.py
	poetry run python src/data/adjustments/backfill.py
	poetry run python src/analysis/backfill.py

live:
	poetry run python src/live.py
