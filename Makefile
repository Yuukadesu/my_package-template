.PHONY: help install install-dev test test-cov lint format clean build

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: ## Run tests
	pytest

test-cov: ## Run tests with coverage
	pytest --cov=. --cov-report=html --cov-report=term-missing

lint: ## Run linting checks
	flake8 .
	black --check .
	isort --check-only .
	mypy . --ignore-missing-imports

format: ## Format code
	black .
	isort .

clean: ## Clean up build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -type d -name __pycache__ -delete
	find . -type f -name "*.pyc" -delete


check: lint test ## Run linting and tests

pre-commit: format lint test ## Run all checks (format, lint, test)

IMG ?= time-function:latest

build-image:
	docker build --push -t $(IMG) .

PLATFORMS ?= linux/arm64,linux/amd64

docker-buildx:
	docker buildx build --platform $(PLATFORMS) --push -t $(IMG) .