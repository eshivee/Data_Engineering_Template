# -----------------------------
# Makefile for Medallion Pipeline
# -----------------------------

# Default environment
ENV ?= dev

# Run the pipeline
run:
	@echo "Running Medallion pipeline in $(ENV) environment..."
	python -m medallion_architecture.run_pipeline

# Run tests
test:
	@echo "Running all unit tests..."
	pytest tests/

# Lint code
lint:
	@echo "Linting code with ruff..."
	ruff medallion_architecture tests

# Format code
format:
	@echo "Formatting code with black..."
	black medallion_architecture tests

# Build Docker image
docker-build:
	@echo "Building Docker image..."
	docker build -t medallion-pipeline .

# Clean temporary files
clean:
	@echo "Cleaning temporary files..."
	rm -rf __pycache__ .pytest_cache

# Help command
help:
	@echo "Available commands:"
	@echo "  make run         - Run the pipeline"
	@echo "  make test        - Run all tests"
	@echo "  make lint        - Lint code"
	@echo "  make format      - Format code"
	@echo "  make docker-build- Build Docker image"
	@echo "  make clean       - Remove temp files"