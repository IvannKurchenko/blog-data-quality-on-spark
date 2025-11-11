# Quality-of-life commands for managing the uv project.

.PHONY: help env setup install sync add-spark run tests clean

setup:
	@uv venv

lock:
	@uv lock

install:
	@uv sync

clean: ## Remove uv-managed artifacts
	rm -rf .venv uv.lock __pycache__
