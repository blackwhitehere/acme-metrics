# acme-metrics
# Run `just --list` to see available commands

# Default recipe
default:
    @just --list

# Make local python env in-sync with lock file
sync-py-env:
    uv sync --all-extras --dev

# Run all tests
test:
    uv run pytest tests/ -v

# Lint code
lint:
    uv run ruff check src/ tests/

# Format code
format:
    uv run ruff format src/ tests/

# Fix linting issues
fix:
    uv run ruff check src/ tests/ --fix

# Install dependencies
install:
    uv sync

# Clean generated files
clean:
    rm -rf .pytest_cache __pycache__ .ruff_cache
    find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find . -type f -name "*.pyc" -delete 2>/dev/null || true

# Launch demo: Streamlit dashboard showcasing metrics visualization
demo:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Launching metrics demo dashboard..."
    uv run python -m streamlit run src/acme_metrics/demo_app.py \
        --server.address localhost \
        --browser.gatherUsageStats false

# Build and serve docs in dev mode
docs:
    cd docs && uv run mkdocs serve
