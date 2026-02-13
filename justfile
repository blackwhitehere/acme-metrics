# acme-metrics
# Run `just --list` to see available commands

# Default recipe
default:
    @just --list

# Make local python env in-sync with lock file
sync-py-env:
    uv sync --all-extras --dev

upgrade-py-env:
    #!/bin/bash
    set -euoxv pipefail
    if [ $# -eq 1 ]; then
        uv lock --upgrade-package "$1"
    else
        uv lock --upgrade
    fi
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

# Remove demo fixture data
demo-clean:
    rm -rf src/acme_metrics/_demo_data
    @echo "Demo data cleaned"

# Populate demo fixture data
demo-fixtures:
    uv run python -m acme_metrics.demo --setup

# Launch Streamlit demo UI (requires fixtures)
demo-ui:
    uv run python -m streamlit run src/acme_metrics/demo_app.py \
        --server.address localhost \
        --browser.gatherUsageStats false

# Full demo: clean, populate fixtures, and launch UI
demo: demo-clean demo-fixtures demo-ui

# Build and serve docs in dev mode
docs:
    cd docs && uv run mkdocs serve
