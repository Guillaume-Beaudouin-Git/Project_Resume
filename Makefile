# =============================================================================
# Arkéa Quant — Makefile
# =============================================================================
.PHONY: help install install-dev lint test build-data backtest report clean

PYTHON  ?= python
ARKEA   ?= $(PYTHON) -m arkea_quant.cli
CONFIG  ?= configs/strategy.yaml

help:          ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
install:       ## Install package (editable)
	pip install -e .

install-dev:   ## Install package + dev extras
	pip install -e ".[dev]"

# ---------------------------------------------------------------------------
# Quality
# ---------------------------------------------------------------------------
lint:          ## Run ruff linter
	ruff check src/ tests/

format:        ## Auto-format with ruff
	ruff format src/ tests/

typecheck:     ## Run mypy
	mypy src/

test:          ## Run pytest with coverage
	pytest tests/ --cov=arkea_quant --cov-report=term-missing

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
build-data:    ## Fetch & clean market data → data/processed/
	$(ARKEA) build-data --config configs/data_sources.yaml

backtest:      ## Run full backtest → reports/
	$(ARKEA) backtest --config $(CONFIG)

report:        ## Generate HTML tearsheet from last backtest results
	$(ARKEA) report --config $(CONFIG)

pipeline:      ## Run full pipeline: build-data → backtest → report
	$(MAKE) build-data
	$(MAKE) backtest
	$(MAKE) report

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
clean:         ## Remove generated artifacts (keep raw data)
	rm -rf data/interim/* data/processed/*
	rm -rf reports/figures/* reports/tearsheets/*
	rm -rf .pytest_cache __pycache__ src/**/__pycache__
	find . -name "*.pyc" -delete

clean-all:     ## Remove ALL generated files including raw data
	$(MAKE) clean
	rm -rf data/raw/*
