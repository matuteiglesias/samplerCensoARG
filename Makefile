
# Auto-generated stub Makefile.
# Purpose: provide a stable interface for portfolio governance.
# Replace placeholder targets with real commands when ready.

PROJECT := $(notdir $(CURDIR))

.PHONY: help check test sample-release-fixture sample-release-check smoke

FIXTURE_RELEASE_ROOT ?= /tmp/samplerCensoARG-fixture-release
FIXTURE_SOURCE := fixtures/cpv2010_valid

help:
	@echo "Project: $(PROJECT)"
	@echo ""
	@echo "Targets:"
	@echo "  make check                  - offline tests and fixture release validation"
	@echo "  make test                   - unit tests"
	@echo "  make sample-release-fixture - build deterministic synthetic release"
	@echo "  make sample-release-check RELEASE_DIR=..."

test:
	python -m unittest discover -v

sample-release-fixture:
	rm -rf "$(FIXTURE_RELEASE_ROOT)"
	python -m censo_sampler.cli release --databasepath "$(FIXTURE_SOURCE)" --geography "$(FIXTURE_SOURCE)/GEOGRAPHY.csv" --fraction 0.1 --seed 20260804 --analysis-period 2024-Q1 --name FIXTURE --weight-policy legacy_department_projection_candidate --output-root "$(FIXTURE_RELEASE_ROOT)" --max-households 20

sample-release-check:
	test -n "$(RELEASE_DIR)"
	python -m censo_sampler.cli check-release "$(RELEASE_DIR)"

check: test sample-release-fixture
	$(MAKE) sample-release-check RELEASE_DIR="$$(find "$(FIXTURE_RELEASE_ROOT)" -mindepth 1 -maxdepth 1 -type d | head -1)"

smoke: check
