PROJECT := $(notdir $(CURDIR))

.PHONY: help check test \
	sample-release-fixture sample-release-check \
	frame-fixture frame-check sample-v2-fixture sample-v2-check smoke

FIXTURE_SOURCE := fixtures/cpv2010_valid
LEGACY_FIXTURE_RELEASE_ROOT ?= /tmp/samplerCensoARG-fixture-release-v1
FRAME_FIXTURE_ROOT ?= /tmp/samplerCensoARG-fixture-frame
SAMPLE_V2_FIXTURE_ROOT ?= /tmp/samplerCensoARG-fixture-sample-v2
TARGET_FIXTURE ?= /tmp/samplerCensoARG-target.csv

help:
	@echo "Project: $(PROJECT)"
	@echo ""
	@echo "Primary targets:"
	@echo "  make test              - run the Python test suite"
	@echo "  make frame-fixture     - build CPV-2010 fixture as research.census-frame/v1"
	@echo "  make frame-check       - deep-check the prepared fixture frame"
	@echo "  make sample-v2-fixture - sample the fixture through the vintage-neutral v2 path"
	@echo "  make sample-v2-check   - validate the generated v2 release"
	@echo "  make check             - run tests + frame/v2 fixture acceptance"
	@echo ""
	@echo "Compatibility targets:"
	@echo "  make sample-release-fixture - build the historical v1 fixture release"
	@echo "  make sample-release-check RELEASE_DIR=..."

test:
	python -m pytest -q

# Historical governed release retained as a compatibility oracle.
sample-release-fixture:
	rm -rf "$(LEGACY_FIXTURE_RELEASE_ROOT)"
	python -m censo_sampler.cli release --databasepath "$(FIXTURE_SOURCE)" --geography "$(FIXTURE_SOURCE)/GEOGRAPHY.csv" --fraction 0.1 --seed 20260804 --analysis-period 2024-Q1 --name FIXTURE --weight-policy legacy_department_projection_candidate --output-root "$(LEGACY_FIXTURE_RELEASE_ROOT)" --max-households 20

sample-release-check:
	test -n "$(RELEASE_DIR)"
	python -m censo_sampler.cli check-release "$(RELEASE_DIR)"

frame-fixture:
	rm -rf "$(FRAME_FIXTURE_ROOT)"
	python -m censo_sampler.frontdoor frame build-2010 --databasepath "$(FIXTURE_SOURCE)" --geography "$(FIXTURE_SOURCE)/GEOGRAPHY.csv" --output-root "$(FRAME_FIXTURE_ROOT)"

frame-check: frame-fixture
	FRAME_DIR="$$(find "$(FRAME_FIXTURE_ROOT)" -mindepth 1 -maxdepth 1 -type d | head -1)"; \
	test -n "$$FRAME_DIR"; \
	python -m censo_sampler.frontdoor frame check "$$FRAME_DIR"

sample-v2-fixture: frame-fixture
	rm -rf "$(SAMPLE_V2_FIXTURE_ROOT)"
	printf '%s\n' \
	  'department_2010_id,target_year,target_person_mass' \
	  '02001,2024,4' \
	  '50007,2024,2' \
	  '90084,2024,3' \
	  '94008,2024,1' > "$(TARGET_FIXTURE)"
	FRAME_DIR="$$(find "$(FRAME_FIXTURE_ROOT)" -mindepth 1 -maxdepth 1 -type d | head -1)"; \
	python -m censo_sampler.frontdoor sample --frame "$$FRAME_DIR" --target-population "$(TARGET_FIXTURE)" --target-year 2024 --fraction 0.5 --seed 20260831 --materialize full-payload --output-root "$(SAMPLE_V2_FIXTURE_ROOT)"

sample-v2-check: sample-v2-fixture
	RELEASE_DIR="$$(find "$(SAMPLE_V2_FIXTURE_ROOT)" -mindepth 1 -maxdepth 1 -type d | head -1)"; \
	test -n "$$RELEASE_DIR"; \
	python -m censo_sampler.frontdoor check-release-v2 "$$RELEASE_DIR"

check: test frame-check sample-v2-check

smoke: check
