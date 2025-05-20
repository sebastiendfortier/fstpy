PIXI_CHECK := $(shell command -v pixi 2> /dev/null)
PIXI_CMD := $(if $(PIXI_CHECK),pixi,. ssmuse-sh -p /fs/ssm/eccc/cmd/cmds/apps/pixi/202503/00/pixi_0.41.4_all && pixi)

# Define variables for source command generation
GITLAB_CI_SOURCES := $(shell awk '/^\s*-\s*\.\s*(r\.load\.dot|ssmuse-sh)/ {gsub(/^\s*-\s*\./, ""); gsub(/^\s+|\s+$$/, ""); printf ". %s && ", $$0}' .gitlab-ci.yml)

.PHONY: test lint lint-fix format build doc conda-build conda-upload test-all clean help

# Development targets
test:
	@echo "********* Running test target *********"
	$(PIXI_CMD) run -e dev test

test-ssm:
	@echo "********* Running test-ssm target *********"
	$(GITLAB_CI_SOURCES) \
	python -m pytest -vrf

lint:
	@echo "********* Running lint target *********"
	$(PIXI_CMD) run -e dev lint

lint-fix:
	@echo "********* Running lint-fix target *********"
	$(PIXI_CMD) run -e dev lint-fix

format:
	@echo "********* Running format target *********"
	$(PIXI_CMD) run -e dev format

build:
	@echo "********* Running build target *********"
	$(PIXI_CMD) run -e dev build

doc:
	@echo "********* Running doc target *********"
	$(PIXI_CMD) run -e dev doc

# Conda package management
conda-build: clean
	@echo "********* Running conda-build target *********"
	$(PIXI_CMD) run -e dev conda-build

conda-upload: 
	@echo "********* Running conda-upload target *********"
	$(PIXI_CMD) run -e dev conda-upload

test-py38: clean
	@echo "********* Testing package with python 3.8 *********"
	cd package_tests/environments && $(PIXI_CMD) run -e py38 tests

test-py313: clean
	@echo "********* Testing package with python 3.13 *********"
	cd package_tests/environments && $(PIXI_CMD) run -e py313 tests

test-both: test-py38 test-py313


# Default target
all: lint test doc
	@echo "********* Running default target (lint test doc) *********"

# Clean target
clean:
	@echo "********* Running clean target *********"
	scripts/clean.sh
	

help:
	@echo "Available targets:"
	@echo "  test: Run tests"
	@echo "  lint: Run linting"
	@echo "  lint-fix: Run linting and fix issues"
	@echo "  format: Run code formatting"
	@echo "  build: Build the package"
	@echo "  doc: Build documentation"
	@echo "  conda-build: Build conda package"
	@echo "  conda-upload: Upload conda package"
	@echo "  test-py38: Test with Python 3.8"
	@echo "  test-py313: Test with Python 3.13"
	@echo "  test-both: Test with Python 3.8 and 3.13"
	@echo "  all: Run lint, test, and doc targets"
	@echo "  clean: Clean up build artifacts"
	@echo "  help: Show this help message"
