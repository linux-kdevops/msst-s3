# SPDX-License-Identifier: MIT
# Multi-vendor S3 Storage Test Suite (MSST-S3)

# Project version
VERSION = 1
PATCHLEVEL = 0
SUBLEVEL = 0
EXTRAVERSION =

# Kconfig setup (Python-based using kconfiglib)
export KCONFIG_CONFIG := $(CURDIR)/.config
export KCONFIG_YAMLCFG := $(CURDIR)/s3_config.yaml

# Include Python-based kconfig build system
include Makefile.kconfig

# Python setup
PYTHON := python3
PIP := pip3
VENV_DIR := venv

# Test runner
TEST_RUNNER := scripts/test-runner.py

# Output configuration
OUTPUT_DIR := results
TIMESTAMP := $(shell date +%Y%m%d_%H%M%S)

# Ansible configuration
ANSIBLE := ansible-playbook
ANSIBLE_INVENTORY := playbooks/inventory/hosts
ANSIBLE_VERBOSE := $(if $(V),-vvv,)

# Extract configuration values (remove quotes)
ifdef CONFIG_S3_ENDPOINT_URL
S3_ENDPOINT := $(subst ",,$(CONFIG_S3_ENDPOINT_URL))
endif

ifdef CONFIG_S3_BUCKET_PREFIX
S3_BUCKET_PREFIX := $(subst ",,$(CONFIG_S3_BUCKET_PREFIX))
endif

ifdef CONFIG_OUTPUT_DIR
OUTPUT_DIR := $(subst ",,$(CONFIG_OUTPUT_DIR))
endif

# Build S3 test arguments for YAML generation
S3_ARGS :=
ifdef CONFIG_S3_ENDPOINT_URL
S3_ARGS += s3_endpoint='$(S3_ENDPOINT)'
endif
ifdef CONFIG_S3_ACCESS_KEY
S3_ARGS += s3_access_key='$(subst ",,$(CONFIG_S3_ACCESS_KEY))'
endif
ifdef CONFIG_S3_BUCKET_PREFIX
S3_ARGS += s3_bucket_prefix='$(S3_BUCKET_PREFIX)'
endif

# Extra variables for ansible
EXTRA_VARS_FILE := extra_vars.yaml
WORKFLOW_ARGS := $(S3_ARGS)

# Default target
.PHONY: all
all: config test

# Help target
.PHONY: help
help:
	@echo "MSST-S3: Multi-vendor S3 Storage Test Suite"
	@echo ""
	@echo "Configuration targets:"
	@echo "  make config         - Configure test suite interactively"
	@echo "  make menuconfig     - Interactive menu configuration"
	@echo "  make defconfig      - Use default configuration"
	@echo "  make oldconfig      - Update existing configuration"
	@echo ""
	@echo "Available defconfigs:"
	@for cfg in $(DEFCONFIGS); do \
		echo "  make defconfig-$$cfg"; \
	done
	@echo ""
	@echo "Test targets:"
	@echo "  make test           - Run all enabled tests"
	@echo "  make test-basic     - Run basic tests (001-099)"
	@echo "  make test-multipart - Run multipart tests (100-199)"
	@echo "  make test-versioning- Run versioning tests (200-299)"
	@echo "  make test-acl       - Run ACL tests (300-399)"
	@echo "  make test-encryption- Run encryption tests (400-499)"
	@echo "  make test-lifecycle - Run lifecycle tests (500-599)"
	@echo "  make test-performance - Run performance tests (600-699)"
	@echo "  make test-stress    - Run stress tests (700-799)"
	@echo "  make test TEST=001  - Run specific test"
	@echo "  make test GROUP=acl - Run specific test group"
	@echo ""
	@echo "Ansible targets:"
	@echo "  make ansible-deploy - Deploy test infrastructure"
	@echo "  make ansible-run    - Run tests via Ansible"
	@echo "  make ansible-results- Collect test results"
	@echo "  make ansible-clean  - Clean test infrastructure"
	@echo ""
	@echo "Setup targets:"
	@echo "  make install-deps   - Install Python dependencies"
	@echo "  make venv          - Create Python virtual environment"
	@echo "  make clean         - Clean build artifacts"
	@echo "  make distclean     - Clean everything including config"
	@echo ""
	@echo "Maintenance:"
	@echo "  make check-config   - Validate configuration"
	@echo "  make show-config    - Display current configuration"
	@echo "  make kconfig-help   - Show kconfig target help"

# Configuration targets
.PHONY: config
config: menuconfig

.PHONY: show-config
show-config: check-config
	@echo "Current S3 configuration:"
	@grep -E "^CONFIG_S3_|^CONFIG_TEST_" $(KCONFIG_CONFIG) | sed 's/CONFIG_/  /'

# Generate extra_vars.yaml from kconfig
$(EXTRA_VARS_FILE): $(KCONFIG_CONFIG)
	@echo "Generating $(EXTRA_VARS_FILE) from kconfig..."
	@echo "---" > $(EXTRA_VARS_FILE)
	@echo "# Generated from kconfig - do not edit directly" >> $(EXTRA_VARS_FILE)
	@if [ -f "$(KCONFIG_YAMLCFG)" ]; then \
		cat $(KCONFIG_YAMLCFG) >> $(EXTRA_VARS_FILE); \
	fi
	@for arg in $(WORKFLOW_ARGS); do \
		echo "$$arg" | sed "s/=/: /" >> $(EXTRA_VARS_FILE); \
	done

# Python environment setup
.PHONY: venv
venv:
	@if [ ! -d "$(VENV_DIR)" ]; then \
		echo "Creating Python virtual environment..."; \
		$(PYTHON) -m venv $(VENV_DIR); \
		. $(VENV_DIR)/bin/activate && $(PIP) install --upgrade pip; \
		. $(VENV_DIR)/bin/activate && $(PIP) install -r requirements.txt; \
	else \
		echo "Virtual environment already exists"; \
	fi

# Install dependencies
.PHONY: install-deps
install-deps: requirements.txt
	@echo "Installing Python dependencies..."
	$(PIP) install -r requirements.txt

requirements.txt:
	@echo "Creating requirements.txt..."
	@echo "boto3>=1.26.0" > requirements.txt
	@echo "pytest>=7.0.0" >> requirements.txt
	@echo "pyyaml>=6.0" >> requirements.txt
	@echo "click>=8.0.0" >> requirements.txt
	@echo "tabulate>=0.9.0" >> requirements.txt
	@echo "jsonschema>=4.0.0" >> requirements.txt
	@echo "jinja2>=3.0.0" >> requirements.txt
	@echo "requests>=2.28.0" >> requirements.txt

# Test execution targets
.PHONY: test
test: check-config $(TEST_RUNNER)
	@echo "Running S3 tests..."
	@mkdir -p $(OUTPUT_DIR)
	$(PYTHON) $(TEST_RUNNER) \
		--config $(KCONFIG_YAMLCFG) \
		--output-dir $(OUTPUT_DIR)/$(TIMESTAMP) \
		$(if $(TEST),--test $(TEST),) \
		$(if $(GROUP),--group $(GROUP),) \
		$(if $(V),--verbose,)

.PHONY: test-basic
test-basic: check-config $(TEST_RUNNER)
	@echo "Running basic S3 tests (001-099)..."
	@mkdir -p $(OUTPUT_DIR)
	$(PYTHON) $(TEST_RUNNER) \
		--config $(KCONFIG_YAMLCFG) \
		--output-dir $(OUTPUT_DIR)/$(TIMESTAMP) \
		--group basic \
		$(if $(V),--verbose,)

.PHONY: test-multipart
test-multipart: check-config $(TEST_RUNNER)
	@echo "Running multipart upload tests (100-199)..."
	@mkdir -p $(OUTPUT_DIR)
	$(PYTHON) $(TEST_RUNNER) \
		--config $(KCONFIG_YAMLCFG) \
		--output-dir $(OUTPUT_DIR)/$(TIMESTAMP) \
		--group multipart \
		$(if $(V),--verbose,)

.PHONY: test-versioning
test-versioning: check-config $(TEST_RUNNER)
	@echo "Running versioning tests (200-299)..."
	@mkdir -p $(OUTPUT_DIR)
	$(PYTHON) $(TEST_RUNNER) \
		--config $(KCONFIG_YAMLCFG) \
		--output-dir $(OUTPUT_DIR)/$(TIMESTAMP) \
		--group versioning \
		$(if $(V),--verbose,)

.PHONY: test-acl
test-acl: check-config $(TEST_RUNNER)
	@echo "Running ACL tests (300-399)..."
	@mkdir -p $(OUTPUT_DIR)
	$(PYTHON) $(TEST_RUNNER) \
		--config $(KCONFIG_YAMLCFG) \
		--output-dir $(OUTPUT_DIR)/$(TIMESTAMP) \
		--group acl \
		$(if $(V),--verbose,)

.PHONY: test-encryption
test-encryption: check-config $(TEST_RUNNER)
	@echo "Running encryption tests (400-499)..."
	@mkdir -p $(OUTPUT_DIR)
	$(PYTHON) $(TEST_RUNNER) \
		--config $(KCONFIG_YAMLCFG) \
		--output-dir $(OUTPUT_DIR)/$(TIMESTAMP) \
		--group encryption \
		$(if $(V),--verbose,)

.PHONY: test-lifecycle
test-lifecycle: check-config $(TEST_RUNNER)
	@echo "Running lifecycle tests (500-599)..."
	@mkdir -p $(OUTPUT_DIR)
	$(PYTHON) $(TEST_RUNNER) \
		--config $(KCONFIG_YAMLCFG) \
		--output-dir $(OUTPUT_DIR)/$(TIMESTAMP) \
		--group lifecycle \
		$(if $(V),--verbose,)

.PHONY: test-performance
test-performance: check-config $(TEST_RUNNER)
	@echo "Running performance tests (600-699)..."
	@mkdir -p $(OUTPUT_DIR)
	$(PYTHON) $(TEST_RUNNER) \
		--config $(KCONFIG_YAMLCFG) \
		--output-dir $(OUTPUT_DIR)/$(TIMESTAMP) \
		--group performance \
		$(if $(V),--verbose,)

.PHONY: test-stress
test-stress: check-config $(TEST_RUNNER)
	@echo "Running stress tests (700-799)..."
	@mkdir -p $(OUTPUT_DIR)
	$(PYTHON) $(TEST_RUNNER) \
		--config $(KCONFIG_YAMLCFG) \
		--output-dir $(OUTPUT_DIR)/$(TIMESTAMP) \
		--group stress \
		$(if $(V),--verbose,)

# Ansible integration targets
.PHONY: ansible-deploy
ansible-deploy: $(EXTRA_VARS_FILE)
	@echo "Deploying test infrastructure via Ansible..."
	$(ANSIBLE) $(ANSIBLE_VERBOSE) \
		-i $(ANSIBLE_INVENTORY) \
		playbooks/s3-deploy.yml \
		--extra-vars=@$(EXTRA_VARS_FILE)

.PHONY: ansible-run
ansible-run: $(EXTRA_VARS_FILE)
	@echo "Running tests via Ansible..."
	$(ANSIBLE) $(ANSIBLE_VERBOSE) \
		-i $(ANSIBLE_INVENTORY) \
		playbooks/s3-tests.yml \
		--extra-vars=@$(EXTRA_VARS_FILE)

.PHONY: ansible-results
ansible-results: $(EXTRA_VARS_FILE)
	@echo "Collecting test results via Ansible..."
	$(ANSIBLE) $(ANSIBLE_VERBOSE) \
		-i $(ANSIBLE_INVENTORY) \
		playbooks/s3-results.yml \
		--extra-vars=@$(EXTRA_VARS_FILE)

.PHONY: ansible-clean
ansible-clean: $(EXTRA_VARS_FILE)
	@echo "Cleaning test infrastructure via Ansible..."
	$(ANSIBLE) $(ANSIBLE_VERBOSE) \
		-i $(ANSIBLE_INVENTORY) \
		playbooks/s3-clean.yml \
		--extra-vars=@$(EXTRA_VARS_FILE)

# Include workflow makefiles if enabled
ifdef CONFIG_TEST_PERFORMANCE
-include workflows/performance/Makefile
endif

ifdef CONFIG_TEST_STRESS
-include workflows/stress/Makefile
endif

# Clean targets
.PHONY: clean
clean:
	@echo "Cleaning build artifacts..."
	@rm -f $(EXTRA_VARS_FILE)
	@rm -rf __pycache__ .pytest_cache
	@find . -type f -name "*.pyc" -delete
	@find . -type d -name "__pycache__" -delete
	@echo "Clean complete"

.PHONY: distclean
distclean: clean
	@echo "Removing all generated files..."
	@rm -f .config .config.old
	@rm -f $(KCONFIG_YAMLCFG)
	@rm -rf $(VENV_DIR)
	@rm -rf $(OUTPUT_DIR)
	@echo "Distclean complete"

# Check if test runner exists
$(TEST_RUNNER):
	@echo "Test runner not found. Creating template..."
	@mkdir -p scripts
	@echo '#!/usr/bin/env python3' > $(TEST_RUNNER)
	@echo '# Test runner will be implemented' >> $(TEST_RUNNER)
	@chmod +x $(TEST_RUNNER)

# Development targets
.PHONY: dev-setup
dev-setup: venv config
	@echo "Development environment ready"

.PHONY: ci-test
ci-test: defconfig test
	@echo "CI test complete"

# Style/formatting target
.PHONY: style
style:
	@echo "Running black formatter on Python files..."
	@black scripts/*.py tests/common/*.py 2>/dev/null || true
	@echo "Style formatting complete"

# Docker S3 provider management
.PHONY: docker-up
docker-up:
	@echo "Starting S3 provider containers..."
	@$(PYTHON) scripts/docker-compose-wrapper.py up -d $(PROVIDER)
	@echo "Waiting for services to be ready..."
	@sleep 5
	@echo "S3 providers are ready"

.PHONY: docker-down
docker-down:
	@echo "Stopping S3 provider containers..."
	@$(PYTHON) scripts/docker-compose-wrapper.py down
	@echo "S3 providers stopped"

.PHONY: docker-status
docker-status:
	@echo "S3 provider container status:"
	@$(PYTHON) scripts/docker-compose-wrapper.py ps

.PHONY: docker-logs
docker-logs:
	@$(PYTHON) scripts/docker-compose-wrapper.py logs $(if $(PROVIDER),$(PROVIDER),)

# Start specific S3 providers
.PHONY: docker-minio
docker-minio:
	@$(MAKE) docker-up PROVIDER=minio

.PHONY: docker-localstack
docker-localstack:
	@$(MAKE) docker-up PROVIDER=localstack

.PHONY: docker-garage
docker-garage:
	@$(MAKE) docker-up PROVIDER=garage

.PHONY: docker-seaweedfs
docker-seaweedfs:
	@$(MAKE) docker-up PROVIDER=seaweedfs

.PHONY: docker-ceph
docker-ceph:
	@$(MAKE) docker-up PROVIDER=ceph

# Synthetic data population
.PHONY: populate-data
populate-data:
	@echo "Populating synthetic test data..."
	@$(PYTHON) scripts/populate-data.py --config $(KCONFIG_YAMLCFG)

# Test with Docker provider and data population
.PHONY: test-with-docker
test-with-docker: docker-up populate-data test
	@echo "Docker-based testing complete"

# Include dependency tracking
-include .depend

.PHONY: FORCE
FORCE:
