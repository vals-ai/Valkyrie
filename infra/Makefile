SHELL := /bin/bash

.PHONY: help install style format lint typecheck test venv_check \
	validate-target validate-scope validate-deploy-scope \
	preflight bootstrap synth plan diff deploy deploy-deployment-access \
	diff-deployment-access diff-dns-zone diff-shared diff-tracker diff-worker \
	diff-monitoring diff-all deploy-dns-zone deploy-shared deploy-tracker \
	deploy-worker deploy-monitoring deploy-all hotswap destroy

CDK_VERSION := 2.1117.0
CDK := uv run -- npx --yes aws-cdk@$(CDK_VERSION)
PRODUCTION_ACCOUNT_ID := 613431292675
DEPLOYMENT_REGION := us-east-1

STAGE ?=
AWS_REGION ?=
DEV_ACCOUNT_ID ?=
PROFILE ?=
SCOPE ?=
OUTPUT ?=

EXPECTED_ACCOUNT_ID = $(if $(filter dev,$(STAGE)),$(DEV_ACCOUNT_ID),$(if $(filter prod,$(STAGE)),$(PRODUCTION_ACCOUNT_ID),))
STACK_PREFIX = $(if $(filter dev,$(STAGE)),Valk-Dev-,)

STACKS_deployment-access = $(STACK_PREFIX)DeploymentAccessStack
STACKS_dns-zone = $(STACK_PREFIX)DnsZoneStack
STACKS_shared = $(STACK_PREFIX)SharedStack
STACKS_tracker = $(STACK_PREFIX)TrackerStack
STACKS_worker = $(STACK_PREFIX)WorkerStack
STACKS_monitoring = $(STACK_PREFIX)MonitoringStack
STACKS_all = $(STACKS_shared) $(STACKS_tracker) $(STACKS_worker) $(STACKS_monitoring)
SELECTED_STACKS = $(STACKS_$(SCOPE))

CDK_DEFAULT_ACCOUNT ?= $(EXPECTED_ACCOUNT_ID)
CDK_DEFAULT_REGION ?= $(AWS_REGION)

export STAGE AWS_REGION DEV_ACCOUNT_ID PROFILE EXPECTED_ACCOUNT_ID
export CDK_DEFAULT_ACCOUNT CDK_DEFAULT_REGION

CDK_CONTEXT = -c stage=$(STAGE) -c scope=$(SCOPE)
CDK_PROFILE = $(if $(strip $(PROFILE)),--profile "$(PROFILE)",)
CDK_OUTPUT = $(if $(strip $(OUTPUT)),--output "$(OUTPUT)",)

help:
	@echo "Makefile for Valkyrie infrastructure"
	@echo ""
	@echo "Every AWS operation requires STAGE and AWS_REGION. Dev also requires DEV_ACCOUNT_ID."
	@echo "Set PROFILE to an AWS profile name for local credentials, or leave PROFILE empty for OIDC."
	@echo ""
	@echo "  make preflight STAGE=dev AWS_REGION=us-east-1 DEV_ACCOUNT_ID=... PROFILE="
	@echo "  make bootstrap STAGE=dev AWS_REGION=us-east-1 DEV_ACCOUNT_ID=... PROFILE=..."
	@echo "  make plan SCOPE=<scope> STAGE=<stage> AWS_REGION=us-east-1 ..."
	@echo "  make deploy SCOPE=<scope> STAGE=<stage> AWS_REGION=us-east-1 ..."
	@echo "  make test && make lint && make typecheck"
	@echo ""
	@echo "Scopes: deployment-access, dns-zone, shared, tracker, worker, monitoring, all"
	@echo "The generic deploy target rejects deployment-access; use deploy-deployment-access locally."

install:
	uv sync --python 3.12
	@echo "Done! Run 'source .venv/bin/activate' to activate."

style: format lint typecheck

format: venv_check
	uv run ruff format .
	uv run ruff check --fix .

lint: venv_check
	uv run ruff format --check .
	uv run ruff check .

typecheck: venv_check
	uv run basedpyright .

test: venv_check
	PYTHONPATH=. uv run pytest tests

venv_check:
	@if [ ! -f .venv/bin/activate ]; then \
		echo "Virtualenv not found. Run 'make install' first."; \
		exit 1; \
	fi

validate-target:
	@if [ -z "$(STAGE)" ]; then \
		echo "STAGE is required. Set STAGE=dev or STAGE=prod."; \
		exit 1; \
	fi
	@if [ "$(STAGE)" != "dev" ] && [ "$(STAGE)" != "prod" ]; then \
		echo "STAGE must be dev or prod; got $(STAGE)."; \
		exit 1; \
	fi
	@if [ -z "$(AWS_REGION)" ]; then \
		echo "AWS_REGION is required. Set AWS_REGION=$(DEPLOYMENT_REGION)."; \
		exit 1; \
	fi
	@if [ "$(AWS_REGION)" != "$(DEPLOYMENT_REGION)" ]; then \
		echo "AWS_REGION must be $(DEPLOYMENT_REGION); got $(AWS_REGION)."; \
		exit 1; \
	fi
	@if [ "$(STAGE)" = "dev" ] && [ -z "$(DEV_ACCOUNT_ID)" ]; then \
		echo "DEV_ACCOUNT_ID is required for STAGE=dev."; \
		exit 1; \
	fi
	@if [ "$(STAGE)" = "dev" ] && [ "$(DEV_ACCOUNT_ID)" = "$(PRODUCTION_ACCOUNT_ID)" ]; then \
		echo "DEV_ACCOUNT_ID must not be the production AWS account."; \
		exit 1; \
	fi

validate-scope:
	@case "$(SCOPE)" in \
		deployment-access|dns-zone|shared|tracker|worker|monitoring|all) ;; \
		*) echo "SCOPE must be deployment-access, dns-zone, shared, tracker, worker, monitoring, or all; got $(SCOPE)."; exit 1 ;; \
	esac
	@if [ "$(STAGE)" = "prod" ] && { [ "$(SCOPE)" = "deployment-access" ] || [ "$(SCOPE)" = "dns-zone" ]; }; then \
		echo "SCOPE=$(SCOPE) is only available for STAGE=dev."; \
		exit 1; \
	fi

validate-deploy-scope: validate-scope
	@if [ "$(SCOPE)" = "deployment-access" ]; then \
		echo "The generic deploy target cannot deploy deployment-access. Use make deploy-deployment-access with local administrator credentials."; \
		exit 1; \
	fi

preflight: validate-target venv_check
	uv run python deployment_target.py

bootstrap: STAGE := dev
bootstrap: preflight
	$(CDK) bootstrap aws://$(EXPECTED_ACCOUNT_ID)/$(AWS_REGION) $(CDK_PROFILE)

synth: preflight validate-scope
	$(CDK) synth $(SELECTED_STACKS) $(CDK_CONTEXT) $(CDK_PROFILE) $(CDK_OUTPUT)

plan: preflight validate-scope
	$(CDK) diff $(SELECTED_STACKS) $(CDK_CONTEXT) $(CDK_PROFILE)

diff: plan

deploy: preflight validate-deploy-scope
	$(CDK) deploy $(SELECTED_STACKS) $(CDK_CONTEXT) $(CDK_PROFILE) --require-approval never

deploy-deployment-access: SCOPE := deployment-access
deploy-deployment-access: preflight validate-scope
	$(CDK) deploy $(SELECTED_STACKS) $(CDK_CONTEXT) $(CDK_PROFILE) --require-approval never

diff-deployment-access: SCOPE := deployment-access
diff-deployment-access: plan

diff-dns-zone: SCOPE := dns-zone
diff-dns-zone: plan

diff-shared: SCOPE := shared
diff-shared: plan

diff-tracker: SCOPE := tracker
diff-tracker: plan

diff-worker: SCOPE := worker
diff-worker: plan

diff-monitoring: SCOPE := monitoring
diff-monitoring: plan

diff-all: SCOPE := all
diff-all: plan

deploy-dns-zone: SCOPE := dns-zone
deploy-dns-zone: deploy

deploy-shared: SCOPE := shared
deploy-shared: deploy

deploy-tracker: SCOPE := tracker
deploy-tracker: deploy

deploy-worker: SCOPE := worker
deploy-worker: deploy

deploy-monitoring: SCOPE := monitoring
deploy-monitoring: deploy

deploy-all: SCOPE := all
deploy-all: deploy

hotswap: preflight validate-deploy-scope
	$(CDK) deploy $(SELECTED_STACKS) $(CDK_CONTEXT) $(CDK_PROFILE) --hotswap

destroy: preflight validate-scope
	@read -r -p "Destroy $(STAGE) stacks for scope $(SCOPE)? [y/N] " confirm && [ "$$confirm" = "y" ]
	$(CDK) destroy $(SELECTED_STACKS) $(CDK_CONTEXT) $(CDK_PROFILE)
