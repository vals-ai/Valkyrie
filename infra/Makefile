.PHONY: help install lint typecheck test venv_check validate-scope preflight \
	plan diff deploy deploy-shared deploy-tracker hotswap destroy

PRODUCTION_ACCOUNT_ID ?= 613431292675

STAGE ?=
AWS_REGION ?=
DEV_ACCOUNT_ID ?=
PROFILE ?=
SCOPE ?= all

EXPECTED_ACCOUNT_ID = $(if $(filter dev,$(STAGE)),$(DEV_ACCOUNT_ID),$(if $(filter prod,$(STAGE)),$(PRODUCTION_ACCOUNT_ID),))
STACK_PREFIX = $(if $(filter dev,$(STAGE)),Valk-Dev-,)
STACKS_shared = $(STACK_PREFIX)SharedStack
STACKS_tracker = $(STACK_PREFIX)TrackerStack
STACKS_worker = $(STACK_PREFIX)WorkerStack
STACKS_monitoring = $(STACK_PREFIX)MonitoringStack
STACKS_all = --all
SELECTED_STACKS = $(STACKS_$(SCOPE))

CDK_DEFAULT_ACCOUNT ?= $(EXPECTED_ACCOUNT_ID)
CDK_DEFAULT_REGION ?= $(AWS_REGION)
CDK_CTX = -c stage=$(STAGE)
CDK_PROFILE = $(if $(strip $(PROFILE)),--profile "$(PROFILE)",)

export STAGE AWS_REGION DEV_ACCOUNT_ID PRODUCTION_ACCOUNT_ID PROFILE
export CDK_DEFAULT_ACCOUNT CDK_DEFAULT_REGION

help:
	@echo "Makefile for Valkyrie infrastructure"
	@echo ""
	@echo "  make install                                  - Install CDK dependencies"
	@echo "  make plan STAGE=<stage> AWS_REGION=us-east-1 - Show a CDK diff"
	@echo "  make deploy STAGE=<stage> AWS_REGION=us-east-1 - Deploy a component"
	@echo "  make test                                     - Run infrastructure tests"
	@echo "  make lint                                     - Check infrastructure formatting and lint"
	@echo "  make typecheck                                - Type-check infrastructure code"
	@echo ""
	@echo "Set SCOPE to shared, tracker, worker, monitoring, or all. Dev also requires DEV_ACCOUNT_ID."

install:
	uv sync --python 3.12

lint: venv_check
	uv run ruff format --check .
	uv run ruff check .

typecheck: venv_check
	uv run basedpyright .

test: venv_check
	CDK_DEFAULT_ACCOUNT=123456789012 CDK_DEFAULT_REGION=us-east-1 \
		PYTHONPATH=. uv run python -m unittest discover -s tests -p "test_*.py"

venv_check:
	@if [ ! -f .venv/bin/activate ]; then \
		echo "Virtualenv not found. Run 'make install' first."; \
		exit 1; \
	fi

validate-scope:
	@case "$(SCOPE)" in \
		shared|tracker|worker|monitoring|all) ;; \
		*) echo "SCOPE must be shared, tracker, worker, monitoring, or all; got $(SCOPE)."; exit 1 ;; \
	esac

preflight: venv_check
	uv run python deployment_target.py

plan: validate-scope preflight
	uv run cdk diff $(SELECTED_STACKS) $(CDK_CTX) $(CDK_PROFILE)

diff: plan

deploy: validate-scope preflight
	uv run cdk deploy $(SELECTED_STACKS) $(CDK_CTX) $(CDK_PROFILE) --require-approval never

deploy-shared: SCOPE := shared
deploy-shared: deploy

deploy-tracker: SCOPE := tracker
deploy-tracker: deploy

hotswap: preflight
	uv run cdk deploy --all $(CDK_CTX) $(CDK_PROFILE) --hotswap

destroy: preflight
	@read -r -p "Destroy all $(STAGE) stacks? [y/N] " confirm && [ "$$confirm" = "y" ]
	uv run cdk destroy --all $(CDK_CTX) $(CDK_PROFILE)
