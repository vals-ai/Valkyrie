.PHONY: help install lint typecheck test venv_check validate-scope preflight \
	plan diff deploy deploy-shared deploy-tracker deploy-driver deploy-executor hotswap destroy

PRODUCTION_ACCOUNT_ID ?= 613431292675

STAGE ?=
AWS_REGION ?=
DEV_ACCOUNT_ID ?=
PROFILE ?=
SCOPE ?= all

EXPECTED_ACCOUNT_ID = $(if $(filter dev release-test,$(STAGE)),$(DEV_ACCOUNT_ID),$(if $(filter prod,$(STAGE)),$(PRODUCTION_ACCOUNT_ID),))
STACK_PREFIX = $(if $(filter dev,$(STAGE)),ValkDev,$(if $(filter release-test,$(STAGE)),ValkReleaseTest,))
STACKS_shared = $(STACK_PREFIX)SharedStack
STACKS_tracker = $(STACK_PREFIX)TrackerStack
STACKS_driver = $(STACK_PREFIX)DriverStack
# ExecutorStack is the Python owner; WorkerStack is the retained CloudFormation identity.
STACKS_executor = $(STACK_PREFIX)WorkerStack
STACKS_monitoring = $(STACK_PREFIX)MonitoringStack
STACKS_core = $(STACKS_shared) $(STACKS_tracker) $(STACKS_monitoring)
STACKS_all = --all
SELECTED_STACKS = $(STACKS_$(SCOPE))
DEPLOY_EXCLUSIVELY = $(if $(filter executor,$(SCOPE)),--exclusively,)
DIFF_STACKS = $(if $(filter all,$(SCOPE)),,$(SELECTED_STACKS))
CDK_DIFF_METHOD = $(if $(filter release-test,$(STAGE)),--method=template,)

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
	@echo "  make plan STAGE=dev|release-test|prod AWS_REGION=us-east-1 - Show a CDK diff"
	@echo "  make deploy STAGE=<stage> AWS_REGION=us-east-1 - Deploy a component"
	@echo "  make test                                     - Run infrastructure tests"
	@echo "  make lint                                     - Check infrastructure formatting and lint"
	@echo "  make typecheck                                - Type-check infrastructure code"
	@echo ""
	@echo "Set SCOPE to shared, tracker, driver, executor, monitoring, core, or all. Driver is release-test only."

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
		shared|tracker|driver|executor|monitoring|core|all) ;; \
		*) echo "SCOPE must be shared, tracker, driver, executor, monitoring, core, or all; got $(SCOPE)."; exit 1 ;; \
	esac
	@if [ "$(SCOPE)" = "driver" ] && [ "$(STAGE)" != "release-test" ]; then \
		echo "SCOPE=driver requires STAGE=release-test; got $(STAGE)."; \
		exit 1; \
	fi

preflight: venv_check
	uv run python deployment_target.py

plan: validate-scope preflight
	uv run cdk diff $(CDK_DIFF_METHOD) $(DIFF_STACKS) $(CDK_CTX) $(CDK_PROFILE)

diff: plan

deploy: validate-scope preflight
	uv run cdk deploy $(SELECTED_STACKS) $(CDK_CTX) $(CDK_PROFILE) $(DEPLOY_EXCLUSIVELY) --require-approval never

deploy-shared: SCOPE := shared
deploy-shared: deploy

deploy-tracker: SCOPE := tracker
deploy-tracker: deploy

deploy-driver: SCOPE := driver
deploy-driver: deploy

deploy-executor: SCOPE := executor
deploy-executor: deploy

hotswap: preflight
	uv run cdk deploy --all $(CDK_CTX) $(CDK_PROFILE) --hotswap

destroy: preflight
	@read -r -p "Destroy all $(STAGE) stacks? [y/N] " confirm && [ "$$confirm" = "y" ]
	uv run cdk destroy --all $(CDK_CTX) $(CDK_PROFILE)
