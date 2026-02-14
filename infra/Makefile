.PHONY: help install deploy deploy-shared deploy-tracker deploy-swebench hotswap diff destroy

help:
	@echo "Makefile for agentic-harness infrastructure"
	@echo ""
	@echo "  make install                - Install CDK dependencies"
	@echo "  make hotswap                - Hot-swap deploy (fast, skips CloudFormation)"
	@echo "  make deploy                 - Deploy all stacks"
	@echo "  make deploy-shared          - Deploy shared infrastructure only"
	@echo "  make deploy-tracker         - Deploy tracker service only"
	@echo "  make deploy-swebench        - Deploy swebench service only"
	@echo "  make diff                   - Show diff of pending changes"
	@echo "  make destroy                - Destroy all stacks"

install:
	uv venv --python 3.12
	uv sync
	@echo "Done! Run 'source .venv/bin/activate' to activate."

venv_check:
	@if [ ! -f .venv/bin/activate ]; then \
		echo "Virtualenv not found! Run 'make install' first."; \
		exit 1; \
	fi

deploy: venv_check
	cdk deploy --all --require-approval never

deploy-shared: venv_check
	cdk deploy SharedStack --require-approval never

deploy-tracker: venv_check
	cdk deploy TrackerStack --require-approval never

deploy-swebench: venv_check
	cdk deploy SwebenchStack --require-approval never

hotswap: venv_check
	cdk deploy --all --hotswap

diff: venv_check
	cdk diff

destroy: venv_check
	@read -p "Are you sure you want to destroy all stacks? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	cdk destroy --all
