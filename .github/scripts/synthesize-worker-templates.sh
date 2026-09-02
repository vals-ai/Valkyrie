#!/usr/bin/env bash
set -euo pipefail

# PR classification runs this helper from the trusted base revision while
# synthesizing candidate source with synthetic account IDs and no AWS access.
revision="${1:?revision is required}"
target_branch="${2:?target branch is required}"
label="${3:?label is required}"
artifact_directory="${4:?artifact directory is required}"
repository_directory="${5:-.}"

source_directory="$RUNNER_TEMP/source-$label"
mkdir -p "$source_directory" "$artifact_directory"
: > "$artifact_directory/stack-ids.txt"
if [[ ! "$revision" =~ ^0+$ ]]; then
  git -C "$repository_directory" archive "$revision" | tar -x -C "$source_directory"
fi

has_bench_stage=false
if [[ ! "$revision" =~ ^0+$ ]] && python3 -c \
  'import ast, sys; from pathlib import Path; tree = ast.parse(Path(sys.argv[1]).read_text()); matches = [node for node in tree.body if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "BENCH" for target in node.targets)]; raise SystemExit(len(matches) != 1 or not isinstance(matches[0].value, ast.Constant) or matches[0].value.value != "bench")' \
  "$source_directory/infra/stage.py"; then
  has_bench_stage=true
fi

production_account_id=222222222222
if [[ "$has_bench_stage" == "true" ]]; then
  production_account_id=333333333333
fi

case "$target_branch" in
  dev)
    targets=("dev:ValkDevWorkerStack:111111111111")
    ;;
  prod)
    if [[ "$has_bench_stage" == "true" ]]; then
      targets=(
        "bench:WorkerStack:222222222222"
        "prod:ValkProdWorkerStack:333333333333"
      )
    else
      targets=(
        "prod:WorkerStack:222222222222"
        "unsupported:ValkProdWorkerStack:333333333333"
      )
    fi
    ;;
  *)
    echo "::error::Unsupported deployment branch '$target_branch'."
    exit 1
    ;;
esac

for target in "${targets[@]}"; do
  IFS=: read -r stage stack_id account_id <<< "$target"
  printf '%s\n' "$stack_id" >> "$artifact_directory/stack-ids.txt"
  template="$artifact_directory/$stack_id.template.json"
  if [[ "$revision" =~ ^0+$ || "$stage" == "unsupported" ]]; then
    printf '{"Resources": {}}\n' > "$template"
    continue
  fi

  assembly="$RUNNER_TEMP/cdk-$label-$stage"
  (
    cd "$source_directory/infra"
    BENCH_ACCOUNT_ID=222222222222 \
    PRODUCTION_ACCOUNT_ID="$production_account_id" \
    DEV_ACCOUNT_ID=111111111111 \
    CDK_DEFAULT_ACCOUNT="$account_id" \
    CDK_DEFAULT_REGION=us-east-1 \
    AWS_REGION=us-east-1 \
    STAGE="$stage" \
    AWS_DEPLOYMENT_ROLE_ORG_IDS=00000000-0000-0000-0000-000000000001 \
    AWS_TRACKER_SECRET_NAME_PREFIXES=offline-synth \
    AWS_EXECUTOR_SECRET_NAME_PREFIXES=offline-synth \
    BENCHMARK_CATALOG_URL=https://offline.invalid \
    SENTRY_DSN_SECRET_NAME=offline-synth \
    DESCOPE_MANAGEMENT_KEY_SECRET_NAME=offline-synth \
    DESCOPE_PROJECT_ID=offline-synth \
    CDK_CONTEXT_JSON="{\"stage\":\"$stage\"}" \
    CDK_OUTDIR="$assembly" \
      uv run --frozen python app.py
  )
  cp "$assembly/$stack_id.template.json" "$template"
done
