#!/usr/bin/env bash
# Upload CSV file(s) to the pipeline's input/ prefix. The bucket is read from the
# deployed stack's BucketName output, so it works with whatever name app.py chose.
#
#   scripts/upload_data.sh --profile <your-aws-profile> [--region <region>] [--prefix <prefix>] [file.csv ...]
#
# With no files, uploads sample_data/orders.csv. --prefix defaults to orders-pipeline
# (the `prefix` context value in cdk.json).
set -euo pipefail
# shellcheck source=scripts/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

parse_common_args "$@"

PREFIX="orders-pipeline"
FILES=()
set -- "${REMAINING_ARGS[@]+"${REMAINING_ARGS[@]}"}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --prefix) PREFIX="${2:?--prefix needs a value}"; shift 2 ;;
    *) FILES+=("$1"); shift ;;
  esac
done
[[ ${#FILES[@]} -gt 0 ]] || FILES=("$PROJECT_ROOT/sample_data/orders.csv")

BUCKET="$(stack_output BucketName)"
if [[ -z "$BUCKET" || "$BUCKET" == "None" ]]; then
  echo "error: no BucketName output for stack $STACK_NAME. Deploy first (scripts/deploy.sh)." >&2
  exit 1
fi

for f in "${FILES[@]}"; do
  if [[ ! -f "$f" ]]; then
    echo "error: file not found: $f" >&2
    exit 1
  fi
  aws s3 cp "$f" "s3://$BUCKET/$PREFIX/input/$(basename "$f")"
done
