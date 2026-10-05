#!/usr/bin/env bash
# Deploy the CDK stack with a chosen AWS profile, optionally uploading input file(s)
# to S3 afterward.
#
#   scripts/deploy.sh --profile <your-aws-profile> [--region <region>] [--bootstrap] \
#     [--upload] [--prefix <prefix>] [file.csv ...] [-- <extra cdk args>]
#
# --bootstrap runs `cdk bootstrap` first (needed once per account/region).
# --upload uploads file(s) to the deployed bucket's input/ prefix after a successful
#   deploy. With no files given, uploads sample_data/orders.csv. Ignored without
#   --upload. --prefix (also only meaningful with --upload) defaults to the stack's
#   own Prefix output (so it always matches what was actually deployed, even if you
#   overrode `-c prefix=...` for this run) -- pass --prefix explicitly only to upload
#   somewhere other than where the pipeline itself reads from.
# Extra args after `--` go to `cdk deploy` untouched, e.g. -- -c environment_tag=prod
set -euo pipefail
# shellcheck source=scripts/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

# Split at the first literal "--": everything before is our own flags/profile/files;
# everything after is forwarded to `cdk deploy` untouched (so e.g. `-c foo=bar` after
# `--` is never mistaken for an upload file).
OWN_ARGS=()
CDK_EXTRA=()
SEEN_SEP=false
for a in "$@"; do
  if ! $SEEN_SEP && [[ "$a" == "--" ]]; then
    SEEN_SEP=true
    continue
  fi
  if $SEEN_SEP; then
    CDK_EXTRA+=("$a")
  else
    OWN_ARGS+=("$a")
  fi
done

BOOTSTRAP=false
UPLOAD=false
PREFIX=""
PREFIX_SET=false
FILES=()
COMMON_ARGS=()
set -- "${OWN_ARGS[@]+"${OWN_ARGS[@]}"}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --bootstrap) BOOTSTRAP=true; shift ;;
    --upload) UPLOAD=true; shift ;;
    --prefix) PREFIX="${2:?--prefix needs a value}"; PREFIX_SET=true; shift 2 ;;
    -p|--profile|-r|--region) COMMON_ARGS+=("$1" "$2"); shift 2 ;;
    *) FILES+=("$1"); shift ;;
  esac
done
parse_common_args "${COMMON_ARGS[@]+"${COMMON_ARGS[@]}"}"

cd "$PROJECT_ROOT"
activate_venv

if $BOOTSTRAP; then
  cdk bootstrap "aws://$CDK_DEFAULT_ACCOUNT/$AWS_REGION" --profile "$AWS_PROFILE"
fi

cdk deploy --profile "$AWS_PROFILE" --require-approval never "${CDK_EXTRA[@]+"${CDK_EXTRA[@]}"}"

echo
echo "Deployed. Stack outputs:"
for k in BucketName Prefix DatabaseName JobName GlueJobRoleArn SilverDqRulesetName GoldDqRulesetName; do
  printf '  %-15s %s\n' "$k" "$(stack_output "$k")"
done

if $UPLOAD; then
  [[ ${#FILES[@]} -gt 0 ]] || FILES=("$PROJECT_ROOT/sample_data/orders.csv")

  BUCKET="$(stack_output BucketName)"
  if [[ -z "$BUCKET" || "$BUCKET" == "None" ]]; then
    echo "error: no BucketName output for stack $STACK_NAME; deploy must have failed." >&2
    exit 1
  fi

  if ! $PREFIX_SET; then
    PREFIX="$(stack_output Prefix)"
    if [[ -z "$PREFIX" || "$PREFIX" == "None" ]]; then
      echo "error: no Prefix output for stack $STACK_NAME; deploy must have failed." >&2
      exit 1
    fi
  fi

  echo
  echo "Uploading to s3://$BUCKET/$PREFIX/input/ ..."
  for f in "${FILES[@]}"; do
    if [[ ! -f "$f" ]]; then
      echo "error: file not found: $f" >&2
      exit 1
    fi
    aws s3 cp "$f" "s3://$BUCKET/$PREFIX/input/$(basename "$f")"
  done
else
  echo
  echo "Next: scripts/deploy.sh --profile $AWS_PROFILE --upload   # uploads sample_data/orders.csv"
fi
