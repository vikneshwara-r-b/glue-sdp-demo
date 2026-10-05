#!/usr/bin/env bash
# Deploy the CDK stack with a chosen AWS profile.
#
#   scripts/deploy.sh --profile <your-aws-profile> [--region <region>] [--bootstrap] [-- <extra cdk args>]
#
# --bootstrap runs `cdk bootstrap` first (needed once per account/region).
# Extra args after `--` go to `cdk deploy`, e.g. -- -c environment_tag=prod
set -euo pipefail
# shellcheck source=scripts/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

BOOTSTRAP=false
ARGS=()
for a in "$@"; do
  if [[ "$a" == "--bootstrap" ]]; then BOOTSTRAP=true; else ARGS+=("$a"); fi
done
parse_common_args "${ARGS[@]+"${ARGS[@]}"}"
# Drop the optional "--" separator before forwarding.
CDK_EXTRA=()
for a in "${REMAINING_ARGS[@]+"${REMAINING_ARGS[@]}"}"; do
  [[ "$a" == "--" ]] || CDK_EXTRA+=("$a")
done

cd "$PROJECT_ROOT"
activate_venv

if $BOOTSTRAP; then
  cdk bootstrap "aws://$CDK_DEFAULT_ACCOUNT/$AWS_REGION" --profile "$AWS_PROFILE"
fi

cdk deploy --profile "$AWS_PROFILE" --require-approval never "${CDK_EXTRA[@]+"${CDK_EXTRA[@]}"}"

echo
echo "Deployed. Stack outputs:"
for k in BucketName DatabaseName JobName GlueJobRoleArn; do
  printf '  %-15s %s\n' "$k" "$(stack_output "$k")"
done
echo
echo "Next: scripts/upload_data.sh --profile $AWS_PROFILE"
