#!/usr/bin/env bash
# Tear down the CDK stack. The bucket uses auto_delete_objects, so its contents
# (including uploaded CSVs and SDP state) are deleted too.
#
#   scripts/destroy.sh --profile <your-aws-profile> [--region <region>] [--yes]
#
# Asks for confirmation unless --yes is given.
set -euo pipefail
# shellcheck source=scripts/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

YES=false
ARGS=()
for a in "$@"; do
  if [[ "$a" == "--yes" || "$a" == "-y" ]]; then YES=true; else ARGS+=("$a"); fi
done
parse_common_args "${ARGS[@]+"${ARGS[@]}"}"

BUCKET="$(stack_output BucketName)"
echo "About to destroy stack $STACK_NAME${BUCKET:+ and delete all objects in s3://$BUCKET}."
if ! $YES; then
  read -r -p "Type the account id ($CDK_DEFAULT_ACCOUNT) to confirm: " reply
  [[ "$reply" == "$CDK_DEFAULT_ACCOUNT" ]] || { echo "Aborted."; exit 1; }
fi

cd "$PROJECT_ROOT"
activate_venv
cdk destroy --profile "$AWS_PROFILE" --force
