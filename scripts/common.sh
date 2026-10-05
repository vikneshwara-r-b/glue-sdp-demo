#!/usr/bin/env bash
# Shared helpers for deploy.sh / destroy.sh. Source this, don't run it.

STACK_NAME="DeclarativeEtlPipelineUsingGlueStack"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Parses --profile/-p <name> and --region/-r <name>; falls back to AWS_PROFILE / AWS_REGION.
# Leaves any other arguments in REMAINING_ARGS.
parse_common_args() {
  REMAINING_ARGS=()
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -p|--profile) AWS_PROFILE="${2:?--profile needs a value}"; shift 2 ;;
      -r|--region)  AWS_REGION="${2:?--region needs a value}"; shift 2 ;;
      *) REMAINING_ARGS+=("$1"); shift ;;
    esac
  done

  if [[ -z "${AWS_PROFILE:-}" ]]; then
    echo "error: no AWS profile. Pass --profile <your-aws-profile> or export AWS_PROFILE." >&2
    exit 1
  fi
  export AWS_PROFILE

  # Region: explicit flag/env, else whatever the profile is configured with.
  AWS_REGION="${AWS_REGION:-$(aws configure get region --profile "$AWS_PROFILE" 2>/dev/null || true)}"
  if [[ -z "$AWS_REGION" ]]; then
    echo "error: no region. Pass --region or set one on the profile." >&2
    exit 1
  fi
  export AWS_REGION AWS_DEFAULT_REGION="$AWS_REGION"

  # Verify credentials work and expose the account for the CDK CLI.
  CDK_DEFAULT_ACCOUNT="$(aws sts get-caller-identity --query Account --output text)" || {
    echo "error: could not authenticate with profile '$AWS_PROFILE' (try: aws sso login --profile $AWS_PROFILE)" >&2
    exit 1
  }
  export CDK_DEFAULT_ACCOUNT CDK_DEFAULT_REGION="$AWS_REGION"
  echo "Using profile=$AWS_PROFILE account=$CDK_DEFAULT_ACCOUNT region=$AWS_REGION"
}

# Prints the value of a stack output, or nothing if the stack/output doesn't exist.
stack_output() {
  aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text 2>/dev/null || true
}

# Activates the project's virtualenv if present and not already active.
activate_venv() {
  if [[ -z "${VIRTUAL_ENV:-}" && -f "$PROJECT_ROOT/.venv/bin/activate" ]]; then
    # shellcheck disable=SC1091
    source "$PROJECT_ROOT/.venv/bin/activate"
  fi
}
