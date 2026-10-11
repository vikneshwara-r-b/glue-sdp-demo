#!/usr/bin/env bash
# Trigger a start-job-run on the deployed orders-sdp-job, with a chosen AWS profile.
# The job name is read from the stack's own JobName output, never hardcoded.
#
#   scripts/run_job.sh --profile <your-aws-profile> [--region <region>] [MODE] [--wait]
#
# MODE (choose at most one; default is --run):
#   --validate                 Dry run: dependency/SQL/Python compilation checks,
#                               writes no data.
#   --run                      Run the pipeline for real (default if no mode given).
#   --full-refresh-all         Reset every table (incl. streaming-table checkpoints)
#                               and recompute everything from scratch.
#   --full-refresh <dataset>   Full refresh of just one dataset, e.g. silver_orders.
#   --refresh <dataset>        Incremental refresh of just one dataset.
#
# --wait polls the run until it finishes and prints the final state (and the
# CloudWatch log group, on failure). Without --wait, prints the run ID and returns
# immediately -- check status later with:
#   aws glue get-job-run --job-name "$JOB_NAME" --run-id <run-id> --region "$AWS_REGION"
set -euo pipefail
# shellcheck source=scripts/common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

MODE="run"
DATASET=""
WAIT=false
ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --validate) MODE="validate"; shift ;;
    --run) MODE="run"; shift ;;
    --full-refresh-all) MODE="full-refresh-all"; shift ;;
    --full-refresh) MODE="full-refresh"; DATASET="${2:?--full-refresh needs a dataset name}"; shift 2 ;;
    --refresh) MODE="refresh"; DATASET="${2:?--refresh needs a dataset name}"; shift 2 ;;
    --wait) WAIT=true; shift ;;
    *) ARGS+=("$1"); shift ;;
  esac
done
parse_common_args "${ARGS[@]+"${ARGS[@]}"}"

JOB_NAME="$(stack_output JobName)"
if [[ -z "$JOB_NAME" || "$JOB_NAME" == "None" ]]; then
  echo "error: no JobName output for stack $STACK_NAME. Deploy first (scripts/deploy.sh)." >&2
  exit 1
fi

case "$MODE" in
  validate)         CONF="spark.glue.sdp.jobMode=VALIDATE" ;;
  run)               CONF="spark.glue.sdp.jobMode=RUN" ;;
  full-refresh-all)  CONF="spark.glue.sdp.jobMode=RUN --conf spark.glue.sdp.runMode=--full-refresh-all" ;;
  full-refresh)      CONF="spark.glue.sdp.jobMode=RUN --conf spark.glue.sdp.runMode=--full-refresh $DATASET" ;;
  refresh)           CONF="spark.glue.sdp.jobMode=RUN --conf spark.glue.sdp.runMode=--refresh $DATASET" ;;
esac

echo "Starting $JOB_NAME (mode: $MODE${DATASET:+, dataset: $DATASET})..."
RUN_ID="$(aws glue start-job-run --job-name "$JOB_NAME" \
  --arguments "{\"--conf\":\"$CONF\"}" --region "$AWS_REGION" \
  --query 'JobRunId' --output text)"
echo "Run ID: $RUN_ID"

if ! $WAIT; then
  echo "Check status with:"
  echo "  aws glue get-job-run --job-name \"$JOB_NAME\" --run-id $RUN_ID --region \"$AWS_REGION\""
  exit 0
fi

echo -n "Waiting for completion"
while true; do
  STATE="$(aws glue get-job-run --job-name "$JOB_NAME" --run-id "$RUN_ID" \
    --region "$AWS_REGION" --query 'JobRun.JobRunState' --output text)"
  case "$STATE" in
    SUCCEEDED|FAILED|ERROR|TIMEOUT|STOPPED) echo; break ;;
    *) echo -n "."; sleep 10 ;;
  esac
done

echo "Final state: $STATE"
if [[ "$STATE" != "SUCCEEDED" ]]; then
  ERROR_MSG="$(aws glue get-job-run --job-name "$JOB_NAME" --run-id "$RUN_ID" \
    --region "$AWS_REGION" --query 'JobRun.ErrorMessage' --output text)"
  echo "Error: $ERROR_MSG" >&2
  exit 1
fi
