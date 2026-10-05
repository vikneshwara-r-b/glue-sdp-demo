# Declarative ETL Pipeline on AWS Glue 6.0

> **Sample code notice:** This is a hobby/learning project, not production-hardened
> infrastructure. Review the IAM, encryption, and removal-policy choices below before
> reusing any part of it against real data.

A single [AWS Glue 6.0](https://aws.amazon.com/blogs/big-data/build-declarative-etl-pipelines-with-aws-glue-6-0/)
job that ingests raw order records from CSV in Amazon S3 and produces three
Data-Catalog-registered tables through a bronze → silver → gold medallion sequence —
with no hand-written orchestration. [AWS Glue Spark Declarative Pipelines (SDP)](https://aws.amazon.com/blogs/big-data/build-declarative-etl-pipelines-with-aws-glue-6-0/)
infers the dependency graph from table references in the transformation code and runs
everything as one job, in place of three Glue jobs plus a Step Functions/MWAA
orchestrator.

Deployed with the [AWS CDK](https://aws.amazon.com/cdk/) (Python).

## What it deploys

- **Amazon S3 bucket** — private, SSE-S3 encrypted, versioned, TLS-enforced. Holds the
  pipeline's input CSV, SDP state, and Data Catalog warehouse location. (The packaged
  pipeline zip itself is uploaded to the CDK bootstrap assets bucket via
  `aws_s3_assets.Asset`, not this bucket.)
- **One AWS Glue 6.0 job** with Spark Declarative Pipelines enabled
  (`--enable-spark-declarative-pipeline`) and Data Catalog registration
  (`--enable-glue-datacatalog`), running with the Flex execution class (cheaper spare
  capacity; start time isn't guaranteed).
- **AWS Glue Data Catalog database** with an explicit `LocationUri` — SDP registers
  tables into it but does not create it, so CDK must.
- **One IAM role** for Glue to assume, scoped to this bucket only (it also carries the
  AWS-managed `AWSGlueServiceRole` policy that Glue itself requires — see "Security"
  below).

## Architecture

```
        ┌─────────────────────────────┐
        │   orders.csv (Amazon S3)    │
        └───────────────┬─────────────┘
                         │
        ┌────────────────▼────────────────┐
        │  bronze_orders (streaming table)    │
        │  raw CSV, every column as string    │
        └────────────────┬────────────────┘
                         │
        ┌────────────────▼────────────────┐
        │  silver_orders (streaming table)    │
        │  typed, filtered to COMPLETE/amount>0 │
        │  + derived amount_band              │
        └────────────────┬────────────────┘
                         │
        ┌────────────────▼────────────────┐
        │  gold_sales_summary (SQL view)      │
        │  aggregated by region               │
        └─────────────────────────────────┘
```

All three tables are produced by **one** Glue job (`orders-sdp-job`); SDP resolves the
bronze → silver → gold ordering from the table references in
`pipeline_src/transformations/` (`spark.readStream.table("bronze_orders")`,
`FROM silver_orders`), not from a hand-written DAG. `bronze_orders`/`silver_orders` are
streaming tables (incremental, checkpointed in S3) and `gold_sales_summary` is a SQL
materialized view (full recompute each run — it's an aggregation/report).

## Security

This is a personal/sandbox project. Under the
[AWS Shared Responsibility Model](https://aws.amazon.com/compliance/shared-responsibility-model/),
the controls below are what's deployed as a baseline — review them before pointing this
at anything beyond synthetic sample data.

**Implemented:**

- S3 public access fully blocked, SSE-S3 encryption, TLS enforced, versioning enabled.
- IAM role scoped to this bucket's ARN only for all data-plane S3 actions (no `"*"`
  resources) — see `test_glue_role_inline_policy_is_scoped_not_wildcard`.
- No hardcoded account IDs, ARNs, or bucket names in committed source — naming comes
  from `PipelineStackProps`, populated in [`app.py`](app.py) from CDK context (with dev
  defaults), and account/region from `CDK_DEFAULT_ACCOUNT`/`CDK_DEFAULT_REGION`.

**Accepted exception:**

- The Glue job role attaches the AWS-managed `AWSGlueServiceRole` policy, required by
  the Glue service itself for its own operational needs (logging, catalog calls, ENI
  management) — hand-rolling an equivalent inline policy would just duplicate what AWS
  maintains. Compensating control: actual *data* access (`s3:GetObject`/`PutObject`/
  `DeleteObject`/`ListBucket`) is granted separately via an inline policy scoped to this
  bucket's ARN only, enforced by
  `test_glue_role_inline_policy_is_scoped_not_wildcard`.

**Customer/you responsibilities:**

- The bucket uses `RemovalPolicy.DESTROY` + `auto_delete_objects=True` so `cdk destroy`
  cleans up fully — do not point `bucket_name` at anything you need retained.
- `sample_data/orders.csv` is synthetic sample data for the walkthrough. It's
  gitignored (not committed) — provide your own CSV matching the schema in
  `pipeline_src/transformations/01_bronze.py` before the "Post-deploy" step below.

## Prerequisites

- Python 3.11+
- AWS CDK v2 (`npm install -g aws-cdk`)
- An AWS account/region and credentials profile with permission to deploy S3, IAM, and
  Glue resources.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
```

## Configuration

Bucket/prefix/database/job naming and sizing are CDK context values, read in
[`app.py`](app.py) with dev defaults (`bucket_name=glue-sdp-demo`,
`prefix=orders-pipeline`, `database_name=sdp_demo_db`, `job_name=orders-sdp-job`,
`worker_type=G.1X`, `num_workers=2`, `environment_tag=dev`, `owner_tag=de-team`).

The S3 bucket name is chosen by [`app.py`](app.py), not passed on the command line. S3
bucket names are globally unique across every AWS account, so the deployed name is
always `<bucket_name>-<bucket_suffix>` (e.g. `glue-sdp-demo-<suffix>`), where
`bucket_suffix` is a 6-letter string. `app.py` generates a random one if `bucket_suffix`
isn't set in [`cdk.json`](cdk.json). Keep the suffix pinned in `cdk.json` once you've
deployed — a bucket name that changes on every synth makes CloudFormation replace the
bucket, losing its contents, instead of updating it in place. Don't pass
`-c bucket_name`/`-c bucket_suffix` at deploy time; read the actual name from the
`BucketName` stack output (see "Deploy").

Other settings can still be overridden, either per-run:

```bash
cdk deploy -c environment_tag=dev -c num_workers=2   # minimum for G.1X (Glue rejects 1)
```

or durably, by adding entries under `"context"` in [`cdk.json`](cdk.json):

```json
{
  "context": {
    "prefix": "orders-pipeline",
    "database_name": "sdp_demo_db",
    "job_name": "orders-sdp-job"
  }
}
```

## Deploy

```bash
cdk bootstrap    # first time only, per account/region
cdk synth
cdk deploy
```

`cdk deploy` prints `BucketName`, `Prefix`, `DatabaseName`, `JobName`, and
`GlueJobRoleArn` as stack outputs. Load them into shell variables for the steps below:

```bash
export AWS_REGION=<region>

stack_output() {
  aws cloudformation describe-stacks --stack-name DeclarativeEtlPipelineUsingGlueStack \
    --region "$AWS_REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}
export BUCKET=$(stack_output BucketName)
export PREFIX=$(stack_output Prefix)
export JOB_NAME=$(stack_output JobName)
export DATABASE=$(stack_output DatabaseName)
```

Reading `PREFIX` from the `Prefix` output (instead of hardcoding `orders-pipeline`)
means it always matches what was actually deployed, even if you overrode
`-c prefix=...` for this run.

## Helper scripts (optional)

Wrapper scripts in [`scripts/`](scripts/) run the same steps with a named AWS profile,
instead of the manual `cdk deploy` + `stack_output` + `aws s3 cp` sequence above.

### `scripts/deploy.sh`

```
scripts/deploy.sh --profile <your-aws-profile> [--region <region>] [--bootstrap] \
  [--upload] [--prefix <prefix>] [file.csv ...] [-- <extra cdk args>]
```

| Flag | Meaning |
|---|---|
| `--profile <name>` | Required (or export `AWS_PROFILE`). AWS CLI/CDK profile to deploy with. |
| `--region <region>` | Optional. Defaults to `AWS_REGION`, or the profile's configured region. |
| `--bootstrap` | Runs `cdk bootstrap` first. Needed once per account/region — safe to pass on every run after that too, it's a no-op if already bootstrapped. |
| `--upload` | After a successful deploy, also uploads file(s) to the deployed bucket's `<prefix>/input/`. Without this flag, no upload happens and any trailing file args below are simply unused. |
| `--prefix <prefix>` | Only meaningful with `--upload`. Which `input/` prefix to upload into. Defaults to the just-deployed stack's own `Prefix` output — so it always matches what the pipeline actually reads from, even if you overrode `-c prefix=...` for this run. Pass `--prefix` explicitly only to upload somewhere *other* than where the pipeline reads. |
| `[file.csv ...]` | Only meaningful with `--upload`. One or more local files to upload. With none given, uploads `sample_data/orders.csv`. |
| `-- <extra cdk args>` | Everything after a literal `--` is forwarded to `cdk deploy` untouched, e.g. `-- -c environment_tag=prod`. Put this last — args after `--upload` but before `--` are still parsed as upload files, not cdk args. |

What it does: runs `cdk deploy --require-approval never` (so it won't pause for an
IAM-change prompt), prints the `BucketName`/`Prefix`/`DatabaseName`/`JobName`/
`GlueJobRoleArn` stack outputs, then — only if `--upload` was passed — uploads your
file(s) and prints the destination. Examples:

```bash
scripts/deploy.sh --profile <your-aws-profile> --bootstrap
# ^ first deploy ever in this account/region

scripts/deploy.sh --profile <your-aws-profile> --upload
# ^ deploy + upload sample_data/orders.csv in one step

scripts/deploy.sh --profile <your-aws-profile> --upload sample_data/orders_incremental.csv
# ^ deploy + upload a specific file (e.g. to demo an incremental streaming-table run)

scripts/deploy.sh --profile <your-aws-profile> --upload a.csv b.csv --prefix custom-prefix
# ^ explicit --prefix overrides the stack's own Prefix output -- only needed to upload
#   somewhere other than where the pipeline itself reads from

scripts/deploy.sh --profile <your-aws-profile> -- -c environment_tag=prod -c num_workers=4
# ^ deploy only, forwarding extra context overrides straight to `cdk deploy`

scripts/deploy.sh --profile <your-aws-profile> --upload -- -c environment_tag=prod
# ^ --upload and a `--` cdk passthrough can be combined
```

### `scripts/destroy.sh`

```bash
scripts/destroy.sh --profile <your-aws-profile>          # asks you to confirm the account ID
scripts/destroy.sh --profile <your-aws-profile> --yes     # skips the confirmation prompt
```

Deletes the stack and every object in the bucket (see "Cleanup" below).

## Post-deploy: upload sample data

Not part of the CDK stack — a one-time step per environment, done either via
`scripts/deploy.sh --profile <your-aws-profile> --upload` (see above) or directly:

```bash
aws s3 cp sample_data/orders.csv "s3://$BUCKET/$PREFIX/input/orders.csv" --region "$AWS_REGION"
```

## Run the pipeline

```bash
# Dry-run validate: dependency/SQL/Python compilation checks, no data written
aws glue start-job-run --job-name "$JOB_NAME" \
  --arguments '{"--conf":"spark.glue.sdp.jobMode=VALIDATE"}' --region "$AWS_REGION"

# Run for real
aws glue start-job-run --job-name "$JOB_NAME" \
  --arguments '{"--conf":"spark.glue.sdp.jobMode=RUN"}' --region "$AWS_REGION"
```

### Full refresh

A full refresh (reset every table — including `bronze_orders`/`silver_orders`'s
streaming-table checkpoints — and recompute everything from scratch) is a **runtime
job argument**, not a CDK/infra change: no stack update, no deleting/recreating any
deployed resource. `spark.glue.sdp.runMode` controls it, combined into the *same*
`--conf` value as `jobMode` (space-separated, as one string — `--arguments` is a flat
JSON map, so it can only hold a single `--conf` key; you can't pass two separate
`--conf` entries):

```bash
# Full refresh: resets every table (including streaming-table checkpoints) and
# recomputes everything from scratch.
aws glue start-job-run --job-name "$JOB_NAME" \
  --arguments '{"--conf":"spark.glue.sdp.jobMode=RUN --conf spark.glue.sdp.runMode=--full-refresh-all"}' \
  --region "$AWS_REGION"

# Full refresh of just one dataset (e.g. after fixing a bug in silver's logic,
# without re-ingesting bronze from scratch):
aws glue start-job-run --job-name "$JOB_NAME" \
  --arguments '{"--conf":"spark.glue.sdp.jobMode=RUN --conf spark.glue.sdp.runMode=--full-refresh silver_orders"}' \
  --region "$AWS_REGION"
```

`--full-refresh-all`/`--full-refresh <dataset>` reset streaming tables' checkpoints and
reprocess all source data; for `gold_sales_summary` (a materialized view) it's a no-op
beyond what a normal `RUN` already does, since materialized views always fully
recompute anyway.

## Verify results

```bash
aws glue get-tables --database-name "$DATABASE" --region "$AWS_REGION" \
  --query 'TableList[].Name' --output table
```

Query `bronze_orders`, `silver_orders`, and `gold_sales_summary` via Amazon Athena
(downstream consumer concern, not provisioned by this stack).

## Tests

```bash
pytest tests/
```

Covers CDK fine-grained assertions (bucket encryption/public-access, IAM trust policy
and inline-policy scoping, Glue job version/worker count/SDP flags) and
`scripts/package_pipeline.py`'s manifest-rendering/zip-layout logic.

## Cleanup

```bash
cdk destroy
# or, with a named profile and a confirmation prompt:
scripts/destroy.sh --profile <your-aws-profile>
```

The bucket and its objects are deleted along with the stack (see "Security" above).
