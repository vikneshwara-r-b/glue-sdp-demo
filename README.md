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
        │  bronze_orders (materialized view)  │
        │  raw CSV, every column as string    │
        └────────────────┬────────────────┘
                         │
        ┌────────────────▼────────────────┐
        │  silver_orders (materialized view)  │
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
bronze → silver → gold ordering from `spark.table(...)` references in
`pipeline_src/transformations/`, not from a hand-written DAG.

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

`cdk deploy` prints `BucketName`, `DatabaseName`, `JobName`, and `GlueJobRoleArn` as
stack outputs. Load them into shell variables for the steps below:

```bash
export AWS_REGION=<region>
export PREFIX=orders-pipeline    # or your -c prefix override

stack_output() {
  aws cloudformation describe-stacks --stack-name DeclarativeEtlPipelineUsingGlueStack \
    --region "$AWS_REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}
export BUCKET=$(stack_output BucketName)
export JOB_NAME=$(stack_output JobName)
export DATABASE=$(stack_output DatabaseName)
```

## Helper scripts (optional)

Wrapper scripts in [`scripts/`](scripts/) run the same steps with a named AWS profile.
Pass `--profile <your-aws-profile>` (or export `AWS_PROFILE`); the region comes from
`--region`, `AWS_REGION`, or the profile's configured region.

```bash
scripts/deploy.sh      --profile <your-aws-profile> --bootstrap   # --bootstrap: first time only
scripts/upload_data.sh --profile <your-aws-profile>               # uploads sample_data/orders.csv
scripts/upload_data.sh --profile <your-aws-profile> a.csv b.csv   # or your own files
scripts/destroy.sh     --profile <your-aws-profile>               # asks you to confirm the account ID
```

- `deploy.sh` runs `cdk deploy --require-approval never` (no IAM-change prompt) and prints
  the stack outputs. Extra `cdk deploy` args go after `--`, e.g. `-- -c environment_tag=prod`.
- `upload_data.sh` reads the bucket from the `BucketName` stack output and writes to
  `<prefix>/input/` (`--prefix` defaults to `orders-pipeline`).
- `destroy.sh` deletes the stack and every object in the bucket; `--yes` skips the prompt.

## Post-deploy: upload sample data

Not part of the CDK stack — a manual step, run once per environment:

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
