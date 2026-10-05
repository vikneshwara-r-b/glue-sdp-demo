# CLAUDE.md — AWS Glue 6.0 Declarative ETL Pipeline (CDK / Python)

This file gives Claude Code the full context needed to build, test, and maintain the
Infrastructure-as-Code (CDK, Python) for a **Glue 6.0 Spark Declarative Pipeline (SDP)**
project, based on the AWS Big Data Blog post:
https://aws.amazon.com/blogs/big-data/build-declarative-etl-pipelines-with-aws-glue-6-0/

Read this whole file before generating or modifying code. When in doubt, prefer the
architecture and defaults described here over inventing new patterns.

---

## 1. What we're building

A single AWS Glue 6.0 ETL job that reads raw order records from CSV in S3 and produces
three Data-Catalog-registered tables through a medallion (bronze → silver → gold)
sequence — **with no hand-written orchestration**. Instead of 3 Glue jobs + an
orchestrator (Step Functions/MWAA), Spark Declarative Pipelines (SDP) infers the
dependency graph from table references in the code and runs everything as one job.

Layers:
- **bronze_orders** (streaming table, Python — `dp.create_streaming_table()` +
  `@dp.append_flow(target="bronze_orders")`) — raw CSV ingested incrementally via
  `spark.readStream`, all columns typed as `StringType` (no coercion, no filtering).
- **silver_orders** (streaming table, Python — `dp.create_streaming_table()` +
  `@dp.append_flow(target="silver_orders")`) — reads `spark.readStream.table("bronze_orders")`,
  casts types, filters to `status == COMPLETE` and `amount > 0`, derives an
  `amount_band` column (`large` ≥500, `medium` ≥100, else `small`).
- **gold_sales_summary** (SQL materialized view) — aggregates `silver_orders` by
  `region` (order_count, total_sales, average_order_value). Stays a materialized view:
  it's an aggregation/report, exactly the case the docs say MVs are for.

SDP resolves bronze → silver → gold ordering automatically from these table references;
we never write a DAG. Bronze and silver are streaming tables (not materialized views) so
each run processes only new data since the last checkpoint — per AWS's SDP docs
(https://docs.aws.amazon.com/glue/latest/dg/spark-declarative-pipelines.html), this needs
their checkpoint/`_spark_metadata` state to persist in S3, which is already satisfied by
the Glue database's `LocationUri` (see item 3 below) — no extra CDK change required.
`dp.create_streaming_table()` + `@dp.append_flow(target=...)` is the Python API Glue's
docs specify for streaming tables; do not use a bare `@dp.table` decorator instead — it
is not the documented/supported syntax for this product.

Two independent runtime controls (set via `--conf` job arguments, not CDK-managed
resources, but the CDK code must make it easy to pass them at `start-job-run` time):
- `spark.glue.sdp.jobMode` = `VALIDATE` (dry run, no data written) or `RUN` (default).
- `spark.glue.sdp.runMode` = `--refresh <ds>`, `--full-refresh <ds>`, or `--full-refresh-all`.

---

## 2. Scope of the IaC (what CDK must provision)

1. **S3 bucket** — private, SSE-encrypted (SSE-S3 or KMS), Block Public Access fully
   enabled, versioning recommended (blog's "Automate" section suggests bucket
   versioning if not zip-versioning by object key). Holds:
   - `<prefix>/input/orders.csv` (sample data)
   - `<prefix>/state/` (SDP pipeline storage/state — must exist as a prefix, no object
     needed)
   - `<prefix>/warehouse/` (Glue Data Catalog database `LocationUri`)
   Use an explicit `bucket_name` (not CDK auto-generated) so the name is known at synth
   time and can be rendered into `pipeline_src/spark-pipeline.yml` (see item 4) before
   packaging. S3 bucket names are globally unique across all AWS accounts, so `app.py`
   appends a random 6-letter suffix (`bucket_suffix` context key) to the configured
   `bucket_name`: `<bucket_name>-<bucket_suffix>`. The suffix is generated once and
   should then be pinned in `cdk.json` (or via `-c bucket_suffix=...`) rather than left
   to regenerate — a bucket name that changes on every `cdk synth` makes CloudFormation
   replace the bucket (and lose its contents) instead of updating it in place.
   Note: the packaged pipeline artifact itself is **not** stored under this bucket. CDK's
   `aws_s3_assets.Asset` uploads it to the account's CDK bootstrap staging bucket
   (`cdk-<qualifier>-assets-<account>-<region>`), and `CfnJob.ScriptLocation` points
   there — see item 4.
2. **IAM role** for Glue to assume:
   - Trust policy: principal `glue.amazonaws.com`, action `sts:AssumeRole`.
   - Attach AWS managed policy `AWSGlueServiceRole`.
   - Inline policy scoping `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`,
     `s3:ListBucket` to the bucket ARN and `bucket/*` ARN only (least privilege — do
     not grant account-wide S3 access).
3. **Glue Data Catalog database** — must exist *before* the pipeline runs (SDP does not
   create it). Needs an explicit `LocationUri` pointing at the `warehouse/` prefix.
   Use `aws_glue.CfnDatabase` (L1) since this is a simple metadata object.
4. **Pipeline artifact packaging** — source lives under `pipeline_src/`:
   `spark-pipeline.yml` at its root, `transformations/01_bronze.py`,
   `transformations/02_silver.py`, `transformations/03_gold.sql` in a subdirectory.
   Bucket/prefix/database reach these files by two different mechanisms, chosen per
   what each file is able to read:
   - `spark-pipeline.yml` is static YAML the SDP runtime reads **before** any Python
     job-argument code executes, so it cannot pull values from Glue job arguments.
     `pipeline_src/spark-pipeline.yml` is committed as a `{bucket}`/`{prefix}`/
     `{database}` `str.format` template (no real values), rendered into
     `build/package/spark-pipeline.yml` at **packaging time**
     (`scripts/package_pipeline.py::render_package`) using `PipelineStackProps`, so
     bucket/prefix/database are defined in exactly one place (the stack's props) and
     never hand-duplicated as literals.
   - `transformations/*.py` are copied into the build directory **unchanged** — no
     file-content substitution. Glue job arguments do NOT reach them (SDP runs them via
     its own CLI wrapper, so `sys.argv` has no `--BUCKET`/`--PREFIX`; `getResolvedOptions`
     fails). Instead the rendered `spark-pipeline.yml` carries a `configuration:` entry
     `orders.input.path: s3://{bucket}/{prefix}/input/`, and `01_bronze.py`
     reads it with `spark.conf.get("orders.input.path")`. This must be the input
     **directory**, not a specific file: `bronze_orders` is a streaming table reading via
     `spark.readStream`, and Spark's file-streaming source requires `.load(path)` to
     point at a directory it can monitor for new files — pointing it at a literal file
     fails with `Option 'basePath' must be a directory`. `01_bronze.py` scopes the read
     to CSVs with `.option("pathGlobFilter", "*.csv")`. `03_gold.sql` needs no
     bucket/prefix/database at all; it only references the already-registered
     `silver_orders` table.
   CDK must:
   - Render `pipeline_src/` into a `build/package/` directory at synth time (since the
     bucket is explicitly named per item 1, bucket/prefix/database are plain Python
     strings available before synth) — `scripts/package_pipeline.py::render_package`.
     `render_package` produces a plain directory, not a zip.
   - Pass that directory to `aws_s3_assets.Asset`, which zips it and uploads the zip to
     the CDK bootstrap assets bucket; point `CfnJob.ScriptLocation` at
     `pipeline_asset.s3_object_url`.
   - `spark-pipeline.yml` and `transformations/` must end up at the zip **root** — i.e.
     at the top of `build/package/`, since `Asset` zips exactly what it's pointed at.
5. **Glue Job** (`aws_glue.CfnJob`, L1 — the SDP flags are new and unlikely to be
   exposed on any L2 construct yet, always check current CDK docs before assuming
   otherwise):
   - `Command.Name = "glueetl"`, `Command.ScriptLocation` = S3 path to the zip (or an
     unzipped prefix ending in `/`), `Command.PythonVersion = "3"`.
   - `GlueVersion = "6.0"`.
   - `WorkerType = "G.1X"`, `NumberOfWorkers = 2` (defaults from the walkthrough —
     make these configurable props).
   - `DefaultArguments`:
     - `--enable-spark-declarative-pipeline: "true"` (required — activates SDP)
     - `--enable-glue-datacatalog: "true"` (registers output tables in Data Catalog)
   - `Role` = the IAM role ARN from step 2.
6. **Sample data upload** — **manual/CLI step, not part of the CDK stack.** Do not
   provision a `BucketDeployment` (or any construct) for `orders.csv`. Document the
   `aws s3 cp sample_data/orders.csv s3://<bucket>/<prefix>/input/orders.csv` command in
   the README as a post-deploy step the user runs themselves. `sample_data/` is
   gitignored (see `.gitignore`) — it's local sample content, not committed source.
7. **Outputs** — CfnOutput for bucket name, prefix, database name, job name, role ARN,
   so the user can immediately run `aws glue start-job-run` for validate/run steps. The
   `Prefix` output lets `scripts/deploy.sh --upload` read back the actually-deployed
   prefix instead of hardcoding it, so it can't drift from a `-c prefix=...` override.

### Explicitly NOT part of the CDK stack (operational, not infra)
- Triggering `VALIDATE` / `RUN` job runs — these are `start-job-run` CLI/SDK calls, not
  CloudFormation resources. Document them as `Makefile`/README commands, not CDK code.
- Querying via Athena — downstream consumer concern, not IaC.
- The manifest-rendering/directory-layout build is a small Python script
  (`scripts/package_pipeline.py`), invoked from CDK before `aws_s3_assets.Asset` points
  at the pre-built `build/package/` dir, keeping CDK declarative and the packaging
  logic separate/testable.

---

## 3. Coding standards for this repo

- **Language/tooling**: Python 3.11+, `aws-cdk-lib` v2 (pin an exact version in
  `requirements.txt`), `constructs` v10. Use `cdk.json` with the standard CDK Python
  app layout (`app.py` at root).
- **Type hints everywhere**; run `mypy` if available.
- Prefer **L1 (`Cfn*`) constructs for Glue resources** (`CfnJob`, `CfnDatabase`) since
  SDP-related job arguments and GlueVersion 6.0 support may lag in L2 constructs —
  verify current L2 support before using it; don't assume.
- Use a **stack props dataclass** (bucket name, prefix, database name, job name, region,
  worker count/type, environment) rather than hardcoded literals in the stack — mirror
  the blog's exported shell variables (`AWS_REGION`, `BUCKET`, `PREFIX`, `DATABASE`,
  `ROLE_ARN`, `JOB_NAME`) as CDK props/context.
- **No hardcoded account IDs, bucket names, or ARNs** in code — pass via CDK context
  (`cdk.json` / `-c` flags) or environment variables, with sane local defaults for dev.
  Implemented in `app.py`: every `PipelineStackProps` field is read via
  `app.node.try_get_context(...)` with an `or "<dev-default>"` fallback, and
  account/region come from `CDK_DEFAULT_ACCOUNT`/`CDK_DEFAULT_REGION` (set by the CDK
  CLI from the active profile). Override per-run with `cdk deploy -c bucket_name=...`,
  or add durable values under `"context"` in `cdk.json`. `bucket_name` is further
  combined with a random `bucket_suffix` for S3 global-uniqueness — see item 1 in
  section 2.
- **No PII or personal identifiers** in committed source or defaults — this includes
  personal usernames/emails used as example `owner_tag` values and personal AWS
  profile names in comments. Use generic placeholders (e.g. `de-team`,
  `<your-aws-profile>`) instead, even for "just a dev default."
- **IAM least privilege**: scope every inline policy statement's `Resource` to the
  specific bucket/prefix ARNs — never `"*"` on S3 actions. The one broader grant this
  stack carries (`AWSGlueServiceRole`, required by the Glue service itself) is explained
  inline as a comment on the `GlueJobRole` construct, not in a separate doc file.
- **Bucket security**: `block_public_access=BlockPublicAccess.BLOCK_ALL`,
  `encryption=BucketEncryption.S3_MANAGED` (or KMS if the org requires CMKs),
  `enforce_ssl=True`.
- **Idempotent packaging**: `scripts/package_pipeline.py` must be safe to re-run — clean
  the `build/` dir first, never mutate `pipeline_src/`.
- **Testing**: every construct should have a corresponding CDK fine-grained assertion
  test (`aws_cdk.assertions.Template`) checking key properties (GlueVersion, worker
  count, DefaultArguments flags, bucket encryption/public-access settings, IAM trust
  policy principal).
- **Multi-environment support**: structure so `dev`/`prod` can use different
  bucket/prefix/database names via context, without duplicating stack code.
- **Tag all resources** (e.g. `Project`, `Environment`, `Owner`) for cost tracking.
- Write a `README.md` documenting: prerequisites, `cdk bootstrap`, `cdk deploy`,
  post-deploy validate/run commands (from Section 5 below), and `cdk destroy` cleanup.

---

## 4. Key facts from the blog Claude Code must respect

- Bronze reads **every column as string** by design — no type coercion at bronze.
- Silver's `to_timestamp` uses an **explicit format string** (`"yyyy-MM-dd'T'HH:mm:ss'Z'"`)
  to keep parsing deterministic regardless of session time zone — do not let generated
  code drop this format string.
- Silver derives `amount_band` via a **second `.select(...)` projection** (not
  `.withColumn`) reading the already-cast `amount` column — Spark 4.1 has ANSI mode on
  by default, so comparing the raw string `amount` against a number would fail. Any
  generated transformation code should preserve this two-projection pattern.
- The Glue Data Catalog **database must already exist** (with `LocationUri`) before the
  job runs; SDP will not create it.
- `ScriptLocation` in `CfnJob` must point to the **zip file** (or an unzipped S3 prefix
  ending in `/`), not a single `.py` file.
- `spark-pipeline.yml`'s bucket/prefix/database are substituted **at packaging time**
  (SDP reads this file before any job-argument code runs, so it has no other way to get
  these values). `transformations/*.py` read them from Spark conf instead
  (`spark.conf.get("orders.input.path")`, set by the manifest's `configuration:` block) —
  the files themselves are copied unchanged, never token-substituted. Do not use
  `getResolvedOptions`/`sys.argv` in transformation files: Glue job arguments don't reach them.
- Materialized views **always fully recompute**; only streaming tables support
  incremental refresh/checkpoints. `bronze_orders` and `silver_orders` are streaming
  tables (`dp.create_streaming_table()` + `@dp.append_flow(target=...)`); their
  checkpoint/`_spark_metadata` state lives in S3 via the Glue database's `LocationUri` —
  not locally. `gold_sales_summary` stays a materialized view since it's a pure
  aggregation/report, which the docs call out as the materialized-view use case.
- `VALIDATE` mode does dependency/SQL/Python compilation checks and writes **no data** —
  useful as a safe post-deploy smoke test before the first real `RUN`.

---

## 5. Operational commands (not CDK, but document in README/Makefile)

```bash
# Deploy infra
cdk deploy

# Upload sample data (manual step — not part of the CDK stack)
aws s3 cp sample_data/orders.csv "s3://$BUCKET/$PREFIX/input/orders.csv" --region "$AWS_REGION"

# Dry-run validate the pipeline (no data written)
aws glue start-job-run --job-name "$JOB_NAME" \
  --arguments '{"--conf":"spark.glue.sdp.jobMode=VALIDATE"}' --region "$AWS_REGION"

# Run the pipeline for real
aws glue start-job-run --job-name "$JOB_NAME" \
  --arguments '{"--conf":"spark.glue.sdp.jobMode=RUN"}' --region "$AWS_REGION"

# Full refresh: reset every table (including streaming-table checkpoints) and
# recompute from scratch -- a runtime job argument, no CDK/infra change involved.
# jobMode and runMode are both `--conf` settings, so they're combined into ONE
# `--conf` value (space-separated), not two separate JSON keys -- `--arguments` is a
# flat map and can only hold one `--conf` entry.
aws glue start-job-run --job-name "$JOB_NAME" \
  --arguments '{"--conf":"spark.glue.sdp.jobMode=RUN --conf spark.glue.sdp.runMode=--full-refresh-all"}' \
  --region "$AWS_REGION"

# Inspect materialized tables
aws glue get-tables --database-name "$DATABASE" --region "$AWS_REGION" \
  --query 'TableList[].Name' --output table

# Tear down
cdk destroy
```

---

## 6. Definition of done for a first working version

- [ ] `cdk synth` produces a template with: 1 S3 bucket (encrypted, public-access
      blocked), 1 IAM role (correct trust + managed + inline policies), 1
      `CfnDatabase`, 1 `CfnJob` with GlueVersion 6.0 and both SDP `DefaultArguments`
      flags set to `"true"`.
- [ ] `scripts/package_pipeline.py` renders `pipeline_src/spark-pipeline.yml`'s
      `{bucket}`/`{prefix}`/`{database}` template correctly and produces a
      `build/package/` directory with `spark-pipeline.yml` and `transformations/` at
      its root (ready for `aws_s3_assets.Asset` to zip); covered by a unit test.
- [ ] `pytest tests/` passes, including CDK `Template` assertions.
- [ ] README documents the full deploy → validate → run → query → destroy loop.
- [ ] No secrets, account IDs, or bucket names are hardcoded in committed source —
      `app.py` reads `PipelineStackProps` fields from CDK context with dev-only
      fallbacks, overridable via `-c key=value` or `cdk.json` `"context"`.
- [ ] No PII or personal identifiers (personal usernames, emails, AWS profile names)
      appear anywhere in committed source, comments, or docs — only generic
      placeholders/team names.
- [ ] The deployed bucket name is `<bucket_name>-<bucket_suffix>` with a pinned
      6-letter `bucket_suffix` in `cdk.json`, not regenerated on every synth.

When implementing, ask the user for their target AWS account/region and desired
bucket/prefix/database naming convention before hardcoding any defaults beyond the
blog's example values.