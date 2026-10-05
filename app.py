#!/usr/bin/env python3
import os
import random
import string

import aws_cdk as cdk

from declarative_etl_pipeline_using_glue.declarative_etl_pipeline_using_glue_stack import (
    DeclarativeEtlPipelineUsingGlueStack,
)
from declarative_etl_pipeline_using_glue.pipeline_stack_props import PipelineStackProps

app = cdk.App()


def _random_suffix(length: int = 6) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=length))


# Naming/sizing knobs are read from CDK context, not hardcoded, so the same app.py
# works across accounts/environments (CLAUDE.md section 3). Set durable per-checkout
# values under "context" in cdk.json, or override per-run with `-c key=value`, e.g.:
#   cdk deploy -c bucket_name=my-bucket -c environment_tag=prod
# The `or` fallbacks below are dev defaults so a first `cdk deploy` with no context
# works out of the box; bucket_name must be globally unique, so override it before
# deploying to a shared account.
bucket_base_name = app.node.try_get_context("bucket_name") or "glue-sdp-demo"

# S3 bucket names are globally unique across all AWS accounts, so a fixed base name
# alone will collide. A random 6-letter suffix is appended to make it unique. The
# suffix is read from the "bucket_suffix" context key so it stays stable across
# `cdk synth`/`deploy` runs (a bucket_name that changes on every synth would make
# CloudFormation replace the bucket -- losing its contents -- instead of updating it
# in place). If "bucket_suffix" isn't set yet, one is generated here; set it in
# cdk.json (or pass -c bucket_suffix=<value>) once you've deployed, so it doesn't
# regenerate on the next run.
bucket_suffix = app.node.try_get_context("bucket_suffix") or _random_suffix()

props = PipelineStackProps(
    bucket_name=f"{bucket_base_name}-{bucket_suffix}",
    prefix=app.node.try_get_context("prefix") or "orders-pipeline",
    database_name=app.node.try_get_context("database_name") or "sdp_demo_db",
    job_name=app.node.try_get_context("job_name") or "orders-sdp-job",
    worker_type=app.node.try_get_context("worker_type") or "G.1X",
    num_workers=int(app.node.try_get_context("num_workers") or 2),
    environment_tag=app.node.try_get_context("environment_tag") or "dev",
    owner_tag=app.node.try_get_context("owner_tag") or "de-team",
)

DeclarativeEtlPipelineUsingGlueStack(
    app,
    "DeclarativeEtlPipelineUsingGlueStack",
    props=props,
    # Account/region are left to resolve from whichever AWS CLI profile is passed at
    # deploy time (e.g. `cdk deploy --profile <your-aws-profile>`), or explicitly via
    # CDK_DEFAULT_ACCOUNT/CDK_DEFAULT_REGION env vars set by the CDK CLI itself.
    env=cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region=os.environ.get("CDK_DEFAULT_REGION"),
    ),
)

app.synth()
