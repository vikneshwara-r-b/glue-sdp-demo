from pathlib import Path

from aws_cdk import (
    CfnOutput,
    RemovalPolicy,
    Stack,
    Tags,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_assets as s3_assets,
)
from constructs import Construct

from scripts.package_pipeline import render_package

from .pipeline_stack_props import PipelineStackProps

REPO_ROOT = Path(__file__).resolve().parent.parent
PIPELINE_SRC_DIR = REPO_ROOT / "pipeline_src"
PIPELINE_BUILD_DIR = REPO_ROOT / "build" / "package"


class DeclarativeEtlPipelineUsingGlueStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        props: PipelineStackProps,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ======================================================================
        # Storage
        # A single private, encrypted bucket holds the pipeline's input data,
        # packaged code, SDP state, and Data Catalog warehouse -- see the prefix
        # layout in CLAUDE.md section 2. Versioning + DESTROY/auto-delete keep a
        # sandbox project easy to tear down without leaving orphaned objects.
        # ======================================================================
        bucket = s3.Bucket(
            self,
            "PipelineBucket",
            bucket_name=props.bucket_name,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=True,
            # Hobby/sandbox project: allow the bucket (and its objects) to be
            # cleaned up on `cdk destroy` instead of retaining it.
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # ======================================================================
        # IAM
        # Single Glue job role. AWSGlueServiceRole is an AWS-managed policy
        # required by the Glue service itself for its own operational needs
        # (logging, catalog calls, ENI management); hand-rolling an equivalent
        # inline policy would just duplicate what AWS maintains, with no security
        # benefit. Actual *data* access is scoped separately below: the inline
        # statements grant only this bucket's ARNs, never "*".
        # ======================================================================
        role = iam.Role(
            self,
            "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
            ],
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                resources=[bucket.arn_for_objects("*")],
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:ListBucket"],
                resources=[bucket.bucket_arn],
            )
        )

        # ======================================================================
        # Data Catalog
        # SDP registers bronze/silver/gold tables into this database at run
        # time but does not create the database itself, so it must exist (with
        # a LocationUri) before the first job run.
        # ======================================================================
        database = glue.CfnDatabase(
            self,
            "PipelineDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=props.database_name,
                location_uri=f"s3://{props.bucket_name}/{props.prefix}/warehouse/",
            ),
        )

        # ======================================================================
        # Pipeline packaging
        # Render pipeline_src/ -> build/package/. Only spark-pipeline.yml is rendered
        # (bucket/prefix/database filled in at synth time); transformations/ is copied
        # unchanged. Glue reads the resulting zip as-is at run time.
        # ======================================================================
        render_package(
            bucket=props.bucket_name,
            prefix=props.prefix,
            database=props.database_name,
            src_dir=PIPELINE_SRC_DIR,
            out_dir=PIPELINE_BUILD_DIR,
        )

        pipeline_asset = s3_assets.Asset(
            self,
            "PipelineAsset",
            path=str(PIPELINE_BUILD_DIR),
        )
        pipeline_asset.grant_read(role)

        # ======================================================================
        # Compute
        # One Glue 6.0 job with Spark Declarative Pipelines enabled; SDP infers
        # the bronze -> silver -> gold dependency graph from table references in
        # transformations/, so no separate orchestration resource is provisioned.
        # ======================================================================
        job = glue.CfnJob(
            self,
            "PipelineJob",
            name=props.job_name,
            role=role.role_arn,
            glue_version="6.0",
            worker_type=props.worker_type,
            number_of_workers=props.num_workers,
            # Flex runs on spare capacity at a lower price; start time isn't guaranteed.
            execution_class="FLEX",
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=pipeline_asset.s3_object_url,
                python_version="3",
            ),
            default_arguments={
                "--enable-spark-declarative-pipeline": "true",
                "--enable-glue-datacatalog": "true",
            },
        )
        job.add_dependency(database)

        # ======================================================================
        # Tags & outputs
        # ======================================================================
        Tags.of(self).add("Project", props.project_tag)
        Tags.of(self).add("Environment", props.environment_tag)
        Tags.of(self).add("Owner", props.owner_tag)

        CfnOutput(self, "BucketName", value=bucket.bucket_name)
        CfnOutput(self, "DatabaseName", value=props.database_name)
        CfnOutput(self, "JobName", value=props.job_name)
        CfnOutput(self, "GlueJobRoleArn", value=role.role_arn)
