from pathlib import Path

from aws_cdk import (
    CfnOutput,
    RemovalPolicy,
    Stack,
    Tags,
    aws_events as events,
    aws_events_targets as events_targets,
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
DQ_RULES_DIR = REPO_ROOT / "dq_rules"


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
        job.add_resource_dependency(database)

        # ======================================================================
        # Data quality
        # Post-pipeline DQ, observability-only: two AWS Glue Data Quality rulesets
        # attached directly to silver_orders/gold_sales_summary, triggered by an
        # EventBridge rule on orders-sdp-job's own SUCCEEDED event -- never part of
        # the Glue job itself, so a DQ failure can never fail the pipeline, and the
        # rule structurally cannot fire before SDP has materialized the tables at
        # least once (solves first-deploy bootstrapping with no table-exists check).
        # DQDL rule bodies live in dq_rules/ as separate files, same pattern as
        # pipeline_src/spark-pipeline.yml, rather than inline Python strings here.
        # ======================================================================
        silver_dq_ruleset_name = f"{props.job_name}-silver-dq"
        gold_dq_ruleset_name = f"{props.job_name}-gold-dq"

        silver_dq_ruleset = glue.CfnDataQualityRuleset(
            self,
            "SilverOrdersDqRuleset",
            name=silver_dq_ruleset_name,
            description="Observability-only DQ rules for silver_orders.",
            ruleset=(DQ_RULES_DIR / "silver_orders.dqdl").read_text(),
            target_table=glue.CfnDataQualityRuleset.DataQualityTargetTableProperty(
                table_name="silver_orders",
                database_name=props.database_name,
            ),
        )
        silver_dq_ruleset.add_resource_dependency(database)

        gold_dq_ruleset = glue.CfnDataQualityRuleset(
            self,
            "GoldSalesSummaryDqRuleset",
            name=gold_dq_ruleset_name,
            description="Observability-only DQ rules for gold_sales_summary.",
            ruleset=(DQ_RULES_DIR / "gold_sales_summary.dqdl").read_text(),
            target_table=glue.CfnDataQualityRuleset.DataQualityTargetTableProperty(
                table_name="gold_sales_summary",
                database_name=props.database_name,
            ),
        )
        gold_dq_ruleset.add_resource_dependency(database)

        # The Role passed to each DQ evaluation run (reusing GlueJobRole, which
        # already has S3 access to this bucket) needs the real DQ data-plane actions.
        # glue:BatchPutDataQualityStatistic is NOT a real Glue IAM action -- the
        # correct action for publishing evaluation results is glue:PublishDataQuality,
        # per AWS's own IAM reference for Glue Data Quality. dataQualityRuleset/* is
        # AWS's own documented minimal-policy scoping for these actions (DQ rulesets
        # don't support tighter per-resource scoping than their resource type).
        dq_ruleset_arn_pattern = self.format_arn(
            service="glue",
            resource="dataQualityRuleset",
            resource_name="*",
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetDataQualityRuleset",
                    "glue:GetDataQualityRulesetEvaluationRun",
                    "glue:PublishDataQuality",
                    "glue:GetDataQualityResult",
                ],
                resources=[dq_ruleset_arn_pattern],
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["glue:GetTable", "glue:GetDatabase", "glue:GetPartitions"],
                resources=[
                    self.format_arn(service="glue", resource="catalog"),
                    self.format_arn(
                        service="glue", resource="database", resource_name=props.database_name
                    ),
                    self.format_arn(
                        service="glue",
                        resource="table",
                        resource_name=f"{props.database_name}/silver_orders",
                    ),
                    self.format_arn(
                        service="glue",
                        resource="table",
                        resource_name=f"{props.database_name}/gold_sales_summary",
                    ),
                ],
            )
        )
        # CloudWatch PutMetricData has no resource-level ARN support at all (confirmed
        # against AWS's own example DQ policy) -- Resource "*" is unavoidable here, so
        # the namespace condition is the compensating control, same spirit as the
        # AWSGlueServiceRole "accepted exception" documented in README's Security
        # section.
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
                conditions={"StringEquals": {"cloudwatch:namespace": "Glue Data Quality"}},
            )
        )
        role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject", "s3:GetObject"],
                resources=[bucket.arn_for_objects(f"{props.prefix}/dq-results/*")],
            )
        )

        # EventBridge rule: fires only on orders-sdp-job SUCCEEDED, so DQ runs
        # automatically right after each successful pipeline run without touching the
        # job itself. AwsApi targets are CDK-managed (a small CDK-generated Lambda
        # makes the actual API call); each gets an explicit scoped policy_statement
        # rather than CDK's auto-derived one, since the auto-derived permission can't
        # know to scope to dataQualityRuleset/* instead of "*".
        dq_trigger_rule = events.Rule(
            self,
            "DqTriggerRule",
            description="Runs post-pipeline DQ evaluation whenever orders-sdp-job succeeds.",
            event_pattern=events.EventPattern(
                source=["aws.glue"],
                detail_type=["Glue Job State Change"],
                detail={"jobName": [props.job_name], "state": ["SUCCEEDED"]},
            ),
        )
        dq_api_policy_statement = iam.PolicyStatement(
            actions=["glue:StartDataQualityRulesetEvaluationRun"],
            resources=[dq_ruleset_arn_pattern],
        )
        dq_trigger_rule.add_target(
            events_targets.AwsApi(
                service="Glue",
                action="startDataQualityRulesetEvaluationRun",
                policy_statement=dq_api_policy_statement,
                parameters={
                    "DataSource": {
                        "GlueTable": {
                            "DatabaseName": props.database_name,
                            "TableName": "silver_orders",
                        }
                    },
                    "Role": role.role_arn,
                    "RulesetNames": [silver_dq_ruleset_name],
                    "AdditionalRunOptions": {
                        "CloudWatchMetricsEnabled": True,
                        "ResultsS3Prefix": f"s3://{props.bucket_name}/{props.prefix}/dq-results/silver/",
                    },
                },
            )
        )
        dq_trigger_rule.add_target(
            events_targets.AwsApi(
                service="Glue",
                action="startDataQualityRulesetEvaluationRun",
                policy_statement=dq_api_policy_statement,
                parameters={
                    "DataSource": {
                        "GlueTable": {
                            "DatabaseName": props.database_name,
                            "TableName": "gold_sales_summary",
                        }
                    },
                    "Role": role.role_arn,
                    "RulesetNames": [gold_dq_ruleset_name],
                    "AdditionalRunOptions": {
                        "CloudWatchMetricsEnabled": True,
                        "ResultsS3Prefix": f"s3://{props.bucket_name}/{props.prefix}/dq-results/gold/",
                    },
                },
            )
        )

        # ======================================================================
        # Tags & outputs
        # ======================================================================
        Tags.of(self).add("Project", props.project_tag)
        Tags.of(self).add("Environment", props.environment_tag)
        Tags.of(self).add("Owner", props.owner_tag)

        CfnOutput(self, "BucketName", value=bucket.bucket_name)
        CfnOutput(self, "Prefix", value=props.prefix)
        CfnOutput(self, "DatabaseName", value=props.database_name)
        CfnOutput(self, "JobName", value=props.job_name)
        CfnOutput(self, "GlueJobRoleArn", value=role.role_arn)
        CfnOutput(self, "SilverDqRulesetName", value=silver_dq_ruleset_name)
        CfnOutput(self, "GoldDqRulesetName", value=gold_dq_ruleset_name)
