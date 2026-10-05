import aws_cdk as core
import aws_cdk.assertions as assertions

from declarative_etl_pipeline_using_glue.declarative_etl_pipeline_using_glue_stack import (
    DeclarativeEtlPipelineUsingGlueStack,
)
from declarative_etl_pipeline_using_glue.pipeline_stack_props import PipelineStackProps


def _synth_template() -> assertions.Template:
    app = core.App()
    props = PipelineStackProps(
        bucket_name="glue-sdp-demo-test",
        prefix="orders-pipeline",
        database_name="sdp_demo_db",
        job_name="orders-sdp-job",
    )
    stack = DeclarativeEtlPipelineUsingGlueStack(app, "TestStack", props=props)
    return assertions.Template.from_stack(stack)


def test_bucket_is_encrypted_and_private():
    template = _synth_template()
    template.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "BucketEncryption": {
                "ServerSideEncryptionConfiguration": [
                    {"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
                ]
            },
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True,
            },
        },
    )


def test_bucket_enforces_ssl():
    template = _synth_template()
    template.has_resource_properties(
        "AWS::S3::BucketPolicy",
        {
            "PolicyDocument": {
                "Statement": assertions.Match.array_with(
                    [
                        assertions.Match.object_like(
                            {
                                "Effect": "Deny",
                                "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                            }
                        )
                    ]
                )
            }
        },
    )


def test_glue_role_trust_policy_and_managed_policy():
    template = _synth_template()
    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "AssumeRolePolicyDocument": {
                "Statement": [
                    assertions.Match.object_like(
                        {
                            "Principal": {"Service": "glue.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    )
                ]
            },
            "ManagedPolicyArns": assertions.Match.array_with(
                [
                    assertions.Match.object_like(
                        {
                            "Fn::Join": assertions.Match.array_with(
                                [
                                    assertions.Match.array_with(
                                        [assertions.Match.string_like_regexp("AWSGlueServiceRole")]
                                    )
                                ]
                            )
                        }
                    )
                ]
            ),
        },
    )


def test_glue_role_inline_policy_is_scoped_not_wildcard():
    template = _synth_template()
    policies = template.find_resources("AWS::IAM::Policy")
    assert policies, "expected an inline IAM policy on the Glue role"

    for policy in policies.values():
        statements = policy["Properties"]["PolicyDocument"]["Statement"]
        for statement in statements:
            resource = statement.get("Resource")
            if resource is None:
                continue
            resources = resource if isinstance(resource, list) else [resource]
            for res in resources:
                if res != "*":
                    continue
                # Accepted exception: CloudWatch PutMetricData has no resource-level
                # ARN support at all (AWS's own Glue Data Quality IAM docs use
                # Resource "*" for it too) -- the namespace condition is the
                # compensating control, same spirit as the AWSGlueServiceRole
                # managed-policy exception documented in README's Security section.
                actions = statement.get("Action")
                actions = [actions] if isinstance(actions, str) else (actions or [])
                is_cloudwatch_metrics_exception = actions == [
                    "cloudwatch:PutMetricData"
                ] and statement.get("Condition") == {
                    "StringEquals": {"cloudwatch:namespace": "Glue Data Quality"}
                }
                assert is_cloudwatch_metrics_exception, (
                    "inline policy resource must not be '*' except the documented "
                    "cloudwatch:PutMetricData exception"
                )


def test_dq_rulesets_target_correct_tables():
    template = _synth_template()
    template.has_resource_properties(
        "AWS::Glue::DataQualityRuleset",
        {
            "Name": "orders-sdp-job-silver-dq",
            "TargetTable": {"TableName": "silver_orders", "DatabaseName": "sdp_demo_db"},
        },
    )
    template.has_resource_properties(
        "AWS::Glue::DataQualityRuleset",
        {
            "Name": "orders-sdp-job-gold-dq",
            "TargetTable": {
                "TableName": "gold_sales_summary",
                "DatabaseName": "sdp_demo_db",
            },
        },
    )


def test_dq_trigger_rule_fires_only_on_job_success():
    template = _synth_template()
    template.has_resource_properties(
        "AWS::Events::Rule",
        {
            "EventPattern": {
                "source": ["aws.glue"],
                "detail-type": ["Glue Job State Change"],
                "detail": {"jobName": ["orders-sdp-job"], "state": ["SUCCEEDED"]},
            }
        },
    )


def test_glue_database_has_location_uri():
    template = _synth_template()
    template.has_resource_properties(
        "AWS::Glue::Database",
        {
            "DatabaseInput": {
                "Name": "sdp_demo_db",
                "LocationUri": "s3://glue-sdp-demo-test/orders-pipeline/warehouse/",
            }
        },
    )


def test_glue_job_uses_version_6_and_sdp_flags():
    template = _synth_template()
    template.has_resource_properties(
        "AWS::Glue::Job",
        {
            "Name": "orders-sdp-job",
            "GlueVersion": "6.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 2,
            "ExecutionClass": "FLEX",
            "DefaultArguments": {
                "--enable-spark-declarative-pipeline": "true",
                "--enable-glue-datacatalog": "true",
            },
        },
    )
