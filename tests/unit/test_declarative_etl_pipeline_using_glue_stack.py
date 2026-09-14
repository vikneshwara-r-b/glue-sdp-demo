import aws_cdk as core
import aws_cdk.assertions as assertions

from declarative_etl_pipeline_using_glue.declarative_etl_pipeline_using_glue_stack import DeclarativeEtlPipelineUsingGlueStack

# example tests. To run these tests, uncomment this file along with the example
# resource in declarative_etl_pipeline_using_glue/declarative_etl_pipeline_using_glue_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = DeclarativeEtlPipelineUsingGlueStack(app, "declarative-etl-pipeline-using-glue")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
