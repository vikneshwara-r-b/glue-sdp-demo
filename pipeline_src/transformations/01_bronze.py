from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# All columns are ingested as-is, no coercion or filtering at bronze.
ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("status", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("order_ts", StringType(), True),
    ]
)

# The input path comes from the `configuration:` block of spark-pipeline.yml (rendered
# at packaging time). Glue job arguments don't reach this file: SDP runs it through its
# own CLI wrapper, so sys.argv doesn't carry --BUCKET/--PREFIX.
INPUT_PATH_CONF = "orders.input.path"


# Streaming table: AWS Glue SDP's documented Python API for incremental tables is
# dp.create_streaming_table() + @dp.append_flow(target=...), not a @dp.table decorator.
# https://docs.aws.amazon.com/glue/latest/dg/spark-declarative-pipelines.html
dp.create_streaming_table(
    "bronze_orders",
    comment="Raw order records, ingested incrementally as new files land under the input path. All columns as string, no coercion or filtering.",
)


@dp.append_flow(target="bronze_orders")
def ingest_bronze_orders() -> DataFrame:
    # Glue's SDP wrapper doesn't inject a `spark` global into pipeline files.
    spark = SparkSession.active()
    return (
        spark.readStream.format("csv")
        .schema(ORDERS_SCHEMA)
        .option("header", "true")
        .option("pathGlobFilter", "*.csv")
        .load(spark.conf.get(INPUT_PATH_CONF))
        # _metadata is core Spark (FileFormat), not Databricks-only; file_name/
        # file_modification_time are deterministic per source file, unlike
        # current_timestamp() which would vary per run/retry.
        .withColumn("source_file_name", F.col("_metadata.file_name"))
        .withColumn("source_file_load_date", F.col("_metadata.file_modification_time"))
    )
