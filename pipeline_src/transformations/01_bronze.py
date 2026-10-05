from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
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


@dp.materialized_view
def bronze_orders() -> DataFrame:
    # Glue's SDP wrapper doesn't inject a `spark` global into pipeline files.
    spark = SparkSession.active()
    return (
        spark.read.format("csv")
        .schema(ORDERS_SCHEMA)
        .option("header", "true")
        .load(spark.conf.get(INPUT_PATH_CONF))
    )
