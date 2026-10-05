from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Explicit format string keeps timestamp parsing deterministic regardless of
# session time zone.
ORDER_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"


@dp.materialized_view
def silver_orders() -> DataFrame:
    # Glue's SDP wrapper doesn't inject a `spark` global into pipeline files.
    spark = SparkSession.active()
    typed = spark.table("bronze_orders").select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("region"),
        F.col("status"),
        F.col("amount").cast("decimal(18,2)").alias("amount"),
        F.to_timestamp(F.col("order_ts"), ORDER_TS_FORMAT).alias("order_ts"),
    )

    filtered = typed.filter((F.col("status") == "COMPLETE") & (F.col("amount") > 0))

    # Second projection (not .withColumn): amount_band is derived from the
    # already-cast `amount` column. Spark 4.1 has ANSI mode on by default, so
    # comparing the raw string `amount` against a number would fail.
    return filtered.select(
        "order_id",
        "customer_id",
        "region",
        "status",
        "amount",
        "order_ts",
        F.when(F.col("amount") >= 500, "large")
        .when(F.col("amount") >= 100, "medium")
        .otherwise("small")
        .alias("amount_band"),
    )
