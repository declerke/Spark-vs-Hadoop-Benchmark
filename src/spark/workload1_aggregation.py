import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType
)

PAYSIM_SCHEMA = StructType([
    StructField("step", LongType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
])

BANKSIM_SCHEMA = StructType([
    StructField("step", DoubleType(), True),
    StructField("customer", StringType(), True),
    StructField("age", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("zipcodeOri", StringType(), True),
    StructField("merchant", StringType(), True),
    StructField("zipMerchant", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("fraud", IntegerType(), True),
])


def run_workload1(spark, data_dir, output_dir):
    paysim_path = os.path.join(data_dir, "paysim_transactions.csv")
    banksim_path = os.path.join(data_dir, "banksim_transactions.csv")

    results = []

    if os.path.exists(paysim_path):
        paysim_df = spark.read.csv(
            paysim_path,
            schema=PAYSIM_SCHEMA,
            header=True,
            mode="DROPMALFORMED"
        )

        paysim_agg = (
            paysim_df
            .filter(F.col("step").isNotNull() & F.col("nameOrig").isNotNull())
            .withColumn("day", (F.col("step") / 24).cast("long"))
            .groupBy("nameOrig", "day", "type")
            .agg(
                F.sum("amount").alias("total_amount"),
                F.count("*").alias("tx_count"),
                F.avg("amount").alias("avg_amount"),
                F.max("amount").alias("max_amount"),
                F.sum(F.when(F.col("isFraud") == 1, 1).otherwise(0)).alias("fraud_count")
            )
            .withColumn("dataset", F.lit("PAYSIM"))
            .withColumnRenamed("nameOrig", "account")
        )
        results.append(paysim_agg)

    if os.path.exists(banksim_path):
        banksim_df = spark.read.csv(
            banksim_path,
            schema=BANKSIM_SCHEMA,
            header=True,
            mode="DROPMALFORMED"
        )

        banksim_agg = (
            banksim_df
            .filter(F.col("step").isNotNull() & F.col("customer").isNotNull())
            .withColumn("day", F.col("step").cast("long"))
            .groupBy("customer", "day", "category")
            .agg(
                F.sum("amount").alias("total_amount"),
                F.count("*").alias("tx_count"),
                F.avg("amount").alias("avg_amount"),
                F.max("amount").alias("max_amount"),
                F.sum(F.when(F.col("fraud") == 1, 1).otherwise(0)).alias("fraud_count")
            )
            .withColumn("dataset", F.lit("BANKSIM"))
            .withColumnRenamed("customer", "account")
            .withColumnRenamed("category", "type")
        )
        results.append(banksim_agg)

    if not results:
        raise RuntimeError("No input data found in data_dir: " + data_dir)

    combined_schema_cols = ["dataset", "account", "day", "type",
                            "total_amount", "tx_count", "avg_amount",
                            "max_amount", "fraud_count"]

    aligned = []
    for df in results:
        aligned.append(df.select(combined_schema_cols))

    final_df = aligned[0]
    for df in aligned[1:]:
        final_df = final_df.union(df)

    out_path = os.path.join(output_dir, "workload1_aggregation")
    final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_path)

    row_count = final_df.count()
    return row_count


if __name__ == "__main__":
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "/benchmark/data"
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "/benchmark/logs"

    spark = (
        SparkSession.builder
        .appName("BenchmarkWorkload1_Aggregation")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    row_count = run_workload1(spark, data_dir, output_dir)
    print(f"WORKLOAD1_ROWS={row_count}")
    spark.stop()