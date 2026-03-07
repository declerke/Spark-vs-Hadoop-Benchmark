import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType
)

WINDOW_SIZE_STEPS = 24

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

BANKSIM_TX_SCHEMA = StructType([
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

BANKSIM_NET_SCHEMA = StructType([
    StructField("Source", StringType(), True),
    StructField("Target", StringType(), True),
    StructField("Weight", DoubleType(), True),
    StructField("typeTrans", StringType(), True),
    StructField("fraud", IntegerType(), True),
])


def build_paysim_dimension(paysim_df):
    dest_dim = (
        paysim_df
        .filter(F.col("nameDest").isNotNull())
        .groupBy("nameDest")
        .agg(
            F.count("*").alias("total_received_tx"),
            F.sum("amount").alias("total_received_amount"),
            F.avg("amount").alias("avg_received_amount"),
            F.sum(F.when(F.col("isFraud") == 1, 1).otherwise(0)).alias("fraud_received_count")
        )
        .withColumnRenamed("nameDest", "dim_account")
        .withColumn("dim_source", F.lit("PAYSIM_DEST"))
    )
    return dest_dim


def apply_rolling_window_paysim(df):
    w = (
        Window.partitionBy("nameOrig")
        .orderBy("step")
        .rangeBetween(-WINDOW_SIZE_STEPS, 0)
    )

    df = (
        df
        .withColumn("rolling_24h_sum", F.sum("amount").over(w))
        .withColumn("rolling_24h_count", F.count("*").over(w))
        .withColumn("rolling_24h_max", F.max("amount").over(w))
    )
    return df


def apply_rolling_window_banksim(df):
    w = (
        Window.partitionBy("customer")
        .orderBy(F.col("step").cast("long"))
        .rangeBetween(-WINDOW_SIZE_STEPS, 0)
    )

    df = (
        df
        .withColumn("rolling_24h_sum", F.sum("amount").over(w))
        .withColumn("rolling_24h_count", F.count("*").over(w))
        .withColumn("rolling_24h_max", F.max("amount").over(w))
    )
    return df


def run_workload3(spark, data_dir, output_dir):
    paysim_path = os.path.join(data_dir, "paysim_transactions.csv")
    banksim_tx_path = os.path.join(data_dir, "banksim_transactions.csv")
    banksim_net_path = os.path.join(data_dir, "banksim_network.csv")

    all_results = []

    if os.path.exists(paysim_path):
        paysim_df = spark.read.csv(
            paysim_path,
            schema=PAYSIM_SCHEMA,
            header=True,
            mode="DROPMALFORMED"
        ).filter(F.col("step").isNotNull() & F.col("amount").isNotNull())

        paysim_dim = build_paysim_dimension(paysim_df)

        paysim_joined = paysim_df.join(
            paysim_dim,
            paysim_df["nameDest"] == paysim_dim["dim_account"],
            how="left"
        ).drop("dim_account")

        paysim_windowed = apply_rolling_window_paysim(paysim_joined)

        paysim_out = (
            paysim_windowed
            .select(
                F.lit("PAYSIM").alias("dataset"),
                F.col("nameOrig").alias("account"),
                "step", "type", "amount",
                "rolling_24h_sum", "rolling_24h_count", "rolling_24h_max",
                "total_received_tx", "total_received_amount",
                "fraud_received_count",
                "isFraud"
            )
        )
        all_results.append(paysim_out)

    if os.path.exists(banksim_tx_path) and os.path.exists(banksim_net_path):
        banksim_tx = spark.read.csv(
            banksim_tx_path,
            schema=BANKSIM_TX_SCHEMA,
            header=True,
            mode="DROPMALFORMED"
        ).filter(F.col("step").isNotNull() & F.col("amount").isNotNull())

        banksim_net = spark.read.csv(
            banksim_net_path,
            schema=BANKSIM_NET_SCHEMA,
            header=True,
            mode="DROPMALFORMED"
        )

        merchant_dim = (
            banksim_net
            .groupBy("Source")
            .agg(
                F.count("*").alias("total_received_tx"),
                F.sum("Weight").alias("total_received_amount"),
                F.avg("Weight").alias("avg_received_amount"),
                F.sum(F.when(F.col("fraud") == 1, 1).otherwise(0)).alias("fraud_received_count")
            )
            .withColumnRenamed("Source", "dim_account")
            .withColumn("dim_source", F.lit("BANKSIM_NET"))
        )

        banksim_joined = banksim_tx.join(
            merchant_dim,
            banksim_tx["merchant"] == merchant_dim["dim_account"],
            how="left"
        ).drop("dim_account")

        banksim_windowed = apply_rolling_window_banksim(banksim_joined)

        banksim_out = (
            banksim_windowed
            .select(
                F.lit("BANKSIM").alias("dataset"),
                F.col("customer").alias("account"),
                F.col("step").cast(LongType()).alias("step"),
                F.col("category").alias("type"),
                "amount",
                "rolling_24h_sum", "rolling_24h_count", "rolling_24h_max",
                "total_received_tx", "total_received_amount",
                "fraud_received_count",
                F.col("fraud").alias("isFraud")
            )
        )
        all_results.append(banksim_out)

    if not all_results:
        raise RuntimeError("No input data found in: " + data_dir)

    final_df = all_results[0]
    for df in all_results[1:]:
        final_df = final_df.union(df)

    out_path = os.path.join(output_dir, "workload3_join_window")
    final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_path)

    row_count = final_df.count()
    return row_count


if __name__ == "__main__":
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "/benchmark/data"
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "/benchmark/logs"

    spark = (
        SparkSession.builder
        .appName("BenchmarkWorkload3_JoinWindow")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    row_count = run_workload3(spark, data_dir, output_dir)
    print(f"WORKLOAD3_ROWS={row_count}")
    spark.stop()