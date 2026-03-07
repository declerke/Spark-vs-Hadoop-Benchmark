import sys
import os
import math
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, LongType
)

VELOCITY_THRESHOLD = 5
AMOUNT_Z_THRESHOLD = 2.5

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


def apply_fraud_proxy(df, account_col, step_col, amount_col, fraud_col, dataset_name):
    df = df.withColumn("hour_bucket", (F.col(step_col) % 24).cast("long"))
    df = df.withColumn("log_amount", F.log1p(F.col(amount_col)))

    velocity_window = Window.partitionBy(account_col, "hour_bucket")
    df = df.withColumn("hourly_tx_count", F.count("*").over(velocity_window))
    df = df.withColumn("velocity_flag", F.col("hourly_tx_count") >= VELOCITY_THRESHOLD)

    global_stats = df.agg(
        F.mean("log_amount").alias("global_mean"),
        F.stddev("log_amount").alias("global_std")
    ).collect()[0]

    global_mean = global_stats["global_mean"] or 0.0
    global_std = global_stats["global_std"] or 1.0

    df = df.withColumn("amount_z_score",
        (F.col("log_amount") - global_mean) / global_std
    )
    df = df.withColumn("amount_flag", F.col("amount_z_score") > AMOUNT_Z_THRESHOLD)
    df = df.withColumn("fraud_proxy",
        F.when(F.col("velocity_flag") | F.col("amount_flag"), 1).otherwise(0)
    )

    precision = df.filter(F.col("fraud_proxy") == 1)
    total_flagged = precision.count()
    true_positives = precision.filter(F.col(fraud_col) == 1).count()
    precision_val = true_positives / total_flagged if total_flagged > 0 else 0.0

    recall_df = df.filter(F.col(fraud_col) == 1)
    total_actual_fraud = recall_df.count()
    recall_val = true_positives / total_actual_fraud if total_actual_fraud > 0 else 0.0

    return df, precision_val, recall_val


def run_workload2(spark, data_dir, output_dir):
    paysim_path = os.path.join(data_dir, "paysim_transactions.csv")
    banksim_path = os.path.join(data_dir, "banksim_transactions.csv")

    all_results = []
    metrics_summary = []

    if os.path.exists(paysim_path):
        paysim_df = spark.read.csv(
            paysim_path,
            schema=PAYSIM_SCHEMA,
            header=True,
            mode="DROPMALFORMED"
        ).filter(F.col("step").isNotNull() & F.col("amount").isNotNull())

        paysim_flagged, prec, rec = apply_fraud_proxy(
            paysim_df, "nameOrig", "step", "amount", "isFraud", "PAYSIM"
        )

        paysim_out = (
            paysim_flagged
            .select(
                F.lit("PAYSIM").alias("dataset"),
                F.col("nameOrig").alias("account"),
                "step", "type", "amount",
                "hourly_tx_count", "amount_z_score",
                "velocity_flag", "amount_flag",
                "fraud_proxy", "isFraud"
            )
        )
        all_results.append(paysim_out)
        metrics_summary.append({
            "dataset": "PAYSIM",
            "precision": prec,
            "recall": rec
        })

    if os.path.exists(banksim_path):
        banksim_df = spark.read.csv(
            banksim_path,
            schema=BANKSIM_SCHEMA,
            header=True,
            mode="DROPMALFORMED"
        ).filter(F.col("step").isNotNull() & F.col("amount").isNotNull())

        banksim_flagged, prec, rec = apply_fraud_proxy(
            banksim_df, "customer", "step", "amount", "fraud", "BANKSIM"
        )

        banksim_out = (
            banksim_flagged
            .select(
                F.lit("BANKSIM").alias("dataset"),
                F.col("customer").alias("account"),
                F.col("step").cast(LongType()).alias("step"),
                F.col("category").alias("type"),
                "amount",
                "hourly_tx_count", "amount_z_score",
                "velocity_flag", "amount_flag",
                "fraud_proxy",
                F.col("fraud").alias("isFraud")
            )
        )
        all_results.append(banksim_out)
        metrics_summary.append({
            "dataset": "BANKSIM",
            "precision": prec,
            "recall": rec
        })

    if not all_results:
        raise RuntimeError("No input data found in: " + data_dir)

    final_df = all_results[0]
    for df in all_results[1:]:
        final_df = final_df.union(df)

    out_path = os.path.join(output_dir, "workload2_fraud")
    final_df.coalesce(1).write.mode("overwrite").option("header", True).csv(out_path)

    for m in metrics_summary:
        print(f"WORKLOAD2_METRICS|{m['dataset']}|precision={m['precision']:.4f}|recall={m['recall']:.4f}")

    row_count = final_df.count()
    return row_count


if __name__ == "__main__":
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "/benchmark/data"
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "/benchmark/logs"

    spark = (
        SparkSession.builder
        .appName("BenchmarkWorkload2_FraudProxy")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    row_count = run_workload2(spark, data_dir, output_dir)
    print(f"WORKLOAD2_ROWS={row_count}")
    spark.stop()