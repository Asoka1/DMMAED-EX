#!/usr/bin/env python3
"""
spark_batch_processing.py
Apache Spark Distributed Batch Processing Pipeline
AUCA Big Data Analytics Final Project
Author: Asoka Niyonsaba Jean Claude (ID: 101410)

Implements:
1. Data cleaning, normalization, null-handling, and schema casting.
2. Product Recommendation Engine:
   - Market Basket Co-Purchase Affinity ("Users who bought X also bought Y")
   - Support, Confidence, and Lift metrics.
   - Clickstream Co-Viewing Affinity ("Products frequently viewed together").
3. User Cohort Analysis:
   - Groups users by registration month.
   - Tracks retention and spending progression across subsequent months.
"""

import os
import sys
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# Configure environment for PySpark
os.environ["SPARK_LOCAL_DIRS"] = "/tmp"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ.pop("HADOOP_CONF_DIR", None)
os.environ.pop("HADOOP_HOME", None)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, ArrayType, TimestampType
)


def create_spark_session(app_name="AUCA_ECommerce_Spark_Batch"):
    return SparkSession.builder \
        .master("local[4]") \
        .appName(app_name) \
        .config("spark.local.dir", "/tmp") \
        .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/tmp") \
        .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=/tmp") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def run_batch_processing():
    print("\n" + "=" * 70)
    print("STAGE 3: APACHE SPARK DISTRIBUTED BATCH PROCESSING")
    print("=" * 70)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    start_total = time.time()

    # -------------------------------------------------------------
    # 1. Ingestion & Data Cleaning / Normalization
    # -------------------------------------------------------------
    print("\n[*] 1. Loading and Cleaning Datasets with PySpark DataFrames...")

    # A. Users
    users_raw = spark.read.option("multiLine", True).json(os.path.join(DATA_DIR, "users.json"))
    users_df = users_raw.select(
        F.col("user_id"),
        F.col("geo_data.city").alias("city"),
        F.col("geo_data.state").alias("state"),
        F.col("geo_data.country").alias("country"),
        F.to_timestamp(F.col("registration_date")).alias("registration_ts"),
        F.to_timestamp(F.col("last_active")).alias("last_active_ts")
    ).filter(F.col("user_id").isNotNull()).dropDuplicates(["user_id"])

    # B. Products
    products_raw = spark.read.option("multiLine", True).json(os.path.join(DATA_DIR, "products.json"))
    products_df = products_raw.select(
        F.col("product_id"),
        F.col("name").alias("product_name"),
        F.col("category_id"),
        F.col("subcategory_id"),
        F.col("base_price").cast("double"),
        F.col("current_stock").cast("int"),
        F.col("is_active").cast("boolean"),
        F.to_timestamp(F.col("creation_date")).alias("creation_ts")
    ).filter(F.col("product_id").isNotNull() & (F.col("base_price") > 0))

    # C. Transactions
    txns_raw = spark.read.option("multiLine", True).json(os.path.join(DATA_DIR, "transactions.json"))
    txns_df = txns_raw.select(
        F.col("transaction_id"),
        F.col("session_id"),
        F.col("user_id"),
        F.to_timestamp(F.col("timestamp")).alias("transaction_ts"),
        F.col("subtotal").cast("double"),
        F.col("discount").cast("double"),
        F.col("total").cast("double"),
        F.col("payment_method"),
        F.col("status"),
        F.col("items")
    ).filter(F.col("total") >= 0)

    # Flatten transaction items for line-level processing
    txn_items_df = txns_df.select(
        F.col("transaction_id"),
        F.col("user_id"),
        F.col("transaction_ts"),
        F.explode(F.col("items")).alias("item")
    ).select(
        F.col("transaction_id"),
        F.col("user_id"),
        F.col("transaction_ts"),
        F.col("item.product_id").alias("product_id"),
        F.col("item.quantity").cast("int").alias("quantity"),
        F.col("item.unit_price").cast("double").alias("unit_price"),
        F.col("item.subtotal").cast("double").alias("subtotal")
    )

    # D. Sessions
    sessions_raw = spark.read.option("multiLine", True).json(os.path.join(DATA_DIR, "sessions_*.json"))
    sessions_df = sessions_raw.select(
        F.col("session_id"),
        F.col("user_id"),
        F.to_timestamp(F.col("start_time")).alias("session_start_ts"),
        F.col("duration_seconds").cast("int"),
        F.col("conversion_status"),
        F.col("referrer"),
        F.col("device_profile.type").alias("device_type"),
        F.col("device_profile.browser").alias("browser"),
        F.col("viewed_products")
    )

    print(f"[+] Cleaned Users Count:        {users_df.count():,}")
    print(f"[+] Cleaned Products Count:     {products_df.count():,}")
    print(f"[+] Cleaned Transactions Count: {txns_df.count():,}")
    print(f"[+] Flattened Line Items Count: {txn_items_df.count():,}")
    print(f"[+] Cleaned Sessions Count:     {sessions_df.count():,}")

    # -------------------------------------------------------------
    # 2. Product Recommendation Engine (Co-Purchase Affinity)
    # -------------------------------------------------------------
    print("\n" + "-" * 70)
    print("TASK A: PRODUCT RECOMMENDATION INDICATORS ('Bought X Also Bought Y')")
    print("-" * 70)

    total_orders = txns_df.count()

    # Step 1: Distinct product occurrences per transaction
    basket_df = txn_items_df.select("transaction_id", "product_id").distinct()

    # Step 2: Product individual purchase counts for Support calculation
    prod_counts = basket_df.groupBy("product_id").agg(F.count("transaction_id").alias("item_count"))
    ca = prod_counts.select(F.col("product_id").alias("pid_a"), F.col("item_count").alias("count_a"))
    cb = prod_counts.select(F.col("product_id").alias("pid_b"), F.col("item_count").alias("count_b"))

    # Step 3: Self-join to form product pairs (A < B to prevent duplicates)
    t1 = basket_df.alias("t1")
    t2 = basket_df.alias("t2")
    pair_df = t1.join(t2, (F.col("t1.transaction_id") == F.col("t2.transaction_id")) & 
                          (F.col("t1.product_id") < F.col("t2.product_id"))) \
        .groupBy(F.col("t1.product_id").alias("prod_a"), F.col("t2.product_id").alias("prod_b")) \
        .agg(F.count("t1.transaction_id").alias("pair_count")) \
        .filter(F.col("pair_count") >= 2)

    # Step 4: Join product counts to compute Confidence and Lift
    recs_df = pair_df \
        .join(ca, F.col("prod_a") == F.col("pid_a")) \
        .join(cb, F.col("prod_b") == F.col("pid_b")) \
        .select(
            F.col("prod_a"),
            F.col("prod_b"),
            F.col("pair_count"),
            F.round(F.col("pair_count") / total_orders, 4).alias("support"),
            F.round(F.col("pair_count") / F.col("count_a"), 4).alias("confidence_a_to_b"),
            F.round((F.col("pair_count") * total_orders) / (F.col("count_a") * F.col("count_b")), 2).alias("lift")
        ) \
        .join(products_df.alias("pa"), F.col("prod_a") == F.col("pa.product_id")) \
        .select(
            F.col("prod_a"),
            F.col("pa.product_name").alias("prod_a_name"),
            F.col("prod_b"),
            F.col("pair_count"),
            F.col("support"),
            F.col("confidence_a_to_b"),
            F.col("lift")
        ) \
        .join(products_df.alias("pb"), F.col("prod_b") == F.col("pb.product_id")) \
        .select(
            F.col("prod_a"),
            F.col("prod_a_name"),
            F.col("prod_b"),
            F.col("pb.product_name").alias("prod_b_name"),
            F.col("pair_count"),
            F.col("confidence_a_to_b"),
            F.col("lift")
        ) \
        .orderBy(F.desc("lift"), F.desc("pair_count")) \
        .limit(10)

    print("\n[✓] Top 10 Product Affinity Rules (Market Basket Analysis):")
    recs_df.show(truncate=35)

    # -------------------------------------------------------------
    # 3. User Cohort Analysis (Registration Month vs Spending Months)
    # -------------------------------------------------------------
    print("\n" + "-" * 70)
    print("TASK B: USER COHORT RETENTION & SPENDING ANALYSIS")
    print("-" * 70)

    # Attach registration month to users
    users_cohort = users_df.withColumn("cohort_month", F.date_format("registration_ts", "yyyy-MM"))

    # Join transactions with user cohort
    txns_with_cohort = txns_df.join(users_cohort, "user_id") \
        .withColumn("txn_month", F.date_format("transaction_ts", "yyyy-MM")) \
        .withColumn("months_since_reg", 
            (F.year("transaction_ts") - F.year("registration_ts")) * 12 + 
            (F.month("transaction_ts") - F.month("registration_ts"))
        )

    # Group by Cohort and Activity Month Offset
    cohort_summary = txns_with_cohort.groupBy("cohort_month", "months_since_reg") \
        .agg(
            F.countDistinct("user_id").alias("active_users"),
            F.round(F.sum("total"), 2).alias("cohort_spending"),
            F.round(F.avg("total"), 2).alias("avg_order_value")
        ) \
        .filter(F.col("months_since_reg") >= 0) \
        .orderBy("cohort_month", "months_since_reg")

    print("\n[✓] User Cohort Purchasing Pattern Sample (First 15 Rows):")
    cohort_summary.show(15, truncate=False)

    total_time = time.time() - start_total
    print(f"\n[✓] PySpark Batch Processing Pipeline Executed Successfully in {total_time:.2f} seconds.")

    spark.stop()


if __name__ == "__main__":
    run_batch_processing()
