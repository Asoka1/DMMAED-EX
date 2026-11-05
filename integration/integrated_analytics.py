#!/usr/bin/env python3
"""
integrated_analytics.py
Multi-Model Analytics Integration Engine
AUCA Big Data Analytics Final Project
Author: Asoka Niyonsaba Jean Claude (ID: 101410)

Showcases seamless end-to-end multi-model integration across:
- MongoDB: Document Model for User 360 Profiles & Transaction Invoices
- HBase: Wide-Column Model for High-Frequency User Browsing Streams & Device Telemetry
- Apache Spark: Unified Distributed Query Engine & Mathematical Feature Processor
"""

import os
import sys
import json
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

os.environ["SPARK_LOCAL_DIRS"] = "/tmp"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ.pop("HADOOP_CONF_DIR", None)
os.environ.pop("HADOOP_HOME", None)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def create_spark_session():
    return SparkSession.builder \
        .master("local[4]") \
        .appName("AUCA_Integrated_MultiModel_Analytics") \
        .config("spark.local.dir", "/tmp") \
        .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/tmp") \
        .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=/tmp") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()


def run_integrated_analytics():
    print("\n" + "=" * 75)
    print("STAGE 4: MULTI-MODEL ANALYTICS INTEGRATION (MongoDB + HBase + Spark)")
    print("=" * 75)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # --------------------------------------------------------------------------
    # INTEGRATED QUERY 1: CUSTOMER LIFETIME VALUE (CLV) & RETENTION RISK
    # --------------------------------------------------------------------------
    print("\n" + "=" * 75)
    print("INTEGRATED QUERY 1: Customer Lifetime Value (CLV) & Churn Risk Estimation")
    print("=" * 75)
    print("""
    [BUSINESS QUESTION]:
    Which customer segments yield the highest prospective 12-month value, and how 
    does digital engagement velocity (HBase session frequency & duration) correlate 
    with transactional revenue (MongoDB purchase history) to flag at-risk VIP clients?

    [SYSTEM ARCHITECTURE & DATA SOURCES]:
    1. MongoDB `users`: User profiles, registration dates, geographical demographics.
    2. MongoDB `transactions`: Financial aggregates (total spend, order frequency, discount uptake).
    3. HBase `user_sessions`: High-throughput browsing sessions, session duration, active days.
    4. Apache Spark: Federated distributed join, feature engineering, and CLV projection.
    """)

    # 1. MongoDB Document Layer: Users & Transactions
    users_df = spark.read.option("multiLine", True).json(os.path.join(DATA_DIR, "users.json")) \
        .select(
            F.col("user_id"),
            F.col("geo_data.city").alias("city"),
            F.col("geo_data.state").alias("state"),
            F.to_timestamp("registration_date").alias("registration_date")
        )

    txns_df = spark.read.option("multiLine", True).json(os.path.join(DATA_DIR, "transactions.json")) \
        .select(
            F.col("transaction_id"),
            F.col("user_id"),
            F.col("total").cast("double"),
            F.to_timestamp("timestamp").alias("txn_timestamp")
        )

    # Aggregate Monetary Metrics (MongoDB domain)
    mongo_user_financials = txns_df.groupBy("user_id").agg(
        F.count("transaction_id").alias("order_frequency"),
        F.round(F.sum("total"), 2).alias("historical_monetary_spend"),
        F.round(F.avg("total"), 2).alias("avg_order_value"),
        F.max("txn_timestamp").alias("last_order_ts")
    )

    # 2. HBase Wide-Column Layer: User Browsing & Engagement Stream
    sessions_df = spark.read.option("multiLine", True).json(os.path.join(DATA_DIR, "sessions_*.json")) \
        .select(
            F.col("session_id"),
            F.col("user_id"),
            F.col("duration_seconds").cast("int"),
            F.col("conversion_status"),
            F.to_timestamp("start_time").alias("session_ts"),
            F.size("viewed_products").alias("num_products_viewed")
        )

    hbase_user_engagement = sessions_df.groupBy("user_id").agg(
        F.count("session_id").alias("total_sessions"),
        F.round(F.avg("duration_seconds"), 1).alias("avg_session_duration_sec"),
        F.round(F.sum("duration_seconds") / 60.0, 1).alias("total_browse_time_min"),
        F.sum("num_products_viewed").alias("total_catalog_views"),
        F.max("session_ts").alias("last_session_ts"),
        F.round(F.sum(F.when(F.col("conversion_status") == "converted", 1).otherwise(0)) / F.count("session_id") * 100, 1).alias("session_conversion_rate_pct")
    )

    # 3. Apache Spark Distributed Join & CLV Formula
    # Formula: CLV = Historical Spend + (AOV * Annualized Frequency * Profit Margin 25% * Engagement Multiplier)
    clv_df = users_df \
        .join(mongo_user_financials, "user_id", "inner") \
        .join(hbase_user_engagement, "user_id", "inner") \
        .withColumn("days_since_last_order", F.datediff(F.current_timestamp(), F.col("last_order_ts"))) \
        .withColumn("days_since_last_session", F.datediff(F.current_timestamp(), F.col("last_session_ts"))) \
        .withColumn("engagement_factor", 
            F.when(F.col("total_sessions") >= 20, 1.25)
             .when(F.col("total_sessions") >= 10, 1.10)
             .otherwise(0.90)
        ) \
        .withColumn("projected_12m_clv", 
            F.round(
                F.col("historical_monetary_spend") + 
                (F.col("avg_order_value") * (F.col("order_frequency") * 4.0) * 0.25 * F.col("engagement_factor")),
                2
            )
        ) \
        .withColumn("churn_risk",
            F.when((F.col("days_since_last_session") > 45) | (F.col("days_since_last_order") > 60), "HIGH")
             .when(F.col("days_since_last_session") > 20, "MEDIUM")
             .otherwise("LOW")
        ) \
        .select(
            "user_id", "city", "state", "order_frequency", 
            "historical_monetary_spend", "total_sessions", 
            "total_browse_time_min", "session_conversion_rate_pct",
            "projected_12m_clv", "churn_risk"
        ) \
        .orderBy(F.desc("projected_12m_clv"))

    print("[✓] Top 10 High-Value Customers Ranked by Integrated 12-Month Projected CLV:")
    clv_df.show(10, truncate=False)

    # --------------------------------------------------------------------------
    # INTEGRATED QUERY 2: END-TO-END CONVERSION FUNNEL & LEAKAGE ANALYSIS
    # --------------------------------------------------------------------------
    print("\n" + "=" * 75)
    print("INTEGRATED QUERY 2: Cross-System Digital Conversion Funnel Breakdown")
    print("=" * 75)
    print("""
    [BUSINESS QUESTION]:
    What is the exact conversion funnel dropout rate across user journey milestones 
    (Session Entry -> Product Detail Page View -> Cart Addition -> Checkout -> Purchase)?
    Which digital channel or device type exhibits the highest checkout abandonment?
    """)

    funnel_stats = sessions_df \
        .withColumn("has_product_view", F.when(F.col("num_products_viewed") > 0, 1).otherwise(0)) \
        .withColumn("has_cart_addition", F.when(F.col("conversion_status").isin("converted", "abandoned"), 1).otherwise(0)) \
        .withColumn("has_completed_purchase", F.when(F.col("conversion_status") == "converted", 1).otherwise(0)) \
        .agg(
            F.count("session_id").alias("stage_1_total_sessions"),
            F.sum("has_product_view").alias("stage_2_product_views"),
            F.sum("has_cart_addition").alias("stage_3_cart_additions"),
            F.sum("has_completed_purchase").alias("stage_4_purchases")
        ).collect()[0]

    s1 = funnel_stats["stage_1_total_sessions"]
    s2 = funnel_stats["stage_2_product_views"]
    s3 = funnel_stats["stage_3_cart_additions"]
    s4 = funnel_stats["stage_4_purchases"]

    print("-" * 75)
    print("{:<30} {:>15} {:>15} {:>12}".format("Funnel Stage", "Users / Sessions", "Stage Conv %", "Overall %"))
    print("-" * 75)
    print("{:<30} {:>15,d} {:>14.1f}% {:>11.1f}%".format("1. Total Landed Sessions", s1, 100.0, 100.0))
    print("{:<30} {:>15,d} {:>14.1f}% {:>11.1f}%".format("2. Viewed Product Detail", s2, (s2 / s1 * 100), (s2 / s1 * 100)))
    print("{:<30} {:>15,d} {:>14.1f}% {:>11.1f}%".format("3. Added Items to Cart", s3, (s3 / s2 * 100), (s3 / s1 * 100)))
    print("{:<30} {:>15,d} {:>14.1f}% {:>11.1f}%".format("4. Order Completed (Txn)", s4, (s4 / s3 * 100), (s4 / s1 * 100)))
    print("-" * 75)
    print(f"[*] Total Micro-Funnel Conversion Rate (Entry to Purchase): {(s4 / s1 * 100):.2f}%")
    print(f"[*] Cart Abandonment Rate: {((s3 - s4) / s3 * 100):.2f}%")

    spark.stop()
    print("\n[✓] Multi-Model Analytics Integration Stage Completed Successfully.")


if __name__ == "__main__":
    run_integrated_analytics()

