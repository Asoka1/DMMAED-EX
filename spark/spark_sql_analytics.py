#!/usr/bin/env python3
"""
spark_sql_analytics.py
Apache Spark SQL Advanced Multi-Model Analytics
AUCA Big Data Analytics Final Project
Author: Asoka Niyonsaba Jean Claude (ID: 101410)

Executes complex Spark SQL analytical queries across unified DataFrames
representing data stored across MongoDB (Profiles, Catalog, Transactions)
and HBase (Time-Series Browsing Clickstreams).
"""

import os
import sys
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


def create_spark_session():
    return SparkSession.builder \
        .master("local[4]") \
        .appName("AUCA_ECommerce_Spark_SQL") \
        .config("spark.local.dir", "/tmp") \
        .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/tmp") \
        .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=/tmp") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def run_spark_sql():
    print("\n" + "=" * 70)
    print("SPARK SQL MULTI-MODEL FEDERATED ANALYTICS")
    print("=" * 70)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Register Temporary Views
    print("[*] Registering distributed temporary views from JSON data stores...")
    users_df = spark.read.option("multiLine", True).json(os.path.join(DATA_DIR, "users.json"))
    users_df.createOrReplaceTempView("v_users")

    prods_df = spark.read.option("multiLine", True).json(os.path.join(DATA_DIR, "products.json"))
    prods_df.createOrReplaceTempView("v_products")

    cats_df = spark.read.option("multiLine", True).json(os.path.join(DATA_DIR, "categories.json"))
    cats_df.createOrReplaceTempView("v_categories")

    txns_df = spark.read.option("multiLine", True).json(os.path.join(DATA_DIR, "transactions.json"))
    txns_df.createOrReplaceTempView("v_transactions")

    sessions_df = spark.read.option("multiLine", True).json(os.path.join(DATA_DIR, "sessions_*.json"))
    sessions_df.createOrReplaceTempView("v_sessions")

    # -------------------------------------------------------------
    # Query 1: Cross-Model Device & Geo Financial Analysis
    # -------------------------------------------------------------
    print("\n" + "-" * 70)
    print("QUERY 1: Device Ecosystem & Cross-Regional Financial Performance")
    print("Notional Systems: MongoDB (Users + Transactions) JOIN HBase (Device Logs)")
    print("-" * 70)

    q1 = """
    SELECT 
        s.device_profile.type AS device_type,
        s.device_profile.os AS operating_system,
        COUNT(DISTINCT t.transaction_id) AS total_orders,
        ROUND(SUM(t.total), 2) AS total_revenue,
        ROUND(AVG(t.total), 2) AS avg_order_value,
        ROUND(SUM(t.discount), 2) AS total_discounts_given
    FROM v_transactions t
    INNER JOIN v_sessions s ON t.session_id = s.session_id
    WHERE t.status = 'completed'
    GROUP BY s.device_profile.type, s.device_profile.os
    ORDER BY total_revenue DESC
    """
    df_q1 = spark.sql(q1)
    df_q1.show(truncate=False)

    # -------------------------------------------------------------
    # Query 2: Category View-to-Purchase Conversion Velocity
    # -------------------------------------------------------------
    print("\n" + "-" * 70)
    print("QUERY 2: Category Clickstream View-to-Purchase Funnel Conversion")
    print("Notional Systems: HBase (Session Page Views) JOIN MongoDB (Order Line Items)")
    print("-" * 70)

    q2 = """
    WITH category_views AS (
        SELECT 
            pv.category_id,
            COUNT(1) AS total_page_views
        FROM (SELECT explode(page_views) AS pv FROM v_sessions)
        WHERE pv.category_id IS NOT NULL
        GROUP BY pv.category_id
    ),
    category_purchases AS (
        SELECT 
            p.category_id,
            COUNT(DISTINCT t.transaction_id) AS order_count,
            SUM(t.item.quantity) AS total_units_sold,
            ROUND(SUM(t.item.subtotal), 2) AS category_revenue
        FROM (
            SELECT transaction_id, status, explode(items) AS item FROM v_transactions
        ) t
        INNER JOIN v_products p ON t.item.product_id = p.product_id
        WHERE t.status = 'completed'
        GROUP BY p.category_id
    )
    SELECT 
        c.category_id,
        c.name AS category_name,
        COALESCE(v.total_page_views, 0) AS views,
        COALESCE(p.order_count, 0) AS orders,
        COALESCE(p.total_units_sold, 0) AS units_sold,
        COALESCE(p.category_revenue, 0.0) AS revenue,
        ROUND((COALESCE(p.order_count, 0) / NULLIF(v.total_page_views, 0)) * 100, 2) AS conversion_rate_pct
    FROM v_categories c
    LEFT JOIN category_views v ON c.category_id = v.category_id
    LEFT JOIN category_purchases p ON c.category_id = p.category_id
    ORDER BY revenue DESC
    LIMIT 10
    """
    df_q2 = spark.sql(q2)
    df_q2.show(truncate=False)

    # -------------------------------------------------------------
    # Query 3: At-Risk High-Value Customers (Customer Retention)
    # -------------------------------------------------------------
    print("\n" + "-" * 70)
    print("QUERY 3: VIP Customer Inactivity & Churn Risk Detection")
    print("Notional Systems: MongoDB User Financial History + HBase Browsing Recency")
    print("-" * 70)

    q3 = """
    SELECT 
        u.user_id,
        u.geo_data.city,
        u.geo_data.state,
        COUNT(DISTINCT t.transaction_id) AS total_purchases,
        ROUND(SUM(t.total), 2) AS lifetime_spend,
        MAX(t.timestamp) AS last_purchase_date,
        COUNT(DISTINCT s.session_id) AS total_browsing_sessions
    FROM v_users u
    INNER JOIN v_transactions t ON u.user_id = t.user_id
    LEFT JOIN v_sessions s ON u.user_id = s.user_id
    GROUP BY u.user_id, u.geo_data.city, u.geo_data.state
    HAVING lifetime_spend > 5000 AND total_browsing_sessions >= 15
    ORDER BY lifetime_spend DESC
    LIMIT 10
    """
    df_q3 = spark.sql(q3)
    df_q3.show(truncate=False)

    # -------------------------------------------------------------
    # Query 4: Optimization Demonstration (Broadcast Join Plan)
    # -------------------------------------------------------------
    print("\n" + "-" * 70)
    print("QUERY 4: Catalyst Optimizer Plan & Broadcast Hash Join Optimization")
    print("-" * 70)

    q4 = """
    SELECT /*+ BROADCAST(c) */
        c.name AS category_name,
        COUNT(p.product_id) AS total_products,
        ROUND(AVG(p.base_price), 2) AS avg_base_price,
        SUM(p.current_stock) AS total_inventory
    FROM v_products p
    JOIN v_categories c ON p.category_id = c.category_id
    GROUP BY c.name
    ORDER BY total_inventory DESC
    LIMIT 5
    """
    df_q4 = spark.sql(q4)
    df_q4.show(truncate=False)
    print("\n[+] Physical Execution Plan (Verifying BroadcastExchange & HashJoin):")
    df_q4.explain()

    spark.stop()
    print("\n[✓] All Spark SQL Federated Queries Executed Successfully.")


if __name__ == "__main__":
    run_spark_sql()

