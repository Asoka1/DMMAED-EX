#!/usr/bin/env python3
"""
hbase_loader.py
HBase Data Ingestion Pipeline for User Sessions & Product Metrics
AUCA Big Data Analytics Final Project
Author: Asoka Niyonsaba Jean Claude (ID: 101410)

Constructs wide-column rows with reverse-timestamp row keys for fast recent-session retrieval:
Row Key Structure: {user_id}#{(9999999999 - epoch_timestamp):010d}#{session_id}
"""

import os
import sys
import json
import time
from glob import glob
from datetime import datetime
from collections import defaultdict

# Add parent directory to path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, PROJECT_ROOT)

from hbase.hbase_engine import get_hbase_connection


def iso_to_epoch(iso_str):
    try:
        dt = datetime.fromisoformat(iso_str)
        return int(dt.timestamp())
    except Exception:
        return 0


def load_hbase_datasets(data_dir=os.path.join(PROJECT_ROOT, "data"), max_sessions=35000):
    start_time = time.time()
    conn = get_hbase_connection()
    user_sessions_table = conn.table("user_sessions")
    product_metrics_table = conn.table("product_metrics")

    print("\n" + "=" * 70)
    print("STAGE 2: HBASE WIDE-COLUMN DATA MODELING & INGESTION")
    print("=" * 70)

    # 1. Load Sessions into user_sessions table
    session_files = sorted(glob(os.path.join(data_dir, "sessions_*.json")))
    loaded_sessions = 0
    product_daily_views = defaultdict(int)
    product_daily_cart = defaultdict(int)

    for sf in session_files:
        with open(sf) as f:
            chunk = json.load(f)

        for sess in chunk:
            user_id = sess["user_id"]
            session_id = sess["session_id"]
            start_iso = sess.get("start_time", "")
            epoch_sec = iso_to_epoch(start_iso)
            date_str = start_iso.split("T")[0] if "T" in start_iso else "2026-01-01"

            # Reverse timestamp (9999999999 - epoch) ensures latest sessions come FIRST
            rev_ts = 9999999999 - epoch_sec
            row_key = f"{user_id}#{rev_ts:010d}#{session_id}"

            geo = sess.get("geo_data", {})
            dev = sess.get("device_profile", {})
            cart = sess.get("cart_contents", {})
            page_views = sess.get("page_views", [])
            viewed_prods = sess.get("viewed_products", [])

            row_data = {
                # Column Family 's' (Session Core)
                "s:start_time": start_iso,
                "s:end_time": sess.get("end_time", ""),
                "s:duration_seconds": str(sess.get("duration_seconds", 0)),
                "s:conversion_status": sess.get("conversion_status", "browsed"),
                "s:referrer": sess.get("referrer", "direct"),

                # Column Family 'd' (Device)
                "d:type": dev.get("type", "unknown"),
                "d:os": dev.get("os", "unknown"),
                "d:browser": dev.get("browser", "unknown"),

                # Column Family 'g' (Geo)
                "g:city": geo.get("city", "unknown"),
                "g:state": geo.get("state", "unknown"),
                "g:country": geo.get("country", "US"),
                "g:ip": geo.get("ip_address", "127.0.0.1"),

                # Column Family 'm' (Metrics)
                "m:page_views_count": str(len(page_views)),
                "m:viewed_products_count": str(len(viewed_prods)),
                "m:cart_items_count": str(sum(c.get("quantity", 0) for c in cart.values()) if isinstance(cart, dict) else 0)
            }

            user_sessions_table.put(row_key, row_data)
            loaded_sessions += 1

            # Accumulate daily product interaction metrics
            for pid in viewed_prods:
                product_daily_views[(pid, date_str)] += 1
            if isinstance(cart, dict):
                for pid, cinfo in cart.items():
                    product_daily_cart[(pid, date_str)] += cinfo.get("quantity", 1)

            if loaded_sessions >= max_sessions:
                break
        if loaded_sessions >= max_sessions:
            break

    print(f"[+] Loaded {loaded_sessions:,} session records into HBase table 'user_sessions'")
    print(f"    Sample Row Key format: 'user_000042#8258380800#sess_a7b3c9d8e2'")

    # 2. Ingest Product Daily Metrics table
    prod_path = os.path.join(data_dir, "products.json")
    with open(prod_path) as f:
        products = {p["product_id"]: p for p in json.load(f)}

    metrics_count = 0
    for (pid, dstr), v_count in product_daily_views.items():
        rk = f"{pid}#{dstr}"
        pinfo = products.get(pid, {})
        row_data = {
            "d:views": str(v_count),
            "d:cart_adds": str(product_daily_cart.get((pid, dstr), 0)),
            "i:stock": str(pinfo.get("current_stock", 0)),
            "i:price": str(pinfo.get("base_price", 0.0))
        }
        product_metrics_table.put(rk, row_data)
        metrics_count += 1

    print(f"[+] Loaded {metrics_count:,} daily metric records into HBase table 'product_metrics'")
    print(f"    Sample Row Key format: 'prod_00123#2026-03-12'")

    elapsed = time.time() - start_time
    print(f"[✓] HBase Table Ingestion Completed in {elapsed:.2f} seconds.")
    return conn


if __name__ == "__main__":
    load_hbase_datasets()

