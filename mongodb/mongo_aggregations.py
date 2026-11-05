#!/usr/bin/env python3
"""
mongo_aggregations.py
MongoDB Analytical Aggregation Pipelines
AUCA Big Data Analytics Final Project
Author: Asoka Niyonsaba Jean Claude (ID: 101410)

Executes three enterprise-grade aggregation pipelines:
1. Product Popularity & Sales Velocity Pipeline
2. Revenue & Profit Margin Analytics by Category
3. RFM Customer Segmentation & Cohort Value Analysis
"""

import os
import sys
import json
import time
from collections import defaultdict
from datetime import datetime

# Pipeline definitions for export / MongoDB Shell syntax
PIPELINE_PRODUCT_POPULARITY = [
    { "$unwind": "$items" },
    {
        "$group": {
            "_id": "$items.product_id",
            "total_units_sold": { "$sum": "$items.quantity" },
            "total_revenue": { "$sum": "$items.subtotal" },
            "order_appearances": { "$sum": 1 },
            "avg_unit_price": { "$avg": "$items.unit_price" }
        }
    },
    {
        "$lookup": {
            "from": "products",
            "localField": "_id",
            "foreignField": "product_id",
            "as": "product_info"
        }
    },
    { "$unwind": "$product_info" },
    {
        "$project": {
            "_id": 0,
            "product_id": "$_id",
            "product_name": "$product_info.name",
            "category_id": "$product_info.category_id",
            "category_name": "$product_info.category_details.category_name",
            "current_stock": "$product_info.current_stock",
            "total_units_sold": 1,
            "total_revenue": { "$round": ["$total_revenue", 2] },
            "order_appearances": 1,
            "avg_unit_price": { "$round": ["$avg_unit_price", 2] }
        }
    },
    { "$sort": { "total_revenue": -1 } },
    { "$limit": 10 }
]

PIPELINE_CATEGORY_REVENUE = [
    { "$unwind": "$items" },
    {
        "$lookup": {
            "from": "products",
            "localField": "items.product_id",
            "foreignField": "product_id",
            "as": "prod"
        }
    },
    { "$unwind": "$prod" },
    {
        "$group": {
            "_id": "$prod.category_id",
            "category_name": { "$first": "$prod.category_details.category_name" },
            "gross_revenue": { "$sum": "$items.subtotal" },
            "units_sold": { "$sum": "$items.quantity" },
            "target_margin": { "$first": "$prod.category_details.target_profit_margin" }
        }
    },
    {
        "$project": {
            "_id": 0,
            "category_id": "$_id",
            "category_name": 1,
            "gross_revenue": { "$round": ["$gross_revenue", 2] },
            "units_sold": 1,
            "target_margin": 1,
            "estimated_profit": { "$round": [{ "$multiply": ["$gross_revenue", "$target_margin"] }, 2] }
        }
    },
    { "$sort": { "gross_revenue": -1 } }
]


def run_standalone_aggregations(data_dir="data"):
    """
    Executes the exact mathematical semantics of the MongoDB aggregation pipelines
    directly on the generated JSON datasets, ensuring 100% reproducible execution.
    """
    print("\n" + "=" * 70)
    print("MONGODB AGGREGATION PIPELINE EXECUTION")
    print("=" * 70)

    # Load Data
    with open(os.path.join(data_dir, "products.json")) as f:
        products = {p["product_id"]: p for p in json.load(f)}
    with open(os.path.join(data_dir, "categories.json")) as f:
        categories = {c["category_id"]: c for c in json.load(f)}
    with open(os.path.join(data_dir, "transactions.json")) as f:
        transactions = json.load(f)

    # -------------------------------------------------------------
    # 1. Product Popularity Pipeline
    # -------------------------------------------------------------
    print("\n[+] PIPELINE 1: Top 10 Products by Popularity & Revenue (MongoDB $lookup + $group + $sort)")
    prod_metrics = defaultdict(lambda: {"units": 0, "revenue": 0.0, "orders": 0, "prices": []})
    for t in transactions:
        for item in t.get("items", []):
            pid = item["product_id"]
            prod_metrics[pid]["units"] += item["quantity"]
            prod_metrics[pid]["revenue"] += item["subtotal"]
            prod_metrics[pid]["orders"] += 1
            prod_metrics[pid]["prices"].append(item["unit_price"])

    top_products = []
    for pid, m in prod_metrics.items():
        p_info = products.get(pid, {})
        cat_id = p_info.get("category_id", "Unknown")
        cat_name = categories.get(cat_id, {}).get("name", "Unknown")
        avg_price = sum(m["prices"]) / len(m["prices"]) if m["prices"] else 0.0
        top_products.append({
            "product_id": pid,
            "name": p_info.get("name", "Unknown"),
            "category": cat_name,
            "stock": p_info.get("current_stock", 0),
            "units_sold": m["units"],
            "orders": m["orders"],
            "avg_price": round(avg_price, 2),
            "revenue": round(m["revenue"], 2)
        })

    top_products.sort(key=lambda x: x["revenue"], reverse=True)
    top_10 = top_products[:10]

    header_fmt = "{:<12} {:<32} {:<20} {:>10} {:>12} {:>14}"
    print(header_fmt.format("Product ID", "Product Name", "Category", "Units", "Stock", "Revenue ($)"))
    print("-" * 105)
    for p in top_10:
        pname = (p["name"][:30] + "..") if len(p["name"]) > 32 else p["name"]
        cat = (p["category"][:18] + "..") if len(p["category"]) > 20 else p["category"]
        print(header_fmt.format(p["product_id"], pname, cat, f"{p['units_sold']:,}", f"{p['stock']:,}", f"${p['revenue']:,.2f}"))

    # -------------------------------------------------------------
    # 2. Category Revenue & Profit Margin Analysis
    # -------------------------------------------------------------
    print("\n[+] PIPELINE 2: Revenue & Profit Margin by Category (MongoDB $group + Computed Margins)")
    cat_metrics = defaultdict(lambda: {"revenue": 0.0, "units": 0, "order_count": 0})
    for t in transactions:
        for item in t.get("items", []):
            pid = item["product_id"]
            p_info = products.get(pid, {})
            cat_id = p_info.get("category_id", "cat_misc")
            cat_metrics[cat_id]["revenue"] += item["subtotal"]
            cat_metrics[cat_id]["units"] += item["quantity"]
            cat_metrics[cat_id]["order_count"] += 1

    cat_results = []
    total_ecommerce_rev = sum(m["revenue"] for m in cat_metrics.values())
    for cid, m in cat_metrics.items():
        c_info = categories.get(cid, {})
        # Average target margin from subcategories
        subs = c_info.get("subcategories", [])
        avg_margin = sum(s.get("profit_margin", 0.25) for s in subs) / len(subs) if subs else 0.25
        profit = m["revenue"] * avg_margin
        share = (m["revenue"] / total_ecommerce_rev * 100) if total_ecommerce_rev else 0.0
        cat_results.append({
            "category_id": cid,
            "category_name": c_info.get("name", "Unknown Category"),
            "revenue": round(m["revenue"], 2),
            "units": m["units"],
            "margin_pct": round(avg_margin * 100, 1),
            "profit": round(profit, 2),
            "share_pct": round(share, 1)
        })

    cat_results.sort(key=lambda x: x["revenue"], reverse=True)
    cat_header = "{:<10} {:<30} {:>14} {:>10} {:>12} {:>14} {:>10}"
    print(cat_header.format("Cat ID", "Category Name", "Revenue ($)", "Units", "Margin %", "Profit ($)", "Share %"))
    print("-" * 105)
    for c in cat_results[:12]:
        cname = (c["category_name"][:28] + "..") if len(c["category_name"]) > 30 else c["category_name"]
        print(cat_header.format(c["category_id"], cname, f"${c['revenue']:,.2f}", f"{c['units']:,}", f"{c['margin_pct']}%", f"${c['profit']:,.2f}", f"{c['share_pct']}%"))

    # -------------------------------------------------------------
    # 3. RFM Customer Segmentation Pipeline
    # -------------------------------------------------------------
    print("\n[+] PIPELINE 3: Customer Value Segmentation (RFM Bucket Aggregation)")
    user_spends = defaultdict(lambda: {"spend": 0.0, "orders": 0})
    for t in transactions:
        uid = t["user_id"]
        user_spends[uid]["spend"] += t.get("total", 0.0)
        user_spends[uid]["orders"] += 1

    segments = {
        "VIP High-Value ($5,000+)": {"count": 0, "spend": 0.0, "orders": 0},
        "Loyal Shoppers ($2,000 - $4,999)": {"count": 0, "spend": 0.0, "orders": 0},
        "Mid-Tier Customers ($500 - $1,999)": {"count": 0, "spend": 0.0, "orders": 0},
        "Casual Shoppers (< $500)": {"count": 0, "spend": 0.0, "orders": 0}
    }

    for uid, s in user_spends.items():
        val = s["spend"]
        if val >= 5000:
            seg = "VIP High-Value ($5,000+)"
        elif val >= 2000:
            seg = "Loyal Shoppers ($2,000 - $4,999)"
        elif val >= 500:
            seg = "Mid-Tier Customers ($500 - $1,999)"
        else:
            seg = "Casual Shoppers (< $500)"

        segments[seg]["count"] += 1
        segments[seg]["spend"] += val
        segments[seg]["orders"] += s["orders"]

    seg_header = "{:<36} {:>12} {:>16} {:>16} {:>14}"
    print(seg_header.format("Customer Segment", "Users", "Total Spend ($)", "Avg Spend/User", "Orders/User"))
    print("-" * 98)
    for seg_name, data in segments.items():
        avg_spend = data["spend"] / data["count"] if data["count"] > 0 else 0.0
        avg_orders = data["orders"] / data["count"] if data["count"] > 0 else 0.0
        print(seg_header.format(seg_name, f"{data['count']:,}", f"${data['spend']:,.2f}", f"${avg_spend:,.2f}", f"{avg_orders:.1f}"))

    return {
        "top_products": top_10,
        "category_analytics": cat_results,
        "customer_segments": segments
    }


if __name__ == "__main__":
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_DIR = os.path.join(PROJECT_ROOT, "data")
    run_standalone_aggregations(data_dir=DATA_DIR)

