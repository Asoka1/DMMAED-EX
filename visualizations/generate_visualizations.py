#!/usr/bin/env python3
"""
generate_visualizations.py
Publication-Quality Analytical Visualizations (300 DPI)
AUCA Big Data Analytics Final Project
Author: Asoka Niyonsaba Jean Claude (ID: 101410)

Generates six professional analytical charts:
1. Category Revenue & Profit Margin Distribution
2. E-Commerce Micro-Conversion Funnel & Drop-Off
3. RFM Customer Segmentation & Value Contribution
4. Product Association Affinity (Market Basket Lift & Confidence)
5. User Cohort Retention & Spending Evolution Heatmap
6. Multi-Model Architecture Query Latency Benchmark
"""

import os
import sys
import json
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
import numpy as np
import pandas as pd

# Styling configuration
plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
plt.rcParams["font.sans-serif"] = "DejaVu Sans"
plt.rcParams["axes.edgecolor"] = "#cccccc"
plt.rcParams["axes.linewidth"] = 0.8

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "visualizations")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def plot_01_category_revenue():
    print("[*] Generating Chart 01: Category Revenue & Profit Margins...")
    with open(os.path.join(DATA_DIR, "categories.json")) as f:
        categories = {c["category_id"]: c for c in json.load(f)}
    with open(os.path.join(DATA_DIR, "products.json")) as f:
        products = {p["product_id"]: p for p in json.load(f)}
    with open(os.path.join(DATA_DIR, "transactions.json")) as f:
        txns = json.load(f)

    cat_rev = {}
    for c in categories:
        cat_rev[c] = {"revenue": 0.0, "units": 0}

    for t in txns:
        for it in t.get("items", []):
            pid = it["product_id"]
            cid = products.get(pid, {}).get("category_id")
            if cid in cat_rev:
                cat_rev[cid]["revenue"] += it["subtotal"]
                cat_rev[cid]["units"] += it["quantity"]

    df_list = []
    for cid, val in cat_rev.items():
        c_info = categories.get(cid, {})
        subs = c_info.get("subcategories", [])
        margin = sum(s.get("profit_margin", 0.25) for s in subs) / len(subs) if subs else 0.25
        df_list.append({
            "cat_id": cid,
            "cat_name": c_info.get("name", "Category"),
            "revenue": val["revenue"],
            "margin_pct": margin * 100,
            "profit": val["revenue"] * margin
        })
    df = pd.DataFrame(df_list).sort_values("revenue", ascending=False).head(10)

    fig, ax1 = plt.subplots(figsize=(12, 6), dpi=300)
    bars = ax1.bar(range(len(df)), df["revenue"] / 1000, color="#1f77b4", width=0.55, label="Gross Revenue ($k)", alpha=0.88)
    ax1.set_ylabel("Gross Revenue ($ in Thousands)", color="#1f77b4", fontsize=12, fontweight="bold")
    ax1.set_xticks(range(len(df)))
    ax1.set_xticklabels([n[:16] + ".." if len(n) > 17 else n for n in df["cat_name"]], rotation=25, ha="right", fontsize=10)
    ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f"${x:,.0f}k"))

    ax2 = ax1.twinx()
    lines = ax2.plot(range(len(df)), df["margin_pct"], color="#d62728", marker="o", linewidth=2.5, markersize=7, label="Profit Margin (%)")
    ax2.set_ylabel("Target Profit Margin (%)", color="#d62728", fontsize=12, fontweight="bold")
    ax2.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f"{x:.1f}%"))
    ax2.grid(False)

    plt.title("E-Commerce Top 10 Product Categories: Revenue vs. Profit Margin", fontsize=14, fontweight="bold", pad=15)
    fig.tight_layout()
    out_path = os.path.join(OUTPUT_DIR, "01_category_revenue_and_margins.png")
    plt.savefig(out_path, dpi=300)
    plt.close()
    print(f"[✓] Saved: {out_path}")


def plot_02_conversion_funnel():
    print("[*] Generating Chart 02: E-Commerce Conversion Funnel...")
    # Exact counts from multi-model integration
    stages = [
        "1. Total Landed Sessions",
        "2. Viewed Product Detail",
        "3. Added Items to Cart",
        "4. Completed Purchase"
    ]
    counts = [35000, 34455, 21705, 5877]
    colors = ["#2b5c8f", "#3a82c4", "#5ca7e6", "#2ca02c"]

    fig, ax = plt.subplots(figsize=(10, 6), dpi=300)
    bars = ax.barh(stages[::-1], [c / 1000 for c in counts[::-1]], color=colors[::-1], height=0.52, edgecolor="#333333", alpha=0.9)
    
    # Annotate conversion rates
    for idx, (bar, count) in enumerate(zip(bars[::-1], counts)):
        pct_of_first = (count / counts[0]) * 100
        stage_drop = f"({(count / counts[idx-1] * 100):.1f}% of prior)" if idx > 0 else "(Baseline)"
        ax.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2,
                f"{count:,} ({pct_of_first:.1f}%) {stage_drop}",
                va="center", ha="left", fontsize=10, fontweight="bold", color="#222222")

    ax.set_xlabel("Sessions Count (Thousands)", fontsize=12, fontweight="bold")
    ax.set_title("E-Commerce User Journey: Multi-Stage Conversion Funnel & Drop-Off", fontsize=14, fontweight="bold", pad=15)
    ax.set_xlim(0, 48)
    fig.tight_layout()
    out_path = os.path.join(OUTPUT_DIR, "02_conversion_funnel_dropout.png")
    plt.savefig(out_path, dpi=300)
    plt.close()
    print(f"[✓] Saved: {out_path}")


def plot_03_rfm_customer_segmentation():
    print("[*] Generating Chart 03: Customer Value Segmentation...")
    segments = ["VIP High-Value\n($5,000+)", "Loyal Shoppers\n($2,000 - $4,999)", "Mid-Tier\n($500 - $1,999)", "Casual Shoppers\n(< $500)"]
    user_counts = [1227, 1043, 210, 14]
    revenue_shares = [9000189.60, 3742264.70, 295527.87, 4145.95]
    colors = ["#1b9e77", "#386cb0", "#fdb462", "#e7298a"]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6), dpi=300)

    # Donut 1: Users count share
    wedges1, texts1, autotexts1 = ax1.pie(user_counts, labels=segments, autopct="%1.1f%%", startangle=140, colors=colors,
                                          wedgeprops=dict(width=0.4, edgecolor='white', linewidth=2))
    for t in autotexts1:
        t.set_fontsize(10)
        t.set_fontweight("bold")
    ax1.set_title("Customer Distribution by Tier (N = 2,500)", fontsize=13, fontweight="bold")

    # Donut 2: Revenue share
    wedges2, texts2, autotexts2 = ax2.pie(revenue_shares, labels=segments, autopct="%1.1f%%", startangle=140, colors=colors,
                                          wedgeprops=dict(width=0.4, edgecolor='white', linewidth=2))
    for t in autotexts2:
        t.set_fontsize(10)
        t.set_fontweight("bold")
    ax2.set_title("Revenue Contribution by Tier ($13.04M)", fontsize=13, fontweight="bold")

    plt.suptitle("RFM Customer Segmentation: User Proportions vs. Revenue Dominance", fontsize=15, fontweight="bold", y=1.02)
    fig.tight_layout()
    out_path = os.path.join(OUTPUT_DIR, "03_rfm_customer_segmentation.png")
    plt.savefig(out_path, dpi=300)
    plt.close()
    print(f"[✓] Saved: {out_path}")


def plot_04_product_affinity():
    print("[*] Generating Chart 04: Product Co-Purchase Affinity Rules...")
    # Top 8 affinity pairs from PySpark batch processing
    rules = [
        ("Grass-Roots System", "Secondary Artif.", 92.88, 0.1176),
        ("Well-Modulated Task", "Demand-Driven Proj.", 90.91, 0.1333),
        ("Open-Source Support", "Empowering Conglom.", 89.29, 0.0833),
        ("Dynamic Capability", "Fresh-Thinking Para.", 85.71, 0.1429),
        ("Driven Emulation", "Pricing Strategy", 83.10, 0.1053),
        ("Zero Admin Multi", "Graphic Interface", 76.73, 0.1176),
        ("3Rdgen Functional", "Demand Projection", 75.76, 0.1111),
        ("Actuating Hierarchy", "Trending Algorithm", 75.76, 0.1111)
    ]
    pair_labels = [f"{r[0]}  ➔  {r[1]}" for r in rules]
    lifts = [r[2] for r in rules]
    confs = [r[3] * 100 for r in rules]

    fig, ax1 = plt.subplots(figsize=(12, 6), dpi=300)
    y_pos = np.arange(len(rules))
    bars = ax1.barh(y_pos, lifts, color="#6baed6", height=0.55, edgecolor="#2171b5", label="Lift Ratio")
    ax1.set_yticks(y_pos)
    ax1.set_yticklabels(pair_labels, fontsize=10)
    ax1.set_xlabel("Lift Ratio (Co-Occurrence / Expected Chance)", fontsize=12, fontweight="bold", color="#2171b5")
    ax1.invert_yaxis()

    for bar, conf in zip(bars, confs):
        ax1.text(bar.get_width() + 1.2, bar.get_y() + bar.get_height() / 2,
                 f"Lift: {bar.get_width():.1f}x (Conf: {conf:.1f}%)",
                 va="center", ha="left", fontsize=9.5, fontweight="bold", color="#333333")

    ax1.set_xlim(0, 115)
    plt.title("Market Basket Analysis: Top Product Recommendation Association Rules", fontsize=14, fontweight="bold", pad=15)
    fig.tight_layout()
    out_path = os.path.join(OUTPUT_DIR, "04_market_basket_product_affinity.png")
    plt.savefig(out_path, dpi=300)
    plt.close()
    print(f"[✓] Saved: {out_path}")


def plot_05_cohort_retention():
    print("[*] Generating Chart 05: User Cohort Spending Retention...")
    cohorts = ["2026-02", "2026-03", "2026-04", "2026-05"]
    offsets = [3, 4, 5, 6, 7, 8]
    data = np.array([
        [0, 0, 0, 408.6, 493.5, 535.2],
        [0, 0, 614.0, 748.8, 760.1, 91.7],
        [0, 584.8, 750.6, 765.5, 93.5, 0],
        [605.8, 743.3, 806.4, 0, 0, 0]
    ])

    fig, ax = plt.subplots(figsize=(10, 6), dpi=300)
    sns.heatmap(data, annot=True, fmt=".1f", cmap="YlGnBu", cbar_kws={'label': 'Spending ($ in Thousands)'},
                xticklabels=[f"Month +{o}" for o in offsets], yticklabels=cohorts, ax=ax, linewidths=1.2, linecolor="white")
    ax.set_title("Customer Cohort Spending Trajectory ($k by Registration Month)", fontsize=14, fontweight="bold", pad=15)
    ax.set_ylabel("Registration Cohort", fontsize=12, fontweight="bold")
    ax.set_xlabel("Elapsed Months Since Registration", fontsize=12, fontweight="bold")
    fig.tight_layout()
    out_path = os.path.join(OUTPUT_DIR, "05_user_cohort_spending_retention.png")
    plt.savefig(out_path, dpi=300)
    plt.close()
    print(f"[✓] Saved: {out_path}")


def plot_06_benchmark_latency():
    print("[*] Generating Chart 06: Multi-Engine Query Latency Benchmarks...")
    engines = [
        "HBase Wide-Column\n(Recent Session Scan)",
        "HBase Column Slice\n(Device/Metrics Projection)",
        "MongoDB Aggregation\n(Product Popularity $lookup)",
        "MongoDB Aggregation\n(Category Margin $group)",
        "Spark SQL Federated\n(3-Way Distributed Join)"
    ]
    latencies_ms = [0.024, 0.017, 12.4, 8.6, 480.0]  # Milliseconds
    colors = ["#2ca02c", "#2ca02c", "#ff7f0e", "#ff7f0e", "#1f77b4"]

    fig, ax = plt.subplots(figsize=(11, 6), dpi=300)
    y_pos = range(len(engines))
    bars = ax.barh(y_pos, latencies_ms, color=colors, height=0.55, edgecolor="#333333", alpha=0.9)
    ax.set_xscale("log")
    ax.set_yticks(y_pos)
    ax.set_yticklabels(engines, fontsize=10.5)
    ax.set_xlabel("Query Latency in Milliseconds (Log Scale)", fontsize=12, fontweight="bold")
    ax.set_title("Multi-Model Database Benchmark: Workload Latency by Engine Model", fontsize=14, fontweight="bold", pad=15)
    ax.invert_yaxis()

    for bar, val in zip(bars, latencies_ms):
        unit = "ms" if val < 1000 else "s"
        txt = f"{val:.3f} ms" if val < 1.0 else f"{val:.1f} ms"
        ax.text(bar.get_width() * 1.3, bar.get_y() + bar.get_height() / 2, txt,
                va="center", ha="left", fontsize=10, fontweight="bold", color="#222222")

    ax.set_xlim(0.005, 3000)
    fig.tight_layout()
    out_path = os.path.join(OUTPUT_DIR, "06_multi_model_query_latency_comparison.png")
    plt.savefig(out_path, dpi=300)
    plt.close()
    print(f"[✓] Saved: {out_path}")


def generate_all_plots():
    print("\n" + "=" * 70)
    print("STAGE 5: GENERATING 300-DPI PUBLICATION VISUALIZATIONS")
    print("=" * 70)
    plot_01_category_revenue()
    plot_02_conversion_funnel()
    plot_03_rfm_customer_segmentation()
    plot_04_product_affinity()
    plot_05_cohort_retention()
    plot_06_benchmark_latency()
    print("\n[✓] All 6 high-resolution charts generated successfully.")


if __name__ == "__main__":
    generate_all_plots()
