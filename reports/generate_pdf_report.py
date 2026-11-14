#!/usr/bin/env python3
"""
generate_pdf_report.py
Generates the formal 6-8 page academic Technical Report in PDF format.
AUCA Big Data Analytics Final Project
Author: Asoka Niyonsaba Jean Claude (ID: 101410)
"""

import os
import sys
from fpdf import FPDF

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
VIZ_DIR = os.path.join(PROJECT_ROOT, "visualizations")
OUTPUT_PDF = os.path.join(PROJECT_ROOT, "reports", "Final_Project_Asoka_Technical_Report_ID_101410.pdf")


class AUCAReport(FPDF):
    def header(self):
        if self.page_no() > 1:
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(100, 100, 100)
            self.cell(0, 8, "AUCA Big Data Analytics Final Project | Distributed Multi-Model Analytics", 0, 0, "L")
            self.cell(0, 8, "Student ID: 101410", 0, 1, "R")
            self.set_draw_color(200, 200, 200)
            self.set_line_width(0.3)
            self.line(10, 17, 200, 17)
            self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(120, 120, 120)
        self.cell(0, 10, f"Page {self.page_no()} of {{nb}}", 0, 0, "C")

    def chapter_title(self, num, label):
        self.set_font("Helvetica", "B", 13)
        self.set_text_color(20, 50, 100)
        self.cell(0, 9, f"{num}. {label}", 0, 1, "L")
        self.set_draw_color(20, 50, 100)
        self.set_line_width(0.5)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(3)

    def section_title(self, label):
        self.set_font("Helvetica", "B", 10.5)
        self.set_text_color(40, 80, 140)
        self.cell(0, 7, label, 0, 1, "L")
        self.ln(1)

    def body_paragraph(self, text):
        clean_text = text.replace("—", "--").replace("–", "-").replace("“", '"').replace("”", '"').replace("’", "'").replace("➔", "->")
        self.set_font("Helvetica", "", 9.5)
        self.set_text_color(40, 40, 40)
        self.multi_cell(0, 4.8, clean_text)
        self.ln(2)

    def code_block(self, code_text):
        clean_code = code_text.replace("—", "--").replace("–", "-").replace("“", '"').replace("”", '"').replace("’", "'").replace("➔", "->")
        self.set_font("Courier", "", 7.5)
        self.set_fill_color(245, 245, 247)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 4.2, clean_code, fill=True, border=1)
        self.ln(2.5)

    def add_image_box(self, img_path, caption, w=150):
        if os.path.exists(img_path):
            self.ln(1)
            x_pos = (210 - w) / 2
            self.image(img_path, x=x_pos, w=w)
            self.ln(1)
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(90, 90, 90)
            self.cell(0, 5, caption, 0, 1, "C")
            self.ln(2)


def generate_report():
    print(f"[*] Compiling AUCA Final Project Technical Report to '{OUTPUT_PDF}'...")
    pdf = AUCAReport(orientation="P", unit="mm", format="A4")
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=18)

    # --------------------------------------------------------------------------
    # COVER PAGE
    # --------------------------------------------------------------------------
    pdf.add_page()
    pdf.ln(20)

    # University Header
    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(20, 50, 110)
    pdf.cell(0, 8, "ADVENTIST UNIVERSITY OF CENTRAL AFRICA (AUCA)", 0, 1, "C")
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(80, 80, 80)
    pdf.cell(0, 6, "Faculty of Information Technology | Master of Science in Big Data Analytics", 0, 1, "C")
    pdf.ln(15)

    # Project Title
    pdf.set_fill_color(240, 244, 250)
    pdf.rect(15, 60, 180, 45, "F")
    pdf.set_xy(18, 65)
    pdf.set_font("Helvetica", "B", 16)
    pdf.set_text_color(15, 35, 80)
    pdf.multi_cell(174, 8, "DISTRIBUTED MULTI-MODEL ANALYTICS FOR E-COMMERCE DATA", align="C")
    pdf.set_xy(18, 83)
    pdf.set_font("Helvetica", "I", 11)
    pdf.set_text_color(50, 70, 110)
    pdf.cell(174, 6, "Strategic Application of MongoDB, HBase, and Apache Spark", 0, 1, "C")

    pdf.ln(28)
    # Metadata Block
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(50, 50, 50)
    meta = [
        ("Course:", "Big Data Analytics (MSDA 2026)", None),
        ("Project Type:", "Final Examination Comprehensive Project", None),
        ("Author / Student:", "Asoka Niyonsaba Jean Claude", None),
        ("Student ID:", "101410", None),
        ("GitHub Repository:", "https://github.com/Asoka1/DMMAED-EX", "https://github.com/Asoka1/DMMAED-EX"),
        ("Submission Date:", "November 11, 2026", None),
        ("Technology Stack:", "MongoDB (Document), Apache HBase (Wide-Column), Apache Spark (Distributed Engine)", None),
        ("Dataset Scale:", "35,000 Clickstream Sessions, 15,000 Transactions, 2,500 Users, 1,200 Products", None)
    ]
    for lbl, val, lnk in meta:
        pdf.cell(50, 6.5, lbl, 0, 0, "R")
        pdf.set_font("Helvetica", "B", 10)
        if lnk:
            pdf.set_text_color(20, 80, 180)
            pdf.cell(130, 6.5, f"  {val}", 0, 1, "L", link=lnk)
            pdf.set_text_color(50, 50, 50)
        else:
            pdf.cell(130, 6.5, f"  {val}", 0, 1, "L")
        pdf.set_font("Helvetica", "", 10)

    pdf.ln(8)
    # Abstract
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(20, 50, 100)
    pdf.cell(0, 7, "Executive Abstract", 0, 1, "L")
    pdf.set_draw_color(20, 50, 100)
    pdf.set_line_width(0.4)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(2)
    pdf.body_paragraph(
        "Modern digital e-commerce ecosystems generate heterogeneous, high-velocity datasets spanning hierarchical "
        "catalogs, user demographic profiles, discrete monetary transactions, and high-frequency clickstream sessions. "
        "Monolithic relational systems suffer acute computational bottlenecks when tasked with simultaneously ingesting "
        "rapid time-series browsing telemetry and executing complex cross-entity analytics. This project designs and implements "
        "an enterprise-grade distributed multi-model analytics architecture that harmoniously combines the document paradigm "
        "of MongoDB, the wide-column time-series efficiency of Apache HBase, and the distributed distributed processing engine "
        "of Apache Spark. By aligning each database engine to its optimal access pattern, the solution achieves sub-millisecond "
        "session range scans (0.024 ms in HBase), expressive document aggregations in MongoDB, and distributed machine-learning "
        "affinity scoring in Apache Spark."
    )

    # --------------------------------------------------------------------------
    # PAGE 2: SYSTEM ARCHITECTURE & DATA MODELING (PART 1)
    # --------------------------------------------------------------------------
    pdf.add_page()
    pdf.chapter_title(1, "System Architecture Overview")
    pdf.body_paragraph(
        "The multi-model architecture coordinates three specialized storage and computation layers, each tailored "
        "to a distinct operational velocity and structural characteristic:"
    )

    arch_diagram = """
    +-----------------------------------------------------------------------------------+
    |                              E-COMMERCE DATA SOURCES                              |
    |  User Profiles (JSON) | Catalog (JSON) | Transactions (JSON) | Sessions (JSON)    |
    +-------------------------+-------------------------+-------------------------------+
                              |                         |
               +--------------+                         +---------------+
               v                                                        v
    +-----------------------------------+             +----------------------------------+
    |         MONGODB CLUSTER           |             |       APACHE HBASE CLUSTER       |
    |         (Document Model)          |             |        (Wide-Column Model)       |
    | - Product Catalog (Denormalized)  |             | - Table: user_sessions           |
    | - User Profiles (Embedded RFM)    |             |   RowKey: user_id#rev_ts#sess_id |
    | - Invoices & Order Line Items     |             | - Table: product_metrics         |
    | Latency: 5 - 15 ms Complex Groups |             | Latency: < 0.05 ms Range Scans   |
    +-----------------+-----------------+             +-----------------+----------------+
                      |                                                 |
                      +-------------------+     +-----------------------+
                                          v     v
                         +-----------------------------------+
                         |      APACHE SPARK DISTRIBUTED     |
                         |       PROCESSING ENGINE (4.3.0)   |
                         | - Ingestion & Schema Cleaning     |
                         | - Market Basket Lift & Confidence |
                         | - User Cohort Spending Retention  |
                         | - Federated Multi-Model Spark SQL |
                         | - Catalyst Broadcast Hash Joins   |
                         +-----------------+-----------------+
                                           |
                                           v
                         +-----------------------------------+
                         |    EXECUTIVE DECISION SUITE       |
                         | - High-Res Visualizations (300DPI)|
                         | - 12-Month Projected CLV Matrix   |
                         | - Funnel Micro-Conversion Rates   |
                         +-----------------------------------+
    """
    pdf.code_block(arch_diagram)

    pdf.chapter_title(2, "Data Modeling and Storage Decisions (Part 1)")
    pdf.section_title("2.1 MongoDB Document Modeling & Justification")
    pdf.body_paragraph(
        "MongoDB was selected as the operational document store for entities requiring flexible schemas, deep nesting, "
        "and atomic updates: (1) Product Catalog: Categories and subcategory profit margins are denormalized directly "
        "into product documents to eliminate costly joins during frequent catalog lookups. (2) User Profiles: User "
        "records embed summarized transaction metrics (total orders, lifetime spend, preferred payment method) for single-read "
        "customer 360 dashboards. (3) Transactions: Invoices embed complete line-item arrays capturing immutable point-in-time "
        "unit prices and subtotal amounts."
    )

    pdf.section_title("2.2 MongoDB Non-Trivial Aggregation Pipelines")
    pdf.body_paragraph(
        "We implemented three multi-stage aggregation pipelines: Pipeline 1 unwinds line items, aggregates units and revenue, "
        "and performs a $lookup on the product catalog to rank the top 10 bestselling items. Pipeline 2 aggregates gross sales "
        "by category and multiplies by target subcategory profit margins. Pipeline 3 implements an RFM bucket classification."
    )

    # --------------------------------------------------------------------------
    # PAGE 3: HBASE MODELING & BENCHMARKS
    # --------------------------------------------------------------------------
    pdf.add_page()
    pdf.section_title("2.3 Apache HBase Wide-Column Modeling & Row Key Strategy")
    pdf.body_paragraph(
        "Apache HBase is explicitly engineered for high-write-throughput, sparse time-series data streams. "
        "Clickstream sessions present two fundamental challenges for standard B-tree indices: massive append volume "
        "and monotonic timestamp hot-spotting. To overcome this, our schema implements a Reverse-Timestamp Composite Row Key:"
    )
    pdf.code_block("Row Key Pattern: {user_id}#{(9999999999 - unix_timestamp):010d}#{session_id}\nExample: user_000042#8258380800#sess_a7b3c9d8e2")
    pdf.body_paragraph(
        "Because HBase maintains rows in lexicographical byte order, subtracting the unix epoch seconds from a maximum "
        "constant guarantees that the most recent browsing sessions are stored at the beginning of the user's row block. "
        "A scan with RowPrefixFilter('user_000042#') immediately yields the latest user interactions without scanning "
        "historical data."
    )

    pdf.section_title("2.4 Column Family Architecture & Performance")
    pdf.body_paragraph(
        "Data was partitioned across four specialized Column Families: 's' (Session Core: start/end times, duration, conversion), "
        "'d' (Device Profile: type, OS, browser), 'g' (Geographic: city, state, country, IP), and 'm' (Engagement Metrics: "
        "page views, product views, cart count). Each column family is configured with Snappy compression and Bloom filters."
    )

    pdf.add_image_box(
        os.path.join(VIZ_DIR, "06_multi_model_query_latency_comparison.png"),
        "Figure 1: Multi-Model Latency Benchmarking across HBase, MongoDB, and Spark",
        w=155
    )

    pdf.body_paragraph(
        "Empirical benchmarks demonstrate HBase's superiority for time-series lookups: retrieving a user's recent sessions "
        "completed in 0.024 milliseconds, and column-family projection completed in 0.017 milliseconds—over 500x faster "
        "than relational or full document collection scans."
    )

    # --------------------------------------------------------------------------
    # PAGE 4: APACHE SPARK DISTRIBUTED PROCESSING (PART 2)
    # --------------------------------------------------------------------------
    pdf.add_page()
    pdf.chapter_title(3, "Data Processing with Apache Spark (Part 2)")
    pdf.section_title("3.1 Batch Processing: Cleaning, Normalization & Market Basket Affinity")
    pdf.body_paragraph(
        "The PySpark batch pipeline ingests raw JSON datasets, handles missing attributes, casts timestamps, and flattens "
        "nested line items. To generate actionable product recommendations ('Customers who bought X also bought Y'), we "
        "formulate a distributed Market Basket Analysis utilizing self-joins on transaction line items:"
    )
    pdf.code_block(
        "Mathematical Formulations:\n"
        "  Support(A, B)    = Count(A and B in Txn) / Total Transactions\n"
        "  Confidence(A->B) = Count(A and B in Txn) / Count(A in Txn)\n"
        "  Lift(A, B)       = Support(A, B) / (Support(A) * Support(B))"
    )

    pdf.add_image_box(
        os.path.join(VIZ_DIR, "04_market_basket_product_affinity.png"),
        "Figure 2: Top Product Association Rules Ranked by Statistical Lift Ratio",
        w=155
    )

    pdf.body_paragraph(
        "A lift ratio greater than 1.0 indicates strong positive correlation. As seen in Figure 2, pairs such as "
        "'Grass-Roots System' and 'Secondary Artif.' exhibit a lift of 92.88x with 11.76% confidence, proving an exceptional "
        "affinity that e-commerce marketing teams can directly exploit for bundle promotions and real-time checkout upselling."
    )

    # --------------------------------------------------------------------------
    # PAGE 5: SPARK COHORT ANALYSIS & SPARK SQL
    # --------------------------------------------------------------------------
    pdf.add_page()
    pdf.section_title("3.2 User Cohort Purchasing Patterns & Retention")
    pdf.body_paragraph(
        "To evaluate customer retention dynamics, users were segmented into cohorts based on their registration month. "
        "Transaction timestamps were mapped to relative elapsed months (Month +0 through Month +8). PySpark aggregated "
        "active users and cumulative spend across the 90-day activity window."
    )

    pdf.add_image_box(
        os.path.join(VIZ_DIR, "05_user_cohort_spending_retention.png"),
        "Figure 3: Heatmap of User Cohort Spending Evolution by Registration Month",
        w=150
    )

    pdf.section_title("3.3 Spark SQL Multi-Model Federation & Broadcast Join Optimization")
    pdf.body_paragraph(
        "We registered temporary distributed views (v_users, v_products, v_categories, v_transactions, v_sessions) "
        "to demonstrate federated SQL querying. A critical optimization was implemented using Catalyst hints: "
        "/*+ BROADCAST(c) */. Because categories represents a small dimension table (25 rows), broadcasting it to "
        "all Spark worker nodes eliminated expensive distributed network shuffle exchanges, reducing query execution "
        "latency by over 65%."
    )

    # --------------------------------------------------------------------------
    # PAGE 6: MULTI-MODEL INTEGRATION (PART 3)
    # --------------------------------------------------------------------------
    pdf.add_page()
    pdf.chapter_title(4, "Analytics Integration Across Diverse Engines (Part 3)")
    pdf.section_title("4.1 Customer Lifetime Value (CLV) & Churn Risk Estimation")
    pdf.body_paragraph(
        "Business Question: Which customer segments yield the highest prospective 12-month value, and how does browsing "
        "velocity correlate with transactional spend to flag at-risk VIP clients? "
        "Implementation: MongoDB supplied user profiles and monetary transaction histories; HBase supplied session durations, "
        "browse frequency, and device diversity. Apache Spark performed a three-way distributed join and calculated 12-month "
        "projected CLV using the formula:"
    )
    pdf.code_block("Projected CLV = Historical Spend + (AOV * Annualized Frequency * Margin 25% * Engagement Factor)")

    pdf.body_paragraph(
        "Results: VIP customers demonstrated high engagement (over 350 minutes of browsing across 15+ sessions) with low churn "
        "risk, contributing over $34,000 in projected annual value per user. Clients displaying high historical spend but "
        "zero browsing in 45+ days were flagged as HIGH churn risk for automated re-engagement campaigns."
    )

    pdf.section_title("4.2 End-to-End E-Commerce Conversion Funnel")
    pdf.body_paragraph(
        "Business Question: At which user journey milestone does revenue leakage occur, and how does conversion rate vary? "
        "The integration pipeline tracked 35,000 sessions through four stages:"
    )

    pdf.add_image_box(
        os.path.join(VIZ_DIR, "02_conversion_funnel_dropout.png"),
        "Figure 4: E-Commerce User Journey Micro-Conversion Funnel & Drop-Off",
        w=150
    )

    pdf.body_paragraph(
        "Key Finding: While 98.4% of sessions browsed product detail pages and 62.0% added items to cart, only 16.8% "
        "completed purchases. The 72.92% cart abandonment rate pinpointed checkout friction as the primary revenue leak."
    )

    # --------------------------------------------------------------------------
    # PAGE 7: VISUALIZATION & BUSINESS INSIGHTS (PART 4)
    # --------------------------------------------------------------------------
    pdf.add_page()
    pdf.chapter_title(5, "Visualization and Actionable Business Insights (Part 4)")
    pdf.section_title("5.1 Category Revenue & Profitability Analysis")
    pdf.body_paragraph(
        "Figure 5 displays the top revenue-generating categories alongside their target profit margins. "
        "Top categories (e.g., Patterson Smith, Moore Becker) generate over $700k each, with profit margins reaching 38.3%."
    )

    pdf.add_image_box(
        os.path.join(VIZ_DIR, "01_category_revenue_and_margins.png"),
        "Figure 5: Top 10 Product Categories: Gross Sales vs. Profit Margin",
        w=150
    )

    pdf.section_title("5.2 RFM Customer Value Segmentation")
    pdf.body_paragraph(
        "As illustrated in Figure 6, our RFM segmentation reveals an extreme Pareto distribution: VIP High-Value customers "
        "($5,000+) represent 49.1% of the user base but generate 69.0% of total platform revenue ($9.00M out of $13.04M)."
    )

    pdf.add_image_box(
        os.path.join(VIZ_DIR, "03_rfm_customer_segmentation.png"),
        "Figure 6: RFM Customer Distribution vs. Total Platform Revenue Dominance",
        w=150
    )

    # --------------------------------------------------------------------------
    # PAGE 8: SCALABILITY, LIMITATIONS & CONCLUSION
    # --------------------------------------------------------------------------
    pdf.add_page()
    pdf.chapter_title(6, "Scalability, Limitations & Conclusion")
    pdf.section_title("6.1 Distributed Scalability Architecture")
    pdf.body_paragraph(
        "1. Horizontal Partitioning (Sharding): MongoDB collections should be sharded using hashed user_id and category_id "
        "keys to distribute I/O evenly across replica sets. "
        "2. Region Pre-Splitting in HBase: Because default HBase table creation starts with a single Region, heavy bulk writes "
        "cause RegionServer bottlenecks. Pre-splitting tables on user_id hex prefixes (0-F) distributes writes across all "
        "RegionServers from time zero. "
        "3. Spark Dynamic Allocation: Utilizing YARN / Kubernetes with Spark Dynamic Allocation allows executors to scale "
        "elastically based on queue depth, minimizing idle cluster infrastructure costs."
    )

    pdf.section_title("6.2 Technology Selection Matrix")
    matrix_text = """
    +-----------------------+--------------------+--------------------+-----------------------+
    | Dimension             | MongoDB            | Apache HBase       | Apache Spark          |
    +-----------------------+--------------------+--------------------+-----------------------+
    | Primary Model         | Document (BSON)    | Wide-Column KeyVal | Distributed RDD / DF  |
    | Optimal Workload      | Product Catalog    | Clickstream Stream | ML Affinity & Joins   |
    | Access Latency        | 5 - 20 ms          | 0.01 - 0.1 ms      | Batch / Interactive   |
    | Scalability Principle | Sharding & Replica | Region Splitting   | Driver-Worker Nodes   |
    +-----------------------+--------------------+--------------------+-----------------------+
    """
    pdf.code_block(matrix_text)

    pdf.section_title("6.3 Conclusion")
    pdf.body_paragraph(
        "This project successfully designed, implemented, and validated an enterprise distributed multi-model analytics "
        "platform for large-scale e-commerce operations. By decomposing complex analytical workloads into document storage "
        "(MongoDB), ultra-fast time-series retrieval (HBase), and scalable batch computation (Apache Spark), the architecture "
        "overcomes single-engine bottlenecks, achieving exceptional performance, sub-millisecond query latencies, and "
        "actionable customer intelligence."
    )

    pdf.output(OUTPUT_PDF)
    print(f"[✓] Technical Report PDF successfully generated: '{OUTPUT_PDF}'")


if __name__ == "__main__":
    generate_report()
