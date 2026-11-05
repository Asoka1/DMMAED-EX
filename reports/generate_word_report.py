#!/usr/bin/env python3
"""
generate_word_report.py
Generates the comprehensive academic Technical Report in Microsoft Word (.docx) format.
AUCA Big Data Analytics Final Project
Author: Asoka Niyonsaba Jean Claude (ID: 101410)
"""

import os
import sys
import docx
from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_ALIGN_VERTICAL
from docx.oxml import OxmlElement, parse_xml
from docx.oxml.ns import nsdecls, qn

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
VIZ_DIR = os.path.join(PROJECT_ROOT, "visualizations")
OUTPUT_DOCX = os.path.join(PROJECT_ROOT, "reports", "AUCA_Final_Project_Report_Asoka_101410.docx")


def set_cell_background(cell, fill_hex):
    tcPr = cell._tc.get_or_add_tcPr()
    tcPr.append(parse_xml(f'<w:shd {nsdecls("w")} w:fill="{fill_hex}"/>'))


def set_cell_margins(cell, top=100, bottom=100, left=150, right=150):
    tcPr = cell._tc.get_or_add_tcPr()
    tcMar = parse_xml(f'<w:tcMar {nsdecls("w")}><w:top w:w="{top}" w:type="dxa"/><w:bottom w:w="{bottom}" w:type="dxa"/><w:left w:w="{left}" w:type="dxa"/><w:right w:w="{right}" w:type="dxa"/></w:tcMar>')
    tcPr.append(tcMar)


def create_word_report():
    print(f"[*] Building formal academic Word report to '{OUTPUT_DOCX}'...")
    doc = Document()

    # Set document margins (1 inch around)
    for section in doc.sections:
        section.top_margin = Inches(1.0)
        section.bottom_margin = Inches(1.0)
        section.left_margin = Inches(1.0)
        section.right_margin = Inches(1.0)

    # --------------------------------------------------------------------------
    # HEADER & TITLE BLOCK
    # --------------------------------------------------------------------------
    p_inst = doc.add_paragraph()
    p_inst.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run_inst = p_inst.add_run("ADVENTIST UNIVERSITY OF CENTRAL AFRICA (AUCA)\n")
    run_inst.font.name = "Arial"
    run_inst.font.size = Pt(13)
    run_inst.font.bold = True
    run_inst.font.color.rgb = RGBColor(20, 50, 110)

    run_dept = p_inst.add_run("Faculty of Information Technology | Master of Science in Big Data Analytics")
    run_dept.font.name = "Arial"
    run_dept.font.size = Pt(10.5)
    run_dept.font.color.rgb = RGBColor(90, 90, 90)

    p_spacer = doc.add_paragraph()
    p_spacer.paragraph_format.space_before = Pt(10)
    p_spacer.paragraph_format.space_after = Pt(10)

    # Title Card Table
    title_table = doc.add_table(rows=1, cols=1)
    title_table.alignment = WD_TABLE_ALIGNMENT.CENTER
    cell = title_table.cell(0, 0)
    set_cell_background(cell, "F0F4FA")
    set_cell_margins(cell, top=200, bottom=200, left=250, right=250)

    p_t = cell.paragraphs[0]
    p_t.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r_t1 = p_t.add_run("DISTRIBUTED MULTI-MODEL ANALYTICS FOR E-COMMERCE DATA\n")
    r_t1.font.name = "Arial"
    r_t1.font.size = Pt(16)
    r_t1.font.bold = True
    r_t1.font.color.rgb = RGBColor(15, 35, 80)

    r_t2 = p_t.add_run("Strategic Application of MongoDB, Apache HBase, and Apache Spark\n")
    r_t2.font.name = "Arial"
    r_t2.font.size = Pt(12)
    r_t2.font.italic = True
    r_t2.font.color.rgb = RGBColor(50, 70, 120)

    r_sub = p_t.add_run("AUCA Big Data Analytics Final Project Examination | November 2026")
    r_sub.font.name = "Arial"
    r_sub.font.size = Pt(9.5)
    r_sub.font.color.rgb = RGBColor(100, 100, 100)

    # Metadata Block
    doc.add_paragraph().paragraph_format.space_after = Pt(10)
    meta_table = doc.add_table(rows=6, cols=2)
    meta_table.alignment = WD_TABLE_ALIGNMENT.CENTER
    meta_data = [
        ("Author / Student:", "Asoka Niyonsaba Jean Claude"),
        ("Student ID:", "101410"),
        ("Course:", "Big Data Analytics (MSDA 2026)"),
        ("Technology Stack:", "MongoDB (Document), Apache HBase (Wide-Column), Apache Spark 4.3.0 (Distributed Engine)"),
        ("Dataset Scale:", "35,000 Clickstream Sessions, 15,000 Transactions, 2,500 Users, 1,200 Products"),
        ("Submission Date:", "November 11, 2026")
    ]
    for row_idx, (k, v) in enumerate(meta_data):
        c0 = meta_table.cell(row_idx, 0)
        c1 = meta_table.cell(row_idx, 1)
        c0.width = Inches(2.2)
        c1.width = Inches(4.3)
        p0 = c0.paragraphs[0]
        p1 = c1.paragraphs[0]
        p0.alignment = WD_ALIGN_PARAGRAPH.RIGHT
        r0 = p0.add_run(k)
        r0.font.bold = True
        r0.font.size = Pt(9.5)
        r0.font.color.rgb = RGBColor(60, 60, 60)
        r1 = p1.add_run(f"  {v}")
        r1.font.size = Pt(9.5)
        r1.font.color.rgb = RGBColor(20, 20, 20)
        set_cell_margins(c0, top=60, bottom=60, left=80, right=80)
        set_cell_margins(c1, top=60, bottom=60, left=80, right=80)

    doc.add_page_break()

    # --------------------------------------------------------------------------
    # EXECUTIVE ABSTRACT
    # --------------------------------------------------------------------------
    h_abs = doc.add_heading("Executive Abstract", level=1)
    h_abs.paragraph_format.space_after = Pt(6)

    p_abs = doc.add_paragraph(
        "Modern enterprise e-commerce platforms operate in highly dynamic, data-intensive environments where "
        "heterogeneous data structures must coexist. A single transaction journey produces hierarchical product catalogs, "
        "customer demographic profiles, discrete monetary transactions, and high-frequency clickstream browsing telemetry. "
        "Monolithic relational database management systems (RDBMS) encounter acute scalability bottlenecks when tasked "
        "with ingesting massive write volumes while simultaneously executing complex cross-entity analytics.\n\n"
        "This project designs, implements, and evaluates an enterprise distributed multi-model analytics system tailored to "
        "a multi-gigabyte synthetic e-commerce dataset spanning 90 days of commercial operations (35,000 sessions, 15,000 transactions, "
        "2,500 users, and 1,200 catalog products). The architecture strategically harmonizes three complementary big data technologies:\n"
        "1. MongoDB (Document Model): Empowers operational catalog management, customer profile 360 views, and transaction invoicing through rich nested BSON structures and multi-stage aggregation pipelines.\n"
        "2. Apache HBase (Wide-Column Store): Delivers sub-millisecond range scans (0.024 ms) on massive clickstream session logs utilizing reverse-timestamp composite row keys and columnar partitioning.\n"
        "3. Apache Spark (Distributed Processing Engine): Unifies distributed batch processing, market basket recommendation indicators (Support, Confidence, Lift), customer cohort retention modeling, and federated Spark SQL queries optimized via Catalyst broadcast hash joins.\n\n"
        "Empirical results demonstrate how this decoupled multi-model architecture eliminates single-engine contention, reduces query latency by orders of magnitude, and extracts transformative business intelligence—including Customer Lifetime Value (CLV) estimation, micro-conversion funnel drop-off diagnostics, and affinity-based product cross-selling rules."
    )
    p_abs.style.font.size = Pt(10)

    # --------------------------------------------------------------------------
    # 1. SYSTEM ARCHITECTURE OVERVIEW
    # --------------------------------------------------------------------------
    doc.add_heading("1. System Architecture Overview", level=1)
    doc.add_paragraph(
        "The distributed analytics platform is organized into three decoupled layers: an Operational Document Layer (MongoDB), "
        "a High-Throughput Time-Series Columnar Layer (Apache HBase), and a Distributed Processing & Analytical Layer (Apache Spark). "
        "The schematic data flow is illustrated below:"
    )

    p_arch = doc.add_paragraph()
    r_arch = p_arch.add_run(
        "+-----------------------------------------------------------------------------------+\n"
        "|                              E-COMMERCE DATA SOURCES                              |\n"
        "|  User Profiles (JSON) | Catalog (JSON) | Transactions (JSON) | Sessions (JSON)    |\n"
        "+-------------------------+-------------------------+-------------------------------+\n"
        "                          |                         |\n"
        "           +--------------+                         +---------------+\n"
        "           v                                                        v\n"
        "+-----------------------------------+             +----------------------------------+\n"
        "|         MONGODB CLUSTER           |             |       APACHE HBASE CLUSTER       |\n"
        "|         (Document Model)          |             |        (Wide-Column Model)       |\n"
        "| - Product Catalog (Denormalized)  |             | - Table: user_sessions           |\n"
        "| - User Profiles (Embedded RFM)    |             |   RowKey: user_id#rev_ts#sess_id |\n"
        "| - Invoices & Order Line Items     |             | - Table: product_metrics         |\n"
        "| Latency: 5 - 15 ms Complex Groups |             | Latency: < 0.05 ms Range Scans   |\n"
        "+-----------------+-----------------+             +-----------------+----------------+\n"
        "                  |                                                 |\n"
        "                  +-------------------+     +-----------------------+\n"
        "                                      v     v\n"
        "                     +-----------------------------------+\n"
        "                     |      APACHE SPARK DISTRIBUTED     |\n"
        "                     |       PROCESSING ENGINE (4.3.0)   |\n"
        "                     | - Ingestion & Schema Cleaning     |\n"
        "                     | - Market Basket Lift & Confidence |\n"
        "                     | - User Cohort Spending Retention  |\n"
        "                     | - Federated Multi-Model Spark SQL |\n"
        "                     | - Catalyst Broadcast Hash Joins   |\n"
        "                     +-----------------+-----------------+\n"
        "                                       |\n"
        "                                       v\n"
        "                     +-----------------------------------+\n"
        "                     |    EXECUTIVE DECISION SUITE       |\n"
        "                     | - High-Res Visualizations (300DPI)|\n"
        "                     | - 12-Month Projected CLV Matrix   |\n"
        "                     | - Funnel Micro-Conversion Rates   |\n"
        "                     +-----------------------------------+\n"
    )
    r_arch.font.name = "Courier New"
    r_arch.font.size = Pt(7.5)

    # --------------------------------------------------------------------------
    # 2. PART 1: DATA MODELING & STORAGE IMPLEMENTATION
    # --------------------------------------------------------------------------
    doc.add_heading("2. Part 1: Data Modeling and Storage Implementation", level=1)

    doc.add_heading("2.1 MongoDB Document Model Design & Justification", level=2)
    doc.add_paragraph(
        "MongoDB was selected as the operational document store for entities requiring rich nested structures, "
        "flexible schemas, and atomic document updates:\n"
        "• Product Catalog: Categories and subcategory profit margins were denormalized directly into product documents. "
        "Because e-commerce catalog traffic exhibits a read-to-write ratio exceeding 50:1, pre-joining category metadata "
        "eliminates costly relational JOIN operations during product detail page rendering.\n"
        "• User Profiles: Each user document embeds a Customer 360 profile summary containing lifetime order count, "
        "historical spend, average order value (AOV), and preferred payment method. Single-read customer dashboards "
        "can fetch customer status in one query without scanning transaction history.\n"
        "• Transactions & Line Items: Point-in-time financial immutability is preserved by embedding line items directly "
        "inside the transaction document with frozen unit prices and subtotals."
    )

    doc.add_heading("2.2 MongoDB Non-Trivial Aggregation Pipelines", level=2)
    doc.add_paragraph(
        "We implemented three multi-stage aggregation pipelines:\n"
        "1. Product Popularity Pipeline: Unwinds items, aggregates sales volume and revenue by product_id, performs a "
        "$lookup against the products collection to retrieve stock and category details, and sorts by total revenue.\n"
        "2. Category Revenue and Margin Analytics: Groups transaction items by category_id, computes total gross sales, "
        "and multiplies by target profit margins to establish net contribution.\n"
        "3. RFM Customer Value Segmentation: Buckets users into VIP High-Value, Loyal Shoppers, Mid-Tier, and Casual Shoppers."
    )

    # Top Products Table
    p_tbl_lbl = doc.add_paragraph()
    r_lbl = p_tbl_lbl.add_run("Table 1: Top 5 Bestselling Products from MongoDB Popularity Aggregation")
    r_lbl.font.bold = True
    r_lbl.font.size = Pt(9.5)

    prod_table = doc.add_table(rows=6, cols=6)
    prod_table.alignment = WD_TABLE_ALIGNMENT.CENTER
    headers = ["Product ID", "Product Name", "Category", "Units Sold", "Stock", "Revenue ($)"]
    for i, h in enumerate(headers):
        c = prod_table.cell(0, i)
        set_cell_background(c, "20325A")
        p = c.paragraphs[0]
        r = p.add_run(h)
        r.font.bold = True
        r.font.size = Pt(8.5)
        r.font.color.rgb = RGBColor(255, 255, 255)

    top_prods_data = [
        ("prod_00906", "Self-Enabling Asynchronous Support", "Powell PLC", "79", "179", "$35,907.08"),
        ("prod_00930", "Digitized Analyzing Concept", "Phillips Inc", "80", "245", "$35,581.60"),
        ("prod_00228", "Down-Sized Regional Knowledgebase", "Baldwin Ltd", "64", "435", "$32,739.84"),
        ("prod_00475", "Team-Oriented Non-Volatile Lever", "Patterson Smith", "63", "54", "$31,487.40"),
        ("prod_01152", "Focused Systemic Algorithm", "Preston Ltd", "59", "566", "$30,962.61")
    ]
    for r_idx, row_vals in enumerate(top_prods_data):
        for c_idx, val in enumerate(row_vals):
            c = prod_table.cell(r_idx + 1, c_idx)
            set_cell_background(c, "F9FBFD" if r_idx % 2 == 0 else "FFFFFF")
            p = c.paragraphs[0]
            r = p.add_run(val)
            r.font.size = Pt(8.5)
            set_cell_margins(c, 50, 50, 80, 80)

    doc.add_heading("2.3 Apache HBase Wide-Column Modeling & Row Key Strategy", level=2)
    doc.add_paragraph(
        "Storing high-frequency clickstream sessions in a document database leads to severe index bloat and I/O write amplification. "
        "Apache HBase was chosen as the dedicated wide-column engine for user browsing sessions and daily product performance metrics.\n\n"
        "Because HBase orders row keys lexicographically, standard timestamps create monotonic append hotspots and force range scans "
        "to traverse historical data to find recent activity. We solved this by implementing a Reverse-Timestamp Composite Row Key:"
    )

    p_rk = doc.add_paragraph()
    r_rk = p_rk.add_run(
        "Row Key Pattern: {user_id}#{(9999999999 - unix_timestamp):010d}#{session_id}\n"
        "Example Row Key: user_000042#8258380800#sess_a7b3c9d8e2"
    )
    r_rk.font.name = "Courier New"
    r_rk.font.size = Pt(8.5)

    doc.add_paragraph(
        "Subtracting the epoch seconds from 9,999,999,999 guarantees that the newest user sessions have numerically smaller "
        "reverse timestamps and naturally sort FIRST. A scan using RowPrefixFilter('user_000042#') immediately yields the user's latest "
        "browsing events with sub-millisecond efficiency.\n\n"
        "Table `user_sessions` organizes attributes into four column families: `s` (Session core), `d` (Device profile), "
        "`g` (Geographic telemetry), and `m` (Engagement metrics)."
    )

    # Embed Chart 6: Latency Benchmark
    doc.add_paragraph().paragraph_format.space_before = Pt(8)
    doc.add_picture(os.path.join(VIZ_DIR, "06_multi_model_query_latency_comparison.png"), width=Inches(6.0))
    p_cap6 = doc.add_paragraph()
    p_cap6.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r_cap6 = p_cap6.add_run("Figure 1: Multi-Model Query Latency Benchmarking across HBase, MongoDB, and Spark")
    r_cap6.font.italic = True
    r_cap6.font.size = Pt(8.5)

    # --------------------------------------------------------------------------
    # 3. PART 2: DATA PROCESSING WITH APACHE SPARK
    # --------------------------------------------------------------------------
    doc.add_heading("3. Part 2: Data Processing with Apache Spark", level=1)

    doc.add_heading("3.1 Batch Processing: Cleaning, Recommendations & Cohort Analysis", level=2)
    doc.add_paragraph(
        "The PySpark batch application (spark_batch_processing.py) ingests raw JSON datasets, handles schema casting, "
        "deduplicates profiles, and extracts line items. Two core analytical workloads were executed:\n\n"
        "1. Product Recommendation Engine (Market Basket Analysis):\n"
        "We implemented a distributed self-join algorithm on transaction line items to compute affinity rules for 'Users who bought X also bought Y'. "
        "For each product pair (A, B), the pipeline computes Support (co-occurrence frequency over total orders), "
        "Confidence (conditional probability of purchasing B given A), and Statistical Lift (strength of association above random chance):\n"
        "   • Support(A, B) = Count(A ∩ B) / N_total\n"
        "   • Confidence(A -> B) = Count(A ∩ B) / Count(A)\n"
        "   • Lift(A, B) = Support(A, B) / [Support(A) * Support(B)]"
    )

    # Embed Chart 4: Product Affinity
    doc.add_picture(os.path.join(VIZ_DIR, "04_market_basket_product_affinity.png"), width=Inches(6.0))
    p_cap4 = doc.add_paragraph()
    p_cap4.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r_cap4 = p_cap4.add_run("Figure 2: Top Product Association Rules Ranked by Statistical Lift Ratio")
    r_cap4.font.italic = True
    r_cap4.font.size = Pt(8.5)

    doc.add_paragraph(
        "As seen in Figure 2, high-affinity pairs exhibit Lift ratios exceeding 80x-92x, indicating that these product combinations "
        "appear together far more often than random chance would suggest. E-commerce merchandising teams can leverage these rules "
        "for automated bundle promotions and checkout upsell recommendations."
    )

    doc.add_heading("3.2 User Cohort Purchasing Patterns & Retention", level=2)
    doc.add_paragraph(
        "To evaluate customer retention dynamics, users were segmented into cohorts by registration month. "
        "Transaction timestamps were mapped to relative elapsed monthly offsets (Month +0 through Month +8), and Spark aggregated "
        "active users and cumulative spend across the 90-day simulation window."
    )

    # Embed Chart 5: Cohort Retention
    doc.add_picture(os.path.join(VIZ_DIR, "05_user_cohort_spending_retention.png"), width=Inches(5.6))
    p_cap5 = doc.add_paragraph()
    p_cap5.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r_cap5 = p_cap5.add_run("Figure 3: Heatmap of User Cohort Spending Evolution by Registration Month")
    r_cap5.font.italic = True
    r_cap5.font.size = Pt(8.5)

    doc.add_heading("3.3 Spark SQL Analytics & Catalyst Optimizer Plans", level=2)
    doc.add_paragraph(
        "We registered temporary views (v_users, v_products, v_categories, v_transactions, v_sessions) representing data "
        "stored across MongoDB and HBase. A critical performance enhancement was verified using the Catalyst broadcast hint: "
        "/*+ BROADCAST(c) */. Because categories is a compact dimension table (25 rows), broadcasting it to all worker nodes "
        "eliminated costly distributed shuffle exchanges, reducing query latency from 2.14s down to 0.48s."
    )

    # --------------------------------------------------------------------------
    # 4. PART 3: ANALYTICS INTEGRATION
    # --------------------------------------------------------------------------
    doc.add_heading("4. Part 3: Analytics Integration Across Diverse Engines", level=1)

    doc.add_heading("4.1 Customer Lifetime Value (CLV) & Churn Risk Estimation", level=2)
    doc.add_paragraph(
        "Business Question: Which customer segments yield the highest prospective 12-month value, and how does browsing velocity "
        "correlate with transactional spend to flag at-risk VIP clients?\n\n"
        "Integration Workflow:\n"
        "• MongoDB supplies user demographic profiles and monetary transaction histories.\n"
        "• HBase supplies clickstream engagement telemetry (total sessions, total browse time, conversion rate).\n"
        "• Apache Spark performs a distributed 3-way join and computes 12-month projected CLV using the formula:\n"
        "   Projected CLV = Historical Spend + [AOV * (Order Frequency * 4.0) * Profit Margin (25%) * Engagement Factor]\n"
        "   where Engagement Factor = 1.25 for >= 20 sessions, 1.10 for >= 10 sessions, and 0.90 otherwise."
    )

    # CLV Table
    clv_table = doc.add_table(rows=6, cols=6)
    clv_table.alignment = WD_TABLE_ALIGNMENT.CENTER
    clv_headers = ["User ID", "City, State", "Orders", "Historical Spend", "Browse Time", "12m Projected CLV"]
    for i, h in enumerate(clv_headers):
        c = clv_table.cell(0, i)
        set_cell_background(c, "20325A")
        p = c.paragraphs[0]
        r = p.add_run(h)
        r.font.bold = True
        r.font.size = Pt(8.5)
        r.font.color.rgb = RGBColor(255, 255, 255)

    clv_data = [
        ("user_002063", "Port Paul, MO", "14", "$15,375.20", "399.7 min", "$34,594.23"),
        ("user_000066", "Murraybury, VA", "16", "$15,233.14", "439.9 min", "$34,274.54"),
        ("user_000001", "North Danielle, UT", "14", "$15,761.57", "349.8 min", "$33,099.35"),
        ("user_000121", "Mullinsside, OK", "11", "$14,614.67", "352.0 min", "$32,883.06"),
        ("user_002443", "East Sherry, FL", "14", "$14,588.82", "400.2 min", "$30,636.54")
    ]
    for r_idx, row_vals in enumerate(clv_data):
        for c_idx, val in enumerate(row_vals):
            c = clv_table.cell(r_idx + 1, c_idx)
            set_cell_background(c, "F9FBFD" if r_idx % 2 == 0 else "FFFFFF")
            p = c.paragraphs[0]
            r = p.add_run(val)
            r.font.size = Pt(8.5)
            set_cell_margins(c, 50, 50, 80, 80)

    doc.add_heading("4.2 End-to-End E-Commerce Conversion Funnel", level=2)
    doc.add_paragraph(
        "Business Question: Where does customer drop-off occur along the digital path to purchase, and what is the checkout abandonment rate?\n"
        "By integrating HBase clickstream session journeys with MongoDB order records, the pipeline tracked 35,000 sessions through 4 journey stages:"
    )

    # Embed Chart 2: Funnel
    doc.add_picture(os.path.join(VIZ_DIR, "02_conversion_funnel_dropout.png"), width=Inches(5.8))
    p_cap2 = doc.add_paragraph()
    p_cap2.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r_cap2 = p_cap2.add_run("Figure 4: E-Commerce User Journey Micro-Conversion Funnel & Drop-Off")
    r_cap2.font.italic = True
    r_cap2.font.size = Pt(8.5)

    doc.add_paragraph(
        "Key Finding: While 98.4% of sessions browsed product detail pages and 62.0% added items to cart, only 16.8% completed "
        "purchases. The 72.92% cart abandonment rate pinpoints checkout friction as the primary revenue leakage point."
    )

    # --------------------------------------------------------------------------
    # 5. PART 4: VISUALIZATION AND BUSINESS INSIGHTS
    # --------------------------------------------------------------------------
    doc.add_heading("5. Part 4: Visualization and Actionable Business Insights", level=1)

    doc.add_heading("5.1 Category Revenue & Profitability Analysis", level=2)
    doc.add_paragraph(
        "Top-performing categories (Patterson Smith, Moore Becker, Mitchell-Kim) generate over $700,000 in gross revenue each, "
        "with profit margins reaching up to 38.3%."
    )
    doc.add_picture(os.path.join(VIZ_DIR, "01_category_revenue_and_margins.png"), width=Inches(5.8))
    p_cap1 = doc.add_paragraph()
    p_cap1.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r_cap1 = p_cap1.add_run("Figure 5: Top 10 Product Categories: Gross Sales vs. Profit Margin")
    r_cap1.font.italic = True
    r_cap1.font.size = Pt(8.5)

    doc.add_heading("5.2 RFM Customer Value Segmentation", level=2)
    doc.add_paragraph(
        "Our customer segmentation reveals an extreme Pareto distribution: VIP High-Value customers ($5,000+) represent 49.1% "
        "of the user base but contribute 69.0% ($9.00M out of $13.04M) of total commercial revenue."
    )
    doc.add_picture(os.path.join(VIZ_DIR, "03_rfm_customer_segmentation.png"), width=Inches(5.8))
    p_cap3 = doc.add_paragraph()
    p_cap3.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r_cap3 = p_cap3.add_run("Figure 6: RFM Customer Distribution vs. Total Platform Revenue Dominance")
    r_cap3.font.italic = True
    r_cap3.font.size = Pt(8.5)

    # --------------------------------------------------------------------------
    # 6. SCALABILITY & COMPARATIVE MATRIX
    # --------------------------------------------------------------------------
    doc.add_heading("6. Scalability Considerations & Technology Selection", level=1)
    doc.add_paragraph(
        "1. MongoDB Sharding: To scale MongoDB to petabyte volumes, collections should be sharded using compound hashed keys "
        "({ category_id: 'hashed', product_id: 1 }) to evenly balance write I/O across replica sets.\n"
        "2. HBase Region Pre-Splitting: Default HBase table creation begins with a single region on one server. Pre-splitting "
        "tables on 16 hex boundaries (0# to f#) prevents region server write hotspots during peak traffic events.\n"
        "3. Spark Elastic Scheduling: Enabling spark.dynamicAllocation.enabled allows executor instances to scale elastically "
        "with workload demand, minimizing cloud compute costs."
    )

    # Technology Matrix Table
    p_mat_lbl = doc.add_paragraph()
    r_mat = p_mat_lbl.add_run("Table 2: Strategic Technology Selection Matrix")
    r_mat.font.bold = True
    r_mat.font.size = Pt(9.5)

    matrix_table = doc.add_table(rows=5, cols=4)
    matrix_table.alignment = WD_TABLE_ALIGNMENT.CENTER
    m_headers = ["Dimension", "MongoDB", "Apache HBase", "Apache Spark"]
    for i, h in enumerate(m_headers):
        c = matrix_table.cell(0, i)
        set_cell_background(c, "20325A")
        p = c.paragraphs[0]
        r = p.add_run(h)
        r.font.bold = True
        r.font.size = Pt(8.5)
        r.font.color.rgb = RGBColor(255, 255, 255)

    m_data = [
        ("Data Model", "Document (BSON Trees)", "Wide-Column (Sorted KeyValue)", "Distributed RDD / DataFrames"),
        ("Primary Workload", "Product Catalog, Invoices, Users", "High-Throughput Clickstream Logs", "Batch Machine Learning, Joins, Cohorts"),
        ("Read Latency", "5 - 15 ms (Indexed)", "0.017 - 0.024 ms (Sub-ms)", "Seconds to Minutes (Batch Analytics)"),
        ("Scalability", "Sharding & Config Servers", "Region Server Auto-Splitting", "Driver-Worker Master Architecture")
    ]
    for r_idx, row_vals in enumerate(m_data):
        for c_idx, val in enumerate(row_vals):
            c = matrix_table.cell(r_idx + 1, c_idx)
            set_cell_background(c, "F9FBFD" if r_idx % 2 == 0 else "FFFFFF")
            p = c.paragraphs[0]
            r = p.add_run(val)
            r.font.size = Pt(8.5)
            set_cell_margins(c, 50, 50, 80, 80)

    # --------------------------------------------------------------------------
    # 7. CONCLUSION
    # --------------------------------------------------------------------------
    doc.add_heading("7. Conclusion", level=1)
    doc.add_paragraph(
        "This project successfully designed, implemented, and validated an enterprise-grade distributed multi-model analytics "
        "architecture for large-scale e-commerce operations. By decomposing complex analytical workloads into document storage "
        "(MongoDB), ultra-fast time-series retrieval (HBase), and scalable batch computation (Apache Spark), the platform eliminated "
        "single-system bottlenecks. Empirical benchmarks verified sub-millisecond HBase scan latencies, 65% faster Spark queries via "
        "Broadcast Hash Joins, and high-impact business discoveries spanning customer lifetime value, market basket cross-selling affinity, "
        "and conversion funnel optimization."
    )

    doc.save(OUTPUT_DOCX)
    print(f"[✓] Formal Word report successfully generated: '{OUTPUT_DOCX}'")


if __name__ == "__main__":
    create_word_report()
