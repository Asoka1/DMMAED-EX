#!/usr/bin/env bash
# ==============================================================================
# run_all.sh
# Master Execution Script for AUCA Big Data Analytics Final Project
# Author: Asoka Niyonsaba Jean Claude (ID: 101410)
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================================================"
echo "    AUCA BIG DATA ANALYTICS FINAL PROJECT: MULTI-MODEL E-COMMERCE       "
echo "    Author: Asoka Niyonsaba Jean Claude | Student ID: 101410            "
echo "========================================================================"

# Locate Python environment
if [ -d "venv" ]; then
    PYTHON_CMD="./venv/bin/python3"
else
    PYTHON_CMD="python3"
fi

export SPARK_LOCAL_DIRS=/tmp
export PYSPARK_PYTHON="$PYTHON_CMD"
export PYSPARK_DRIVER_PYTHON="$PYTHON_CMD"
unset HADOOP_CONF_DIR HADOOP_HOME

echo "[*] Using Python: $($PYTHON_CMD --version)"

# 1. Dataset verification or generation
if [ ! -f "data/transactions.json" ]; then
    echo "[*] Stage 1: Generating representative dataset..."
    $PYTHON_CMD scripts/dataset_generator_modular.py --users 2500 --products 1200 --categories 25 --sessions 35000 --transactions 15000
fi

# 2. Part 1: MongoDB
echo "[*] Stage 2: Running MongoDB Data Modeling & Aggregation Pipelines..."
$PYTHON_CMD mongodb/mongo_loader.py
$PYTHON_CMD mongodb/mongo_aggregations.py

# 3. Part 1: HBase
echo "[*] Stage 3: Running HBase Wide-Column Ingestion & Time-Series Range Queries..."
$PYTHON_CMD hbase/hbase_queries.py

# 4. Part 2: Spark Batch Processing
echo "[*] Stage 4: Running Apache Spark Batch Processing (Recommendations & Cohorts)..."
$PYTHON_CMD spark/spark_batch_processing.py

# 5. Part 2: Spark SQL Federated Analytics
echo "[*] Stage 5: Running Spark SQL Analytics & Catalyst Optimization..."
$PYTHON_CMD spark/spark_sql_analytics.py

# 6. Part 3: Analytics Integration
echo "[*] Stage 6: Running Multi-Model Integration (CLV & Funnel)..."
$PYTHON_CMD integration/integrated_analytics.py

# 7. Part 4: Visualizations
echo "[*] Stage 7: Generating 300-DPI Publication Visualizations..."
$PYTHON_CMD visualizations/generate_visualizations.py

# 8. Report Compilation
echo "[*] Stage 8A: Compiling Academic Technical Report (PDF)..."
$PYTHON_CMD reports/generate_pdf_report.py

echo "[*] Stage 8B: Compiling Academic Technical Report (Word .docx)..."
$PYTHON_CMD reports/generate_word_report.py

echo "========================================================================"
echo "    🎉 ALL STAGES COMPLETED SUCCESSFULLY WITH 100% REPRODUCIBILITY!     "
echo "========================================================================"

