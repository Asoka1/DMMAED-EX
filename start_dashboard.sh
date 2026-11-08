#!/usr/bin/env bash
# ==============================================================================
# AUCA Big Data Analytics - Web Dashboard Launcher
# Student: Asoka Niyonsaba Jean Claude (Student ID: 101410)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

if [ -d "venv" ]; then
    source venv/bin/activate
fi

export PORT="${PORT:-8080}"
echo "================================================================================"
echo " Starting AUCA Big Data Analytics Interactive Web Dashboard on http://127.0.0.1:${PORT}"
echo "================================================================================"

python3 app.py
