#!/usr/bin/env bash
# ==============================================================================
# AUCA Big Data Analytics - Dockerized HBase Management Script
# Student: Asoka Niyonsaba Jean Claude (Student ID: 101410)
# ==============================================================================
set -euo pipefail

CONTAINER_NAME="auca_hbase_cluster"

echo "================================================================================"
echo " AUCA HBASE DOCKER MANAGEMENT HELPER"
echo "================================================================================"

case "${1:-status}" in
  start)
    echo "[INFO] Starting Dockerized HBase instance..."
    docker compose up -d hbase
    echo "[INFO] Waiting for HBase Thrift server (port 9090) to be healthy..."
    sleep 10
    docker ps --filter "name=${CONTAINER_NAME}"
    ;;
  
  init-schema)
    echo "[INFO] Applying HBase DDL schema from hbase/hbase_schema.txt..."
    if docker ps --format '{{.Names}}' | grep -q "${CONTAINER_NAME}"; then
      docker exec -i "${CONTAINER_NAME}" hbase shell < ./hbase/hbase_schema.txt
      echo "[SUCCESS] Schema initialized successfully in Dockerized HBase!"
    else
      echo "[ERROR] Container ${CONTAINER_NAME} is not running. Run: ./hbase/hbase_docker_helper.sh start"
      exit 1
    fi
    ;;

  shell)
    echo "[INFO] Opening interactive HBase shell inside container..."
    docker exec -it "${CONTAINER_NAME}" hbase shell
    ;;

  status)
    echo "[INFO] Checking Dockerized HBase status..."
    docker ps --filter "name=${CONTAINER_NAME}"
    ;;

  stop)
    echo "[INFO] Stopping Dockerized HBase instance..."
    docker compose stop hbase
    ;;

  *)
    echo "Usage: $0 {start|init-schema|shell|status|stop}"
    exit 1
    ;;
esac
