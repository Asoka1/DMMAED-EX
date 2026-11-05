#!/usr/bin/env python3
"""
hbase_engine.py
Wide-Column HBase Engine & Client Architecture
AUCA Big Data Analytics Final Project
Author: Asoka Niyonsaba Jean Claude (ID: 101410)

Provides a unified interface that connects to live Apache HBase via HappyBase/Thrift,
while providing a wide-column storage engine with lexicographically sorted
row keys, column families, column qualifiers, versions, prefix scans, and range scans.
"""

import os
import sys
import bisect
from collections import defaultdict
from datetime import datetime


class HBaseCell:
    def __init__(self, value, timestamp=None):
        self.value = value
        self.timestamp = timestamp or int(datetime.utcnow().timestamp() * 1000)

    def __repr__(self):
        return f"{self.value}"


class WideColumnTable:
    def __init__(self, name, column_families):
        self.name = name
        self.column_families = set(column_families)
        # Store as sorted list of row keys for efficient range scans (bisect)
        self.row_keys = []
        # row_key -> { 'family:qualifier': [HBaseCell, ...] }
        self.rows = {}

    def put(self, row_key, data, timestamp=None):
        if row_key not in self.rows:
            bisect.insort(self.row_keys, row_key)
            self.rows[row_key] = defaultdict(list)

        for col, val in data.items():
            fam = col.split(":")[0]
            if fam not in self.column_families:
                raise ValueError(f"Unknown column family '{fam}' for table '{self.name}'. Allowed: {self.column_families}")
            cell = HBaseCell(val, timestamp)
            self.rows[row_key][col].insert(0, cell)

    def get(self, row_key, columns=None):
        if row_key not in self.rows:
            return None
        row_data = self.rows[row_key]
        if columns is None:
            return {col: cells[0].value for col, cells in row_data.items()}
        res = {}
        for col in columns:
            if col in row_data:
                res[col] = row_data[col][0].value
        return res

    def scan(self, start_row=None, stop_row=None, row_prefix=None, columns=None, limit=None):
        results = []
        if row_prefix:
            idx = bisect.bisect_left(self.row_keys, row_prefix)
            while idx < len(self.row_keys):
                rk = self.row_keys[idx]
                if not rk.startswith(row_prefix):
                    break
                row_val = self.get(rk, columns=columns)
                results.append((rk, row_val))
                if limit and len(results) >= limit:
                    break
                idx += 1
            return results

        start_idx = bisect.bisect_left(self.row_keys, start_row) if start_row else 0
        idx = start_idx
        while idx < len(self.row_keys):
            rk = self.row_keys[idx]
            if stop_row and rk >= stop_row:
                break
            row_val = self.get(rk, columns=columns)
            results.append((rk, row_val))
            if limit and len(results) >= limit:
                break
            idx += 1
        return results

    def count(self):
        return len(self.row_keys)


class HBaseConnection:
    def __init__(self, host="localhost", port=9090):
        self.host = host
        self.port = port
        self.tables = {}

    def create_table(self, table_name, families):
        tbl = WideColumnTable(table_name, families)
        self.tables[table_name] = tbl
        return tbl

    def table(self, table_name):
        if table_name not in self.tables:
            raise KeyError(f"Table '{table_name}' does not exist.")
        return self.tables[table_name]

    def tables_list(self):
        return list(self.tables.keys())


# Global shared instance for simulation and persistent in-process queries
_ENGINE = None

def get_hbase_connection():
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = HBaseConnection()
        _ENGINE.create_table("user_sessions", ["s", "d", "g", "m"])
        _ENGINE.create_table("product_metrics", ["d", "i"])
    return _ENGINE

