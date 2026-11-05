#!/usr/bin/env python3
"""
mongo_loader.py
MongoDB Data Modeling and Ingestion Pipeline
AUCA Big Data Analytics Final Project
Author: Asoka Niyonsaba Jean Claude (ID: 101410)

Loads, enriches, and indexes e-commerce datasets into MongoDB:
- Products catalog (denormalized category metadata for optimal read latency)
- User profiles (embedded transaction summaries for single-read customer 360)
- Transactions (embedded line item arrays with subtotal and pricing snapshots)
"""

import os
import sys
import json
import time
from collections import defaultdict
from datetime import datetime

# In-memory document database engine for robust standalone execution
class InMemoryMongoCollection:
    def __init__(self, name):
        self.name = name
        self.docs = []
        self.indexes = []

    def create_index(self, keys, unique=False):
        self.indexes.append({"keys": keys, "unique": unique})
        return f"idx_{self.name}_{len(self.indexes)}"

    def insert_many(self, documents):
        self.docs.extend(documents)
        return len(documents)

    def count_documents(self, filter_query=None):
        if not filter_query:
            return len(self.docs)
        return sum(1 for d in self.docs if all(d.get(k) == v for k, v in filter_query.items()))

    def find_one(self, filter_query=None):
        if not filter_query:
            return self.docs[0] if self.docs else None
        for d in self.docs:
            if all(d.get(k) == v for k, v in filter_query.items()):
                return d
        return None

    def find(self, filter_query=None, limit=0):
        res = []
        for d in self.docs:
            if not filter_query or all(d.get(k) == v for k, v in filter_query.items()):
                res.append(d)
                if limit and len(res) >= limit:
                    break
        return res


class InMemoryMongoDB:
    def __init__(self, db_name="ecommerce_analytics"):
        self.db_name = db_name
        self.collections = defaultdict(lambda: InMemoryMongoCollection(""))

    def __getitem__(self, name):
        if name not in self.collections:
            self.collections[name] = InMemoryMongoCollection(name)
        return self.collections[name]

    def __getattr__(self, name):
        return self[name]

    def list_collection_names(self):
        return list(self.collections.keys())


def get_mongo_connection(uri="mongodb://localhost:27017/", db_name="ecommerce_analytics"):
    """
    Attempts connection to live MongoDB daemon; falls back gracefully to
    the high-fidelity in-memory document engine.
    """
    try:
        import pymongo
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=1500)
        client.server_info()  # Test connection
        print(f"[✓] Connected to live MongoDB instance at '{uri}'")
        return client[db_name], "live"
    except Exception as e:
        print(f"[i] Live MongoDB daemon not active ({e}). Using robust In-Memory Document Store.")
        return InMemoryMongoDB(db_name), "memory"


def load_all_to_mongodb(data_dir="data", uri="mongodb://localhost:27017/"):
    start_time = time.time()
    db, mode = get_mongo_connection(uri=uri)

    print("\n" + "=" * 70)
    print("STAGE 1: MONGODB DATA MODELING & INGESTION")
    print("=" * 70)

    # 1. Load Categories Lookup
    cat_path = os.path.join(data_dir, "categories.json")
    with open(cat_path) as f:
        categories = json.load(f)
    cat_lookup = {c["category_id"]: c for c in categories}
    subcat_lookup = {}
    for c in categories:
        for sub in c.get("subcategories", []):
            subcat_lookup[sub["subcategory_id"]] = {
                "category_id": c["category_id"],
                "category_name": c["name"],
                "subcategory_name": sub["name"],
                "profit_margin": sub["profit_margin"]
            }

    # 2. Enrich & Ingest Products Catalog
    prod_path = os.path.join(data_dir, "products.json")
    with open(prod_path) as f:
        products = json.load(f)

    enriched_products = []
    for p in products:
        p_copy = dict(p)
        sub_info = subcat_lookup.get(p.get("subcategory_id"), {})
        cat_info = cat_lookup.get(p.get("category_id"), {})
        p_copy["category_details"] = {
            "category_name": cat_info.get("name", "Unknown"),
            "subcategory_name": sub_info.get("subcategory_name", "General"),
            "target_profit_margin": sub_info.get("profit_margin", 0.20)
        }
        enriched_products.append(p_copy)

    db.products.insert_many(enriched_products)
    db.products.create_index([("product_id", 1)], unique=True)
    db.products.create_index([("category_id", 1), ("base_price", 1)])
    print(f"[+] Products Ingested: {len(enriched_products):,} documents (with denormalized category details)")

    # 3. Load & Ingest Transactions
    txn_path = os.path.join(data_dir, "transactions.json")
    with open(txn_path) as f:
        transactions = json.load(f)

    db.transactions.insert_many(transactions)
    db.transactions.create_index([("transaction_id", 1)], unique=True)
    db.transactions.create_index([("user_id", 1), ("timestamp", -1)])
    db.transactions.create_index([("timestamp", 1)])
    print(f"[+] Transactions Ingested: {len(transactions):,} documents (with embedded line-item arrays)")

    # 4. Summarize & Enrich User Profiles
    user_tx_summary = defaultdict(lambda: {"order_count": 0, "total_spend": 0.0, "payment_methods": defaultdict(int), "last_txn": None})
    for t in transactions:
        u_id = t["user_id"]
        user_tx_summary[u_id]["order_count"] += 1
        user_tx_summary[u_id]["total_spend"] += t.get("total", 0.0)
        user_tx_summary[u_id]["payment_methods"][t.get("payment_method", "other")] += 1
        if not user_tx_summary[u_id]["last_txn"] or t["timestamp"] > user_tx_summary[u_id]["last_txn"]:
            user_tx_summary[u_id]["last_txn"] = t["timestamp"]

    user_path = os.path.join(data_dir, "users.json")
    with open(user_path) as f:
        users = json.load(f)

    enriched_users = []
    for u in users:
        u_copy = dict(u)
        summary = user_tx_summary[u["user_id"]]
        pref_pm = max(summary["payment_methods"].items(), key=lambda x: x[1])[0] if summary["payment_methods"] else "None"
        avg_spend = round(summary["total_spend"] / summary["order_count"], 2) if summary["order_count"] > 0 else 0.0
        u_copy["profile_summary"] = {
            "total_orders": summary["order_count"],
            "total_spend": round(summary["total_spend"], 2),
            "avg_order_value": avg_spend,
            "preferred_payment_method": pref_pm,
            "last_order_timestamp": summary["last_txn"]
        }
        enriched_users.append(u_copy)

    db.users.insert_many(enriched_users)
    db.users.create_index([("user_id", 1)], unique=True)
    db.users.create_index([("geo_data.country", 1), ("geo_data.state", 1)])
    print(f"[+] Users Ingested: {len(enriched_users):,} documents (with embedded RFM profile metrics)")

    elapsed = time.time() - start_time
    print(f"\n[✓] MongoDB Data Modeling & Ingestion Completed in {elapsed:.2f} seconds.")
    return db


if __name__ == "__main__":
    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_DIR = os.path.join(PROJECT_ROOT, "data")
    load_all_to_mongodb(data_dir=DATA_DIR)
