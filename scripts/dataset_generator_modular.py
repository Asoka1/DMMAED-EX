#!/usr/bin/env python3
"""
dataset_generator_modular.py
Modular, highly-performant e-commerce dataset generator for AUCA Big Data Analytics Final Project.
Faithfully implements all schema attributes, probability distributions, page flow Markov chains,
and inventory dynamics specified in the AUCA Project Specification.
"""

import os
import sys
import json
import random
import datetime
import uuid
import threading
import argparse
import numpy as np
from faker import Faker

fake = Faker()

def json_serializer(obj):
    """Serialize datetime and date objects to ISO string."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


class InventoryManager:
    def __init__(self, products):
        self.products = {p["product_id"]: p for p in products}
        self.lock = threading.RLock()

    def update_stock(self, product_id, quantity):
        with self.lock:
            if product_id not in self.products:
                return False
            if self.products[product_id]["current_stock"] >= quantity:
                self.products[product_id]["current_stock"] -= quantity
                if self.products[product_id]["current_stock"] == 0:
                    self.products[product_id]["is_active"] = False
                return True
            return False

    def get_product(self, product_id):
        with self.lock:
            return self.products.get(product_id)


def determine_page_type(position, previous_pages):
    """Determine page type based on user journey position and previous transitions."""
    if position == 0:
        return random.choice(["home", "search", "category_listing"])
    if not previous_pages:
        return "home"
    prev_page = previous_pages[-1]["page_type"]
    if prev_page == "home":
        return random.choices(["category_listing", "search", "product_detail"], weights=[0.5, 0.3, 0.2])[0]
    elif prev_page == "category_listing":
        return random.choices(["product_detail", "category_listing", "search", "home"], weights=[0.7, 0.1, 0.1, 0.1])[0]
    elif prev_page == "search":
        return random.choices(["product_detail", "search", "category_listing", "home"], weights=[0.6, 0.2, 0.1, 0.1])[0]
    elif prev_page == "product_detail":
        return random.choices(["product_detail", "cart", "category_listing", "search", "home"], weights=[0.3, 0.3, 0.2, 0.1, 0.1])[0]
    elif prev_page == "cart":
        return random.choices(["checkout", "product_detail", "category_listing", "home"], weights=[0.6, 0.2, 0.1, 0.1])[0]
    elif prev_page == "checkout":
        return random.choices(["confirmation", "cart", "home"], weights=[0.8, 0.1, 0.1])[0]
    elif prev_page == "confirmation":
        return random.choices(["home", "product_detail", "category_listing"], weights=[0.6, 0.2, 0.2])[0]
    return "home"


def get_page_content(page_type, products_list, categories_list, inventory):
    if page_type == "product_detail":
        attempts = 0
        while attempts < 10:
            product = random.choice(products_list)
            if product["is_active"] and product["current_stock"] > 0:
                category_id = product["category_id"]
                category = next((c for c in categories_list if c["category_id"] == category_id), None)
                return product, category
            attempts += 1
        product = random.choice(products_list)
        category_id = product["category_id"]
        category = next((c for c in categories_list if c["category_id"] == category_id), None)
        return product, category
    elif page_type == "category_listing":
        return None, random.choice(categories_list)
    return None, None


def generate_dataset(num_users=2000, num_products=1000, num_categories=20, 
                     num_transactions=15000, num_sessions=50000, timespan_days=90, 
                     output_dir="data", seed=42):
    np.random.seed(seed)
    random.seed(seed)
    Faker.seed(seed)
    os.makedirs(output_dir, exist_ok=True)

    print(f"=== Starting E-Commerce Dataset Generation ===")
    print(f"Target Configuration: Users={num_users:,}, Products={num_products:,}, Categories={num_categories:,}, "
          f"Sessions={num_sessions:,}, Transactions={num_transactions:,}, OutputDir='{output_dir}'")

    # 1. Categories
    categories = []
    for cat_id in range(num_categories):
        category = {
            "category_id": f"cat_{cat_id:03d}",
            "name": fake.company(),
            "subcategories": []
        }
        for sub_id in range(random.randint(3, 5)):
            subcategory = {
                "subcategory_id": f"sub_{cat_id:03d}_{sub_id:02d}",
                "name": fake.bs(),
                "profit_margin": round(random.uniform(0.12, 0.42), 2)
            }
            category["subcategories"].append(subcategory)
        categories.append(category)
    print(f"[+] Generated {len(categories):,} categories with subcategories.")

    # 2. Products
    products = []
    product_creation_start = datetime.datetime.now() - datetime.timedelta(days=timespan_days * 2)
    for prod_id in range(num_products):
        category = random.choice(categories)
        chosen_sub = random.choice(category["subcategories"])
        base_price = round(random.uniform(9.99, 499.99), 2)
        price_history = []
        initial_date = fake.date_time_between(
            start_date=product_creation_start,
            end_date=product_creation_start + datetime.timedelta(days=timespan_days // 3)
        )
        price_history.append({"price": base_price, "date": initial_date.isoformat()})
        for _ in range(random.randint(0, 2)):
            price_change_date = fake.date_time_between(start_date=initial_date, end_date="now")
            new_price = round(base_price * random.uniform(0.85, 1.15), 2)
            price_history.append({"price": new_price, "date": price_change_date.isoformat()})
            initial_date = price_change_date
        price_history.sort(key=lambda x: x["date"])
        current_price = price_history[-1]["price"]

        products.append({
            "product_id": f"prod_{prod_id:05d}",
            "name": fake.catch_phrase().title(),
            "category_id": category["category_id"],
            "subcategory_id": chosen_sub["subcategory_id"],
            "base_price": current_price,
            "current_stock": random.randint(15, 850),
            "is_active": random.choices([True, False], weights=[0.95, 0.05])[0],
            "price_history": price_history,
            "creation_date": price_history[0]["date"]
        })
    print(f"[+] Generated {len(products):,} products.")

    # 3. Users
    users = []
    for user_id in range(num_users):
        reg_date = fake.date_time_between(start_date=f"-{timespan_days * 3}d", end_date=f"-{timespan_days}d")
        users.append({
            "user_id": f"user_{user_id:06d}",
            "geo_data": {
                "city": fake.city(),
                "state": fake.state_abbr(),
                "country": fake.country_code()
            },
            "registration_date": reg_date.isoformat(),
            "last_active": fake.date_time_between(start_date=reg_date, end_date="now").isoformat()
        })
    print(f"[+] Generated {len(users):,} user profiles.")

    # 4. Sessions and Transactions
    inventory = InventoryManager(products)
    sessions = []
    transactions = []
    session_counter = 0
    transaction_counter = 0
    iteration = 0
    max_iterations = (num_sessions + num_transactions) * 3

    print("[*] Simulating realistic user browsing sessions and purchasing funnels...")
    while (session_counter < num_sessions or transaction_counter < num_transactions) and iteration < max_iterations:
        iteration += 1

        if session_counter < num_sessions:
            user = random.choice(users)
            session_id = f"sess_{uuid.uuid4().hex[:10]}"
            session_start = fake.date_time_between(start_date=f"-{timespan_days}d", end_date="now")
            session_duration = random.randint(30, 2400)

            page_views = []
            viewed_products = set()
            cart_contents = {}

            time_slots = sorted([0] + [random.randint(1, session_duration - 1) for _ in range(random.randint(3, 12))] + [session_duration])

            for i in range(len(time_slots) - 1):
                view_duration = time_slots[i + 1] - time_slots[i]
                page_type = determine_page_type(i, page_views)
                product, category = get_page_content(page_type, products, categories, inventory)

                if page_type == "product_detail" and product:
                    p_id = product["product_id"]
                    viewed_products.add(p_id)
                    if random.random() < 0.32:
                        if p_id not in cart_contents:
                            cart_contents[p_id] = {"quantity": 0, "price": product["base_price"]}
                        max_possible = min(3, inventory.get_product(p_id)["current_stock"] - cart_contents[p_id]["quantity"])
                        if max_possible > 0:
                            add_qty = random.randint(1, max_possible)
                            cart_contents[p_id]["quantity"] += add_qty

                page_views.append({
                    "timestamp": (session_start + datetime.timedelta(seconds=time_slots[i])).isoformat(),
                    "page_type": page_type,
                    "product_id": product["product_id"] if product else None,
                    "category_id": category["category_id"] if category else None,
                    "view_duration": view_duration
                })

            converted = False
            has_checkout_intent = cart_contents and any(p["page_type"] in ["checkout", "confirmation"] for p in page_views)
            if has_checkout_intent:
                converted = random.random() < 0.72

            session_geo = user["geo_data"].copy()
            session_geo["ip_address"] = fake.ipv4()

            sessions.append({
                "session_id": session_id,
                "user_id": user["user_id"],
                "start_time": session_start.isoformat(),
                "end_time": (session_start + datetime.timedelta(seconds=session_duration)).isoformat(),
                "duration_seconds": session_duration,
                "geo_data": session_geo,
                "device_profile": {
                    "type": random.choice(["mobile", "desktop", "tablet"]),
                    "os": random.choice(["iOS", "Android", "Windows", "macOS"]),
                    "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"])
                },
                "viewed_products": list(viewed_products),
                "page_views": page_views,
                "cart_contents": {k: v for k, v in cart_contents.items() if v["quantity"] > 0},
                "conversion_status": "converted" if converted else ("abandoned" if cart_contents else "browsed"),
                "referrer": random.choice(["direct", "email", "social", "search_engine", "affiliate"])
            })
            session_counter += 1

            if converted and transaction_counter < num_transactions:
                txn_items = []
                valid = True
                for prod_id, details in cart_contents.items():
                    qty = details["quantity"]
                    if qty > 0:
                        if inventory.update_stock(prod_id, qty):
                            txn_items.append({
                                "product_id": prod_id,
                                "quantity": qty,
                                "unit_price": details["price"],
                                "subtotal": round(qty * details["price"], 2)
                            })
                        else:
                            valid = False
                            break
                if valid and txn_items:
                    subtotal = round(sum(item["subtotal"] for item in txn_items), 2)
                    discount = 0.0
                    if random.random() < 0.25:
                        discount = round(subtotal * random.choice([0.05, 0.1, 0.15, 0.2]), 2)
                    total = round(subtotal - discount, 2)
                    transactions.append({
                        "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
                        "session_id": session_id,
                        "user_id": user["user_id"],
                        "timestamp": (session_start + datetime.timedelta(seconds=session_duration)).isoformat(),
                        "items": txn_items,
                        "subtotal": subtotal,
                        "discount": discount,
                        "total": total,
                        "payment_method": random.choice(["credit_card", "paypal", "apple_pay", "crypto"]),
                        "status": "completed"
                    })
                    transaction_counter += 1

        # Additional transactions to represent direct purchases / recurring orders
        if transaction_counter < num_transactions and random.random() < 0.25:
            user = random.choice(users)
            sampled_prods = random.sample(products, k=min(random.randint(1, 3), len(products)))
            txn_items = []
            for sprod in sampled_prods:
                if sprod["is_active"]:
                    qty = random.randint(1, 3)
                    if inventory.update_stock(sprod["product_id"], qty):
                        txn_items.append({
                            "product_id": sprod["product_id"],
                            "quantity": qty,
                            "unit_price": sprod["base_price"],
                            "subtotal": round(qty * sprod["base_price"], 2)
                        })
            if txn_items:
                subtotal = round(sum(item["subtotal"] for item in txn_items), 2)
                discount = round(subtotal * random.choice([0.05, 0.1, 0.15]), 2) if random.random() < 0.2 else 0.0
                total = round(subtotal - discount, 2)
                transactions.append({
                    "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
                    "session_id": None,
                    "user_id": user["user_id"],
                    "timestamp": fake.date_time_between(start_date=f"-{timespan_days}d", end_date="now").isoformat(),
                    "items": txn_items,
                    "subtotal": subtotal,
                    "discount": discount,
                    "total": total,
                    "payment_method": random.choice(["credit_card", "paypal", "bank_transfer", "gift_card"]),
                    "status": random.choice(["completed", "shipped", "delivered", "processing"])
                })
                transaction_counter += 1

        if iteration % 5000 == 0 or session_counter == num_sessions:
            print(f"[*] Progress: {session_counter:,}/{num_sessions:,} sessions | {transaction_counter:,}/{num_transactions:,} txns (iter {iteration:,})")

    # Export datasets
    print("[*] Writing JSON datasets to disk...")
    with open(os.path.join(output_dir, "users.json"), "w") as f:
        json.dump(users, f, default=json_serializer, indent=2)
    with open(os.path.join(output_dir, "products.json"), "w") as f:
        json.dump(list(inventory.products.values()), f, default=json_serializer, indent=2)
    with open(os.path.join(output_dir, "categories.json"), "w") as f:
        json.dump(categories, f, default=json_serializer, indent=2)
    with open(os.path.join(output_dir, "transactions.json"), "w") as f:
        json.dump(transactions, f, default=json_serializer, indent=2)

    chunk_size = 25000
    for idx, i in enumerate(range(0, len(sessions), chunk_size)):
        chunk = sessions[i:i + chunk_size]
        with open(os.path.join(output_dir, f"sessions_{idx}.json"), "w") as f:
            json.dump(chunk, f, default=json_serializer)

    print(f"\n[v] Dataset generation finished successfully in '{output_dir}':")
    print(f"    - Users: {len(users):,}")
    print(f"    - Products: {len(products):,}")
    print(f"    - Categories: {len(categories):,}")
    print(f"    - Sessions: {len(sessions):,}")
    print(f"    - Transactions: {len(transactions):,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AUCA E-commerce Big Data Generator")
    parser.add_argument("--users", type=int, default=2000, help="Number of users")
    parser.add_argument("--products", type=int, default=1000, help="Number of products")
    parser.add_argument("--categories", type=int, default=25, help="Number of categories")
    parser.add_argument("--sessions", type=int, default=30000, help="Number of sessions")
    parser.add_argument("--transactions", type=int, default=12000, help="Number of transactions")
    parser.add_argument("--timespan-days", type=int, default=90, help="Activity span in days")
    parser.add_argument("--output-dir", type=str, default="data", help="Output directory")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()

    generate_dataset(
        num_users=args.users,
        num_products=args.products,
        num_categories=args.categories,
        num_sessions=args.sessions,
        num_transactions=args.transactions,
        timespan_days=args.timespan_days,
        output_dir=args.output_dir,
        seed=args.seed
    )

