/**
 * schema_validation.js
 * MongoDB Schema Validation & Indexing Definitions
 * AUCA Big Data Analytics Final Project
 * Author: Asoka Niyonsaba Jean Claude (ID: 101410)
 */

// Switch to e-commerce database
use ecommerce_analytics;

// 1. Products Collection Schema
db.createCollection("products", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["product_id", "name", "category_id", "base_price", "current_stock", "is_active"],
      properties: {
        product_id: {
          bsonType: "string",
          description: "Unique string product identifier"
        },
        name: {
          bsonType: "string",
          description: "Descriptive product title"
        },
        category_id: {
          bsonType: "string",
          description: "Reference to parent category identifier"
        },
        subcategory_id: {
          bsonType: ["string", "null"],
          description: "Reference to subcategory"
        },
        category_details: {
          bsonType: "object",
          description: "Embedded category metadata for high-performance reading"
        },
        base_price: {
          bsonType: ["double", "int", "decimal"],
          minimum: 0.0,
          description: "Current base selling price"
        },
        current_stock: {
          bsonType: "int",
          minimum: 0,
          description: "Available real-time inventory count"
        },
        is_active: {
          bsonType: "bool",
          description: "Availability flag"
        },
        price_history: {
          bsonType: "array",
          items: {
            bsonType: "object",
            required: ["price", "date"],
            properties: {
              price: { bsonType: ["double", "int", "decimal"] },
              date: { bsonType: "string" }
            }
          }
        },
        creation_date: {
          bsonType: "string"
        }
      }
    }
  }
});

// Indexes for products
db.products.createIndex({ "product_id": 1 }, { unique: true });
db.products.createIndex({ "category_id": 1, "base_price": 1 });
db.products.createIndex({ "is_active": 1, "current_stock": 1 });

// 2. Users Collection Schema (with embedded summary metrics)
db.createCollection("users", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["user_id", "geo_data", "registration_date"],
      properties: {
        user_id: {
          bsonType: "string",
          description: "Unique user identifier"
        },
        geo_data: {
          bsonType: "object",
          required: ["city", "state", "country"],
          properties: {
            city: { bsonType: "string" },
            state: { bsonType: "string" },
            country: { bsonType: "string" }
          }
        },
        registration_date: { bsonType: "string" },
        last_active: { bsonType: "string" },
        profile_summary: {
          bsonType: "object",
          properties: {
            total_orders: { bsonType: "int" },
            total_spend: { bsonType: ["double", "int"] },
            avg_order_value: { bsonType: ["double", "int"] },
            preferred_payment: { bsonType: "string" }
          }
        }
      }
    }
  }
});

// Indexes for users
db.users.createIndex({ "user_id": 1 }, { unique: true });
db.users.createIndex({ "geo_data.country": 1, "geo_data.state": 1 });
db.users.createIndex({ "registration_date": 1 });

// 3. Transactions Collection Schema (with embedded line items)
db.createCollection("transactions", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["transaction_id", "user_id", "timestamp", "items", "total", "payment_method", "status"],
      properties: {
        transaction_id: { bsonType: "string" },
        session_id: { bsonType: ["string", "null"] },
        user_id: { bsonType: "string" },
        timestamp: { bsonType: "string" },
        items: {
          bsonType: "array",
          minItems: 1,
          items: {
            bsonType: "object",
            required: ["product_id", "quantity", "unit_price", "subtotal"],
            properties: {
              product_id: { bsonType: "string" },
              quantity: { bsonType: "int", minimum: 1 },
              unit_price: { bsonType: ["double", "int"] },
              subtotal: { bsonType: ["double", "int"] }
            }
          }
        },
        subtotal: { bsonType: ["double", "int"] },
        discount: { bsonType: ["double", "int"] },
        total: { bsonType: ["double", "int"] },
        payment_method: { bsonType: "string" },
        status: { bsonType: "string" }
      }
    }
  }
});

// Indexes for transactions
db.transactions.createIndex({ "transaction_id": 1 }, { unique: true });
db.transactions.createIndex({ "user_id": 1, "timestamp": -1 });
db.transactions.createIndex({ "timestamp": 1 });
db.transactions.createIndex({ "items.product_id": 1 });

