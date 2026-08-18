# Medallion Pipeline: Solutions & Explanations Breakdown

This document provides a detailed, step-by-step educational breakdown of the Global Electronics Medallion Pipeline. It highlights what the original `TODO` blocks required, what was provided, and the complete data engineering logic behind each phase.

---

## 1. Bronze Layer: Ingestion & Storage

### 📌 The Challenge
The first task was to correctly configure the write operations for the Bronze layer. The Bronze layer serves as our raw data lake. It must be resilient, immutable, and scalable.

### 🔍 Before (Original Implementation Provided)
The initial notebook already provided the correct syntax, but left a `TODO` instruction for the user to understand it:
```python
# TODO: Complete the write configuration parameters specifying a delta layout with an append mode
customers_sandbox.write.format("delta") \
    .mode("append") \
    .saveAsTable("bronze_customers")
```

### 💡 The Educational Solution
**Why this is the correct approach:**
- `format("delta")`: We do not write raw JSON or Parquet into production systems. Delta Lake adds ACID transactions on top of Parquet files. This means if a write fails halfway through, our data lake is not corrupted.
- `mode("append")`: Bronze is an *append-only* log. We never overwrite (`mode("overwrite")`) or update records here. If customer "John" changes his name to "Jonathan", the Bronze layer simply ingests a new row for "Jonathan", preserving the historical record of "John".

---

## 2. Silver Layer: Flattening Data (Explode)

### 📌 The Challenge
Transactional data often arrives in a hierarchical format (e.g., an Order contains an array/list of Items). To build a relational Fact Table, we need to flatten this structure so every order item has its own row.

### 🔍 Before (Original Implementation Provided)
```python
# TODO: Ensure all incoming columns and the new composite PK are preserved
orders_exploded = orders_deduped.withColumn("item", F.explode("items"))
```

### 💡 The Educational Solution
**Why `F.explode()` is critical:**
If Order 201 has `[Item A, Item B]`, standard SQL joins cannot easily join an Array column to a Products table. 
The `F.explode("items")` PySpark function acts like a generator: it takes the array and duplicates the parent Order row for every item inside the array. 
- Row 1: Order 201, Item A
- Row 2: Order 201, Item B

This allows us to seamlessly join `Item A` to our Product Dimension table in the next step!

---

## 3. Silver Layer: Composite Primary Keys

### 📌 The Challenge
Once we exploded the Orders, the `order_id` was no longer unique (since an order has multiple items). We needed a new Primary Key to identify individual rows in our Fact Table.

### 🔍 Before (Original Implementation Provided)
```python
# TODO: Synthesize the final composite primary key using F.concat_ws
silver_orders_final = orders_exploded.withColumn(
    "order_item_pk", 
    F.concat_ws("-", F.col("order_id"), F.col("product_id"))
)
```

### 💡 The Educational Solution
**Why `F.concat_ws()`?**
A composite key is made by combining two or more columns to create a uniquely identifiable string. Here, combining `order_id` and `product_id` ensures every item in an order has a unique identifier.
We use `concat_ws("-", ...)` ("Concatenate With Separator") instead of standard string concatenation to prevent collisions. Without a separator, Order `12` + Product `3` would equal `123`. Order `1` + Product `23` would also equal `123`. The hyphen ensures `12-3` and `1-23` remain distinct.

---

## 4. Gold Layer: Data Governance & PII Masking

### 📌 The Challenge
The Gold layer powers dashboards and business intelligence. However, exposing full customer names violates data privacy standards (GDPR, CCPA). We needed to obscure the data while keeping it recognizable.

### 🔍 Before (Original Implementation Provided)
```sql
-- TODO: Construct string masking concatenation expression (SUBSTR)
CONCAT(SUBSTR(c.customer_name, 1, 3), '***') AS Masked_Customer_Name
```

### 💡 The Educational Solution
**How this protects data:**
Instead of exposing "Leonidas", this logic extracts the first 3 characters using `SUBSTR` ("Leo") and attaches a static `***` string using `CONCAT`, resulting in "Leo***".
This allows BI analysts to count distinct customers or verify sample data without having access to the actual Personal Identifiable Information (PII) of the user base.
