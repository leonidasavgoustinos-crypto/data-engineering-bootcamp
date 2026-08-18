# Data Vault 2.0: Solutions & Explanations Breakdown

This document thoroughly explains how the missing logic for the Data Vault 2.0 architecture assignment was resolved. Data Vault strictly separates Business Keys (Hubs), context/attributes (Satellites), and relationships (Links) for extreme scalability.

---

## 1. Hubs: Identity Generation (Hashing)

### 📌 The Challenge
A Hub must uniquely identify a business entity (like a Product) using a Surrogate Key. We had to implement a deterministic hashing algorithm on the raw `product_id`.

### 🔍 Before (The Missing Logic)
```python
hub_products_df = raw_prod_df.withColumn(
    "product_hash_key", F.___(F.upper(F.trim(F.col("___").cast("string"))), 256)
)
```

### 💡 After (The Solution)
```python
hub_products_df = raw_prod_df.withColumn(
    "product_hash_key", F.sha2(F.upper(F.trim(F.col("product_id").cast("string"))), 256)
)
```

### 🎓 The Educational Explanation
In traditional warehouses, an auto-incrementing integer (1, 2, 3...) is used as a Surrogate Key. However, this causes bottlenecks when loading massive amounts of data in parallel. 
Data Vault uses Cryptographic Hashing (specifically `SHA-256`) on the natural Business Key (the `product_id`). 
**Why?** Because hashing is deterministic. If "Product 10" is ingested simultaneously from three different source systems, all three systems independently calculate the exact same Hash Key without needing to communicate with a central sequence generator. We clean the string first (`upper`, `trim`, `cast`) to ensure identical hashes.

---

## 2. Satellites: Attaching Context

### 📌 The Challenge
A Satellite holds the descriptive data (e.g., product name, category). However, a Satellite is useless on its own; it must be permanently attached to its parent Hub via a Foreign Key.

### 🔍 Before (The Missing Logic)
```python
sat_products_df = raw_prod_df.withColumn(
    "product_hash_key", F.sha2(...)
).select(
    "___", 
    "product_name", 
    "category", 
    "price", 
    F.current_timestamp().alias("load_datetime")
)
```

### 💡 After (The Solution)
```python
sat_products_df = raw_prod_df.withColumn(
    "product_hash_key", F.sha2(...)
).select(
    "product_hash_key", 
    "product_name", 
    "category", 
    "price", 
    F.current_timestamp().alias("load_datetime")
)
```

### 🎓 The Educational Explanation
By including the `product_hash_key` in the `SELECT` statement, we embed the parent Hub's identity directly into the Satellite table. When an analyst needs to know the price of a product, they perform an `INNER JOIN` between the Hub and the Satellite using this `product_hash_key`.

---

## 3. Links: Establishing Relationships

### 📌 The Challenge
Links map the many-to-many relationships between Hubs. An Order relates to a Customer and a Product. The Link table needs its own unique Hash Key representing this specific transaction.

### 🔍 Before (The Missing Logic)
```python
link_orders_df = raw_orders_exploded.withColumn(
    "link_order_hash_key",
    F.sha2(F.concat_ws("___", 
        F.upper(F.trim(F.coalesce(F.col("order_id").cast("string"), F.lit("")))),
        F.upper(F.trim(F.coalesce(F.col("customer_id").cast("string"), F.lit("")))),
        F.upper(F.trim(F.coalesce(F.col("product_id").cast("string"), F.lit(""))))
    ), 256)
)
```

### 💡 After (The Solution)
```python
link_orders_df = raw_orders_exploded.withColumn(
    "link_order_hash_key",
    F.sha2(F.concat_ws("||", 
        F.upper(F.trim(F.coalesce(F.col("order_id").cast("string"), F.lit("")))),
        F.upper(F.trim(F.coalesce(F.col("customer_id").cast("string"), F.lit("")))),
        F.upper(F.trim(F.coalesce(F.col("product_id").cast("string"), F.lit(""))))
    ), 256)
)
```

### 🎓 The Educational Explanation
To create the Link's Hash Key, we must combine all participating Business Keys (Order + Customer + Product).
We use `concat_ws` with the `"||"` (double-pipe) delimiter. 
**Why is the delimiter critical?** Without it, combining Customer "12" and Product "3" yields "123". Combining Customer "1" and Product "23" also yields "123". This causes a "Hash Collision," meaning two different transactions would receive the identical Hash Key, corrupting the database. The delimiter forces "12||3" vs "1||23", ensuring uniqueness.

---

## 4. Satellites on Links: Transactional Attributes

### 📌 The Challenge
Sometimes the relationship itself has attributes. For example, the `order_date` or the `quantity` of an item purchased doesn't belong to the Customer Hub, nor the Product Hub. It belongs to the specific transaction (the Link). We create a Satellite that attaches to the Link.

### 🔍 Before (The Missing Logic)
```python
sat_link_orders_df = raw_orders_exploded.select(
    "___", 
    "order_date",
    "quantity",
    F.current_timestamp().alias("load_datetime")
)
```

### 💡 After (The Solution)
```python
sat_link_orders_df = raw_orders_exploded.select(
    "link_order_hash_key", 
    "order_date",
    "quantity",
    F.current_timestamp().alias("load_datetime")
)
```

### 🎓 The Educational Explanation
Just like a standard Satellite attaches to a Hub using the Hub's Hash Key, a Link Satellite attaches to a Link using the Link's Hash Key. By placing the `link_order_hash_key` inside this table, we successfully bind the `quantity` and `order_date` to the specific Order/Customer/Product relationship.
