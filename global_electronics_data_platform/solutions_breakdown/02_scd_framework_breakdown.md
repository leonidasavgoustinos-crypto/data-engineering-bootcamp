# Slowly Changing Dimensions (SCD): Solutions & Explanations Breakdown

This document thoroughly explains how the missing Delta Lake `MERGE` statements in the SCD framework assignment were solved.

---

## 1. SCD Type 0: Retain Original (Insert-Only)

### 📌 The Challenge
In SCD Type 0, historical facts are immutable. If a record already exists, we must never update it, even if the source system sends new data. We only care about brand-new records.

### 🔍 Before (The Missing Logic)
```python
    .merge(incoming_df.alias("source"), "target.customer_id = source.customer_id") \
    .___Insert(values = { ... }) 
```

### 💡 After (The Solution)
```python
    .merge(incoming_df.alias("source"), "target.customer_id = source.customer_id") \
    .whenNotMatchedInsert(values = { ... }) 
```

### 🎓 The Educational Explanation
In Delta Lake, the `MERGE` command allows us to define what happens when records match, and when they don't. 
By **only** using `.whenNotMatchedInsert()`, we are strictly instructing the engine: "If you find a `customer_id` that does not exist in our table, insert it. If you find a match, do absolutely nothing." This perfectly implements the strict immutability rule of SCD Type 0.

---

## 2. SCD Type 1: Overwrite (No History)

### 📌 The Challenge
SCD Type 1 is used when we only care about the most up-to-date state (e.g., fixing a typo in a name). If a record arrives with a new city, we want to immediately overwrite the old city.

### 🔍 Before (The Missing Logic)
```python
    .whenMatchedUpdate(set = {
        "customer_name": "source.customer_name",
        "___": "source.city"
    })
```

### 💡 After (The Solution)
```python
    .whenMatchedUpdate(set = {
        "customer_name": "source.customer_name",
        "city": "source.city"
    })
```

### 🎓 The Educational Explanation
We map the target's `"city"` column directly to `"source.city"`. 
The `.whenMatchedUpdate()` block acts like a standard SQL `UPDATE`. It overwrites the row in place. The previous city value is permanently destroyed, and the dimension now reflects the latest reality.

---

## 3. SCD Type 3: Current & Previous (Limited History)

### 📌 The Challenge
SCD Type 3 is used when the business asks, "I want to know where the customer lives now, but also where they lived previously." We don't want multiple rows; we just want two columns: `current_city` and `previous_city`.

### 🔍 Before (The Missing Logic)
```python
    .whenMatchedUpdate(
        condition = "target.current_city <> source.city",
        set = {
            "previous_city": "___",
            "current_city": "source.city"            
        }
    )
```

### 💡 After (The Solution)
```python
    .whenMatchedUpdate(
        condition = "target.current_city <> source.city",
        set = {
            "previous_city": "target.current_city",
            "current_city": "source.city"            
        }
    )
```

### 🎓 The Educational Explanation
When an update arrives, we must perform a "swap". Before we overwrite the `current_city` with the new incoming data (`source.city`), we take the existing data (`target.current_city`) and push it down into the `previous_city` column. This preserves the last known state without growing the table vertically.

---

## 4. SCD Type 2: Full Historization (The Enterprise Standard)

### 📌 The Challenge
SCD Type 2 is the most robust, but most complex method. It keeps the entire history of a dimension by inserting a brand new row for every change. To know which row is the valid one, we use a boolean flag (`is_current`) and timestamps (`start_date`, `end_date`).

### 🔍 Before (The Missing Logic)
```python
    .whenMatchedUpdate(
        condition = "target.is_current = true AND target.city <> source.city",
        set = {
            "___": "false",
            "end_date": "source.end_date"
        }
    )
```

### 💡 After (The Solution)
```python
    .whenMatchedUpdate(
        condition = "target.is_current = true AND target.city <> source.city",
        set = {
            "is_current": "false",
            "end_date": "source.end_date"
        }
    )
```

### 🎓 The Educational Explanation
Implementing SCD Type 2 requires two actions when an existing customer changes their city:
1. **Close the Old Record:** We use `.whenMatchedUpdate` to target the active row (`is_current = true`). We flip `"is_current": "false"` and mark the `end_date` with the current timestamp. This "retires" the record.
2. **Open the New Record:** The staged updates dataframe contains a duplicate of the incoming record specifically formatted as a new insert. The `.whenNotMatchedInsert()` block picks this up and inserts a fresh row with `"is_current": "true"`.

This guarantees that a BI query filtering by `WHERE is_current = true` will always return exactly one active record per customer, while maintaining a perfect audit log of all historical addresses!
