# Global Electronics Data Platform

This repository contains the completed data engineering bootcamp assignments for the Global Electronics Data Platform. It showcases enterprise-grade data engineering patterns using **PySpark** and **Delta Lake**.

## Project Structure

- **`data/raw/`**: Contains sample JSON datasets (customers, products, orders) to simulate the operational source systems.
- **`notebooks_original/`**: The original assignment Jupyter notebooks (`.ipynb`) with the unfilled exercises.
- **`notebooks_final/`**: The completed, highly educational Databricks notebooks. These include step-by-step instructor explanations in Markdown and all TODOs resolved.
- **`solutions_breakdown/`**: Analytical Markdown documents explaining every single TODO in detail (What was missing, what was added, and the deep theory behind it).
- **`src/`**: The completed, production-ready Python scripts that solve the assignments and execute the pipelines locally.
- **`spark-warehouse/`**: (Auto-generated) Local storage directory where Delta tables are saved during script execution.

## The Pipelines (src/)

### 1. Medallion Pipeline (`01_medallion_pipeline_solved.py`)
Implements the core Bronze -> Silver -> Gold architecture:
- **Bronze:** Ingests raw JSON into append-only Delta tables.
- **Silver:** Deduplicates and cleanses data, exploding nested arrays (Order Items) to form a granular **Star Schema** (Fact & Dimension tables).
- **Gold:** Creates Business Views with Data Governance rules (e.g., PII anonymization/masking using string manipulation).

### 2. SCD Framework (`02_scd_complete_framework_solved.py`)
A deep dive into historical data tracking (Slowly Changing Dimensions) using Delta Lake `MERGE`:
- **Type 0:** Retains original values (Insert-Only).
- **Type 1:** Overwrites existing values inline.
- **Type 3:** Tracks current vs previous values in distinct columns.
- **Type 2 (Enterprise Standard):** Full historization using `is_current`, `start_date`, and `end_date` flags to version individual records.

### 3. Data Vault 2.0 (`03_data_vault_modeling_solved.py`)
An advanced modeling technique to decouple entities:
- **Hubs:** Isolate Business Keys using `SHA-256` hashing.
- **Satellites:** Hold mutable contextual attributes with load timestamps.
- **Links:** Map many-to-many relationships (e.g., Orders -> Customers -> Products) with structural hash keys.

## How to Run Locally

You must have Apache Spark and Delta Lake installed in your Python environment.
```bash
pip install pyspark delta-spark
```

Run the scripts directly from the root of the project:
```bash
python src/01_medallion_pipeline_solved.py
python src/02_scd_complete_framework_solved.py
python src/03_data_vault_modeling_solved.py
```

## Developer Notes
All missing logic (TODOs) from the original internal bootcamp have been resolved. The code utilizes PySpark DataFrame transformations (`withColumn`, `explode`, `sha2`, `concat_ws`) and Delta Lake API (`merge`, `whenMatchedUpdate`, `whenNotMatchedInsert`).
