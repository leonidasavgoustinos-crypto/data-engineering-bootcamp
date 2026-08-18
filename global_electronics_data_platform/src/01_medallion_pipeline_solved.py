import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    # Initialize local SparkSession with Delta Lake support
    spark = SparkSession.builder \
        .appName("Medallion Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.master", "local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_data_dir = os.path.join(base_dir, "data", "raw")
    
    print("=== CHAPTER 0: THE ARCHITECTURE SANDBOX ===")
    
    # 0.1 Reading Source Tables
    customers_sandbox = spark.read.json(os.path.join(raw_data_dir, "customers.json"))
    products_sandbox = spark.read.json(os.path.join(raw_data_dir, "products.json"))
    orders_sandbox = spark.read.json(os.path.join(raw_data_dir, "orders.json"))
    
    # 0.2 Unique Key Validation
    dup_customers = customers_sandbox.groupBy("customer_id").count().filter("count > 1").count()
    dup_products  = products_sandbox.groupBy("product_id").count().filter("count > 1").count()
    print(f"-> Found {dup_customers} duplicate customer IDs.")
    print(f"-> Found {dup_products} duplicate product IDs.")
    
    # Cleansing & Composite Key Construction for Orders (Sandbox Check)
    orders_base = orders_sandbox.filter(F.col("order_id").isNotNull())
    orders_deduped = orders_base.dropDuplicates(["order_id", "customer_id", "order_date"])
    orders_exploded = orders_deduped.withColumn("item", F.explode("items")) \
                                    .withColumn("product_id", F.col("item.product_id")) \
                                    .withColumn("quantity", F.col("item.quantity"))
                                    
    # Solved TODO: Synthesize the final composite primary key using F.concat_ws
    silver_orders_final = orders_exploded.withColumn(
        "order_item_pk", 
        F.concat_ws("-", F.col("order_id"), F.col("product_id"))
    ).select(
        "order_item_pk", 
        "order_id", 
        "customer_id", 
        "order_date", 
        "product_id", 
        "quantity"
    )
    print("\nFinal Silver Orders with Composite Primary Key (Sandbox):")
    silver_orders_final.show(3)

    print("\n=== CHAPTER 1: LANDING THE DATA (BRONZE LAYER) ===")
    # Solved TODO: Write with delta format and append mode
    customers_sandbox.write.format("delta").mode("append").saveAsTable("bronze_customers")
    orders_sandbox.write.format("delta").mode("append").saveAsTable("bronze_orders")
    products_sandbox.write.format("delta").mode("append").saveAsTable("bronze_products")
    print("Registered Bronze Layer Tables successfully.")

    print("\n=== CHAPTER 2: DATA QUALITY & CLEANSING (SILVER LAYER) ===")
    bronze_orders = spark.table("bronze_orders")
    duplicate_count = bronze_orders.groupBy("order_id").count().filter("count > 1").count()
    print(f"Profiling Metric - Duplicate Order IDs: {duplicate_count}")

    # Build Dimensions
    silver_customers = spark.table("bronze_customers").dropDuplicates(["customer_id"])
    silver_customers.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("silver_dim_customers")

    silver_products = spark.table("bronze_products").dropDuplicates(["product_id"])
    silver_products.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("silver_dim_products")

    # Fact Table Execution
    # Solved TODO: Complete the select and join conditions
    silver_fact_sales = spark.table("bronze_orders").filter(F.col("order_id").isNotNull()) \
        .dropDuplicates(["order_id", "customer_id", "order_date"]) \
        .withColumn("item", F.explode("items")) \
        .withColumn("product_id", F.col("item.product_id")) \
        .withColumn("quantity", F.col("item.quantity")) \
        .withColumn("order_item_pk", F.concat_ws("-", F.col("order_id"), F.col("product_id"))) \
        .join(silver_products, "product_id", "inner") \
        .select(
            "order_item_pk", 
            "order_id",
            "order_date",
            "customer_id",
            "product_id",
            "quantity"
        )
        
    silver_fact_sales.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("silver_fact_sales")
    print("Silver Layer Status: Dimensional Star Schema Deployed Successfully.")

    print("\n=== CHAPTER 3: ANALYTICS MODELING & SERVING (GOLD LAYER) ===")
    # Solved TODO: Construct string masking concatenation expression (SUBSTR)
    spark.sql("""
    CREATE OR REPLACE VIEW v_serving_sales_performance AS
    SELECT 
        f.order_id AS Order_Number,
        f.order_date AS Order_Date,
        CONCAT(SUBSTR(c.customer_name, 1, 3), '***') AS Masked_Customer_Name,
        c.city AS Delivery_City,
        p.product_name AS Product_Name,
        p.category AS Product_Category,
        f.quantity AS Units_Sold,
        (f.quantity * p.price) AS Total_Revenue
    FROM silver_fact_sales f
    INNER JOIN silver_dim_customers c ON f.customer_id = c.customer_id
    INNER JOIN silver_dim_products p ON f.product_id = p.product_id
    """)

    print("Serving Status: Business View Endpoint Online (PII Masking Rule Enforcement Active).")
    spark.sql("SELECT * FROM v_serving_sales_performance LIMIT 5").show(truncate=False)
    
if __name__ == "__main__":
    main()
