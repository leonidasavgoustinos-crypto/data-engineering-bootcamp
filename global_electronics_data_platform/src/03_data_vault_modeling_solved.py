import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder \
        .appName("Data Vault Modeling") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.master", "local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    
    # We will read the raw datasets again to feed the Data Vault initialization
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_data_dir = os.path.join(base_dir, "data", "raw")
    
    raw_cust_df   = spark.read.json(os.path.join(raw_data_dir, "customers.json"))
    raw_orders_df = spark.read.json(os.path.join(raw_data_dir, "orders.json"))
    raw_prod_df   = spark.read.json(os.path.join(raw_data_dir, "products.json"))
    
    print("=== CHAPTER 2: SCHEMA PROCESSING PIPELINE EXECUTION ===")
    
    # 2.2 Deploying Business Hubs (Identity Tables)
    
    # 1. CUSTOMER HUB
    hub_customers_df = raw_cust_df.withColumn(
        "customer_hash_key", F.sha2(F.upper(F.trim(F.col("customer_id").cast("string"))), 256)
    ).select(
        "customer_hash_key", 
        "customer_id", 
        F.current_timestamp().alias("load_datetime"), 
        F.lit("CRM").alias("record_source")
    ).distinct()

    hub_customers_df.write.format("delta").mode("overwrite").saveAsTable("hub_customer")
    print("-> Hub Customer:")
    spark.table("hub_customer").show()

    # 2. PRODUCT HUB 
    # Solved TODO: Use the SHA-256 function ("sha2") to hash the unique product business natural key ("product_id")
    hub_products_df = raw_prod_df.withColumn(
        "product_hash_key", F.sha2(F.upper(F.trim(F.col("product_id").cast("string"))), 256)
    ).select(
        "product_hash_key", 
        "product_id", 
        F.current_timestamp().alias("load_datetime"), 
        F.lit("ERP").alias("record_source")
    ).distinct()

    hub_products_df.write.format("delta").mode("overwrite").saveAsTable("hub_product")
    print("-> Hub Product:")
    spark.table("hub_product").show()

    # 2.3 Deploying Descriptive Context Satellites
    
    # PRODUCT SATELLITE 
    # Solved TODO: Match the correct parent hash key column name ("product_hash_key")
    sat_products_df = raw_prod_df.withColumn(
        "product_hash_key", F.sha2(F.upper(F.trim(F.col("product_id").cast("string"))), 256)
    ).select(
        "product_hash_key", 
        "product_name", 
        "category", 
        "price", 
        F.current_timestamp().alias("load_datetime"), 
        F.lit("ERP").alias("record_source")
    )

    sat_products_df.write.format("delta").mode("overwrite").saveAsTable("sat_product_details")
    
    # CUSTOMER SATELLITE 
    # Solved TODO: Match the correct parent hash key column name ("customer_hash_key")
    sat_customer_df = raw_cust_df.withColumn(
        "customer_hash_key", F.sha2(F.upper(F.trim(F.col("customer_id").cast("string"))), 256)
    ).select(
        "customer_hash_key", 
        "customer_name", 
        "city", 
        F.current_timestamp().alias("load_datetime"), 
        F.lit("ERP").alias("record_source")
    )

    sat_customer_df.write.format("delta").mode("overwrite").saveAsTable("sat_customer_details")
    
    # 2.4 Deploying Structural Transaction Links
    
    # To successfully map relationships, we flatten nested transactional items first
    raw_orders_exploded = raw_orders_df.filter(F.col("order_id").isNotNull()).withColumn("item", F.explode("items")) \
        .withColumn("product_id", F.col("item.product_id")) \
        .withColumn("quantity", F.col("item.quantity"))

    # ORDER LINK 
    # Solved TODO: Complete the concatenation delimiter token ("||")
    link_orders_df = raw_orders_exploded.withColumn(
        "link_order_hash_key",
        F.sha2(F.concat_ws("||", 
            F.upper(F.trim(F.coalesce(F.col("order_id").cast("string"), F.lit("")))),
            F.upper(F.trim(F.coalesce(F.col("customer_id").cast("string"), F.lit("")))),
            F.upper(F.trim(F.coalesce(F.col("product_id").cast("string"), F.lit(""))))
        ), 256)
    ).withColumn(
        "customer_hash_key", F.sha2(F.upper(F.trim(F.col("customer_id").cast("string"))), 256)
    ).withColumn(
        "product_hash_key", F.sha2(F.upper(F.trim(F.col("product_id").cast("string"))), 256)
    ).select(
        "link_order_hash_key", 
        "order_id", 
        "customer_hash_key", 
        "product_hash_key", 
        F.current_timestamp().alias("load_datetime"), 
        F.lit("WEB_SHOP").alias("record_source")
    ).distinct()

    link_orders_df.write.format("delta").mode("overwrite").saveAsTable("link_order")
    print("-> Link Order:")
    spark.table("link_order").show()

    # 2.5 Deploying a Satellite on top of a Link
    
    # SATELLITE ON LINK
    # Solved TODO: Compute the matching composite relation hash key ("link_order_hash_key")
    sat_link_orders_df = raw_orders_exploded.withColumn(
        "link_order_hash_key",
        F.sha2(F.concat_ws("||", 
            F.upper(F.trim(F.coalesce(F.col("order_id").cast("string"), F.lit("")))),
            F.upper(F.trim(F.coalesce(F.col("customer_id").cast("string"), F.lit("")))),
            F.upper(F.trim(F.coalesce(F.col("product_id").cast("string"), F.lit(""))))
        ), 256)
    ).select(
        "link_order_hash_key", 
        "order_date",
        "quantity",
        F.current_timestamp().alias("load_datetime"),
        F.lit("WEB_SHOP").alias("record_source")
    )

    sat_link_orders_df.write.format("delta").mode("overwrite").saveAsTable("sat_link_order_details")
    print("-> Sat Link Order Details:")
    spark.table("sat_link_order_details").show()

    print("Data Vault Architecture Status: Enterprise Layout Fully Sampled and Generated.")
    
if __name__ == "__main__":
    main()
