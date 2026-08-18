import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType
from delta.tables import DeltaTable

def main():
    spark = SparkSession.builder \
        .appName("SCD Framework") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.master", "local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    
    print("=== CHAPTER 1: PIPELINE BASELINE SETUP ===")
    # Day 2 Daily Staging Batch (Customer 101 moved to Patras! Customer 103 is brand new!)
    delta_schema = StructType([
        StructField("customer_id", LongType(), True),
        StructField("customer_name", StringType(), True),
        StructField("city", StringType(), True)
    ])
    incoming_df = spark.createDataFrame([(101, "Giannis", "Patras"), (103, "Eleni", "Thessaloniki")], delta_schema)
    
    print("Incoming Delta Staging Batch (Day 2):")
    incoming_df.show()

    # Pre-populate some dummy target tables with Day 1 data to see the SCD effects
    day1_schema = delta_schema
    day1_df = spark.createDataFrame([(101, "Giannis", "Athens")], day1_schema)
    
    # Save Day 1 data into all target tables
    day1_df.write.format("delta").mode("overwrite").saveAsTable("silver_cust_type0")
    day1_df.write.format("delta").mode("overwrite").saveAsTable("silver_cust_type1")
    
    # Day 1 for Type 3
    day1_type3_df = spark.createDataFrame([(101, "Giannis", "Athens", None)], ["customer_id", "customer_name", "current_city", "previous_city"])
    day1_type3_df.write.format("delta").mode("overwrite").saveAsTable("silver_cust_type3")
    
    # Day 1 for Type 2
    day1_type2_df = spark.createDataFrame([(101, "Giannis", "Athens", True, "2026-06-24T00:00:00.000Z", None)], ["customer_id", "customer_name", "city", "is_current", "start_date", "end_date"])
    day1_type2_df.withColumn("start_date", F.col("start_date").cast("timestamp")).withColumn("end_date", F.col("end_date").cast("timestamp")).write.format("delta").mode("overwrite").saveAsTable("silver_cust_type2")

    print("\n=== CHAPTER 2: STRATEGY A — SCD TYPE 0: RETAIN ORIGINAL ===")
    # Solved TODO: Use whenNotMatchedInsert to ensure Type 0 never alters existing records
    DeltaTable.forName(spark, "silver_cust_type0").alias("target") \
        .merge(incoming_df.alias("source"), "target.customer_id = source.customer_id") \
        .whenNotMatchedInsert(values = {
            "customer_id": "source.customer_id", 
            "customer_name": "source.customer_name", 
            "city": "source.city"
        }).execute()
        
    print("SCD Type 0 Operational Execution Complete.")
    spark.table("silver_cust_type0").show()

    print("\n=== CHAPTER 3: STRATEGY B — SCD TYPE 1: OVERWRITE ===")
    # Solved TODO: Complete the Type 1 condition block to overwrite attributes ("city")
    DeltaTable.forName(spark, "silver_cust_type1").alias("target") \
        .merge(incoming_df.alias("source"), "target.customer_id = source.customer_id") \
        .whenMatchedUpdate(set = {
            "customer_name": "source.customer_name",
            "city": "source.city" 
        }) \
        .whenNotMatchedInsert(values = {
            "customer_id": "source.customer_id", 
            "customer_name": "source.customer_name", 
            "city": "source.city"
        }).execute()

    print("SCD Type 1 Operational Execution Complete.")
    spark.table("silver_cust_type1").show()

    print("\n=== CHAPTER 4: STRATEGY C — SCD TYPE 3 (CURRENT VS PREVIOUS) ===")
    # Solved TODO: Shift the target's active city down into the previous tracking field ("target.current_city")
    DeltaTable.forName(spark, "silver_cust_type3").alias("target") \
        .merge(incoming_df.alias("source"), "target.customer_id = source.customer_id") \
        .whenMatchedUpdate(
            condition = "target.current_city <> source.city",
            set = {
                "previous_city": "target.current_city", 
                "current_city": "source.city"            
            }
        ) \
        .whenNotMatchedInsert(values = {
            "customer_id": "source.customer_id",
            "customer_name": "source.customer_name",
            "current_city": "source.city",
            "previous_city": "null" 
        }).execute()

    print("SCD Type 3 Operational Execution Complete.")
    spark.table("silver_cust_type3").show()

    print("\n=== CHAPTER 5: STRATEGY D — SCD TYPE 2 (FULL ROW HISTORIZATION) ===")
    updates_with_meta = incoming_df.withColumn("is_current", F.lit(True)) \
        .withColumn("start_date", F.current_timestamp()) \
        .withColumn("end_date", F.lit(None).cast("timestamp"))

    target_df = spark.table("silver_cust_type2").filter("is_current = true")

    staged_updates = updates_with_meta.join(
        target_df, "customer_id", "inner"
    ).select(
        F.lit(None).cast("long").alias("customer_id"),
        updates_with_meta["customer_name"],
        updates_with_meta["city"],
        F.lit(False).alias("is_current"),
        target_df["start_date"],
        F.current_timestamp().alias("end_date")
    ).union(updates_with_meta)

    # Solved TODO: "is_current": "false" for closing the active record
    DeltaTable.forName(spark, "silver_cust_type2").alias("target") \
        .merge(staged_updates.alias("source"), "target.customer_id = source.customer_id") \
        .whenMatchedUpdate(
            condition = "target.is_current = true AND target.city <> source.city",
            set = {
                "is_current": "false",
                "end_date": "source.end_date"
            }
        ) \
        .whenNotMatchedInsert(values = {
            "customer_id": "source.customer_id",
            "customer_name": "source.customer_name",
            "city": "source.city",
            "is_current": "source.is_current",
            "start_date": "source.start_date",
            "end_date": "source.end_date"
        }) \
        .execute()

    print("SCD Matrix Framework Operational Evaluation Complete.")
    spark.table("silver_cust_type2").orderBy("customer_id", "start_date").show(truncate=False)

if __name__ == "__main__":
    main()
