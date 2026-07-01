# Bronze Layer Ingestion
# Reads raw Olist e-commerce CSV files from Unity Catalog Volumes,
# adds ingestion metadata, and writes each dataset as a Delta table.

# Databricks notebook source
raw_path = "/Volumes/dbw_nikao_learning/ecommerce_medallion/raw_files"

display(dbutils.fs.ls(raw_path))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

catalog = "dbw_nikao_learning"
schema = "ecommerce_medallion"
raw_path = f"/Volumes/{catalog}/{schema}/raw_files"

files_to_tables = {
    "olist_customers_dataset.csv": "bronze_customers",
    "olist_orders_dataset.csv": "bronze_orders",
    "olist_order_items_dataset.csv": "bronze_order_items",
    "olist_products_dataset.csv": "bronze_products",
    "olist_order_payments_dataset.csv": "bronze_order_payments"
}

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

for file_name, table_name in files_to_tables.items():
    file_path = f"{raw_path}/{file_name}"

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit(file_name))
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{catalog}.{schema}.{table_name}")
    )

    print(f"Loaded {file_name} into {table_name}: {df.count()} rows")

# COMMAND ----------

spark.sql("""
SHOW TABLES IN dbw_nikao_learning.ecommerce_medallion
""").show(truncate=False)

# COMMAND ----------

display(spark.table("dbw_nikao_learning.ecommerce_medallion.bronze_orders").limit(10))