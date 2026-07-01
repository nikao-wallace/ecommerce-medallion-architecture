# Databricks notebook source
# 02_bronze_to_silver

catalog = "dbw_nikao_learning"
schema = "ecommerce_medallion"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

bronze_customers = spark.table("bronze_customers")
bronze_orders = spark.table("bronze_orders")

display(bronze_customers.limit(10))
display(bronze_orders.limit(10))

bronze_customers.printSchema()
bronze_orders.printSchema()

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct

bronze_orders.agg(
    count("*").alias("total_rows"),
    countDistinct("order_id").alias("distinct_order_ids")
).display()

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct

bronze_customers.agg(
    count("*").alias("total_rows"),
    countDistinct("customer_id").alias("distinct_customer_ids"),
    countDistinct("customer_unique_id").alias("distinct_customer_unique_ids")
).display()

# COMMAND ----------

silver_customers = bronze_customers.dropDuplicates(["customer_id"])

silver_orders = bronze_orders.dropDuplicates(["order_id"])

# COMMAND ----------

silver_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_customers")

silver_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_orders")

# COMMAND ----------

spark.table("silver_customers").count(), spark.table("silver_orders").count()

# COMMAND ----------

# display the bronze order_items, products, and order_payments tables limit 10 on each
display(spark.table("bronze_order_items").limit(10))
display(spark.table("bronze_products").limit(10))
display(spark.table("bronze_order_payments").limit(10))


# COMMAND ----------

bronze_order_payments = spark.table("bronze_order_payments")
bronze_order_items = spark.table("bronze_order_items")
bronze_products = spark.table("bronze_products")
bronze_orders = spark.table("bronze_orders")

# COMMAND ----------

bronze_order_payments.agg(
    count("*").alias("total_rows"),
    countDistinct("order_id").alias("distinct_order_ids"),
).display()

# COMMAND ----------

#display the bronze_order_payments table

display(spark.table("bronze_order_payments"))

# COMMAND ----------

# DBTITLE 1,Check duplicates in bronze_order_payments
# Check for duplicates based on (order_id, payment_sequential) composite key
from pyspark.sql.functions import count

duplicates = (
    bronze_order_payments
    .groupBy("order_id", "payment_sequential")
    .agg(count("*").alias("occurrences"))
    .filter("occurrences > 1")
)

print(f"Duplicate composite key combinations: {duplicates.count()}")
display(duplicates)

# COMMAND ----------

item_duplicates = (
    bronze_order_items
    .groupBy("order_id", "order_item_id")
    .agg(count("*").alias("occurrences"))
    .filter("occurrences > 1")
)

print(f"Duplicate composite key combinations: {item_duplicates.count()}")
display(item_duplicates)

# COMMAND ----------

product_duplicates = (
    bronze_products
    .groupBy("product_id")
    .agg(count("*").alias("occurrences"))
    .filter("occurrences > 1")
)

print(f"Duplicate product_id values: {product_duplicates.count()}")
display(product_duplicates)

# COMMAND ----------

silver_products = bronze_products.dropDuplicates(["product_id"])

silver_order_items = bronze_order_items.dropDuplicates([
    "order_id", 
    "order_item_id"
])

silver_order_payments = bronze_order_payments.dropDuplicates([
    "order_id", 
    "payment_sequential"
])

# COMMAND ----------

silver_products.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_products")

silver_order_items.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_order_items")

silver_order_payments.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_order_payments")

# COMMAND ----------

for table in [
    "silver_customers",
    "silver_orders",
    "silver_products",
    "silver_order_items",
    "silver_order_payments"
]:
    print(table, spark.table(table).count())

# COMMAND ----------

orders_without_customers = (
    spark.table("silver_orders").alias("o")
    .join(
        spark.table("silver_customers").alias("c"),
        on="customer_id",
        how="left_anti"
    )
)

print("Orders without matching customer:", orders_without_customers.count())
display(orders_without_customers)

# COMMAND ----------

items_without_orders = (
    spark.table("silver_order_items").alias("i")
    .join(
        spark.table("silver_orders").alias("o"),
        on="order_id",
        how="left_anti"
    )
)

print("Order items without matching order:", items_without_orders.count())
display(items_without_orders)

# COMMAND ----------

payments_without_orders = (
    spark.table("silver_order_payments").alias("p")
    .join(
        spark.table("silver_orders").alias("o"),
        on="order_id",
        how="left_anti"
    )
)

print("Payments without matching order:", payments_without_orders.count())
display(payments_without_orders)

# COMMAND ----------

items_without_products = (
    spark.table("silver_order_items").alias("i")
    .join(
        spark.table("silver_products").alias("p"),
        on="product_id",
        how="left_anti"
    )
)

print("Order items without matching product:", items_without_products.count())
display(items_without_products)

# COMMAND ----------

from pyspark.sql.functions import col

invalid_order_dates = (
    spark.table("silver_orders")
    .filter(
        (col("order_approved_at").isNotNull()) &
        (col("order_purchase_timestamp").isNotNull()) &
        (col("order_approved_at") < col("order_purchase_timestamp"))
    )
)

print("Orders approved before purchase:", invalid_order_dates.count())
display(invalid_order_dates)

# COMMAND ----------

invalid_delivery_dates = (
    spark.table("silver_orders")
    .filter(
        (col("order_delivered_customer_date").isNotNull()) &
        (col("order_purchase_timestamp").isNotNull()) &
        (col("order_delivered_customer_date") < col("order_purchase_timestamp"))
    )
)

print("Orders delivered before purchase:", invalid_delivery_dates.count())
display(invalid_delivery_dates)


# COMMAND ----------
# Enrich silver_orders with date fields and delivery performance metrics

from pyspark.sql.functions import col, to_date, trunc, year, date_format, datediff, when, lit

silver_orders_enriched = (
    spark.table("silver_orders")
    .withColumn("purchase_date", to_date(col("order_purchase_timestamp")))
    .withColumn("purchase_month", trunc(col("order_purchase_timestamp"), "month"))
    .withColumn("purchase_year", year(col("order_purchase_timestamp")))
    .withColumn("purchase_month_name", date_format(col("order_purchase_timestamp"), "MMMM"))
    .withColumn(
        "days_to_deliver",
        when(
            col("order_delivered_customer_date").isNotNull(),
            datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))
        ).otherwise(lit(None))
    )
    .withColumn(
        "is_late_delivery",
        when(col("order_delivered_customer_date").isNull(), lit(None))
        .when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"), lit(True))
        .otherwise(lit(False))
    )
)

# COMMAND ----------

silver_orders_enriched.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_orders")

# COMMAND ----------

display(spark.table("silver_orders").select(
    "order_id",
    "order_status",
    "purchase_date",
    "purchase_month",
    "purchase_year",
    "purchase_month_name",
    "days_to_deliver",
    "is_late_delivery"
).limit(20))