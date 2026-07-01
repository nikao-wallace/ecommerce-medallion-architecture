# Synthetic Marketing Data
# Generates campaign and customer acquisition tables to simulate
# realistic multi-channel marketing attribution for downstream gold models.

# Databricks notebook source
catalog = "dbw_nikao_learning"
schema = "ecommerce_medallion"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

customer_metrics = spark.table(
    "dbw_nikao_learning.ecommerce_medallion.gold_customer_metrics"
)

display(customer_metrics.limit(10))
customer_metrics.printSchema()

# COMMAND ----------

# Verifying unique customer ids equal to total number of rows

from pyspark.sql.functions import count, countDistinct

customer_metrics.agg(
    count("*").alias("total_rows"),
    countDistinct("customer_unique_id").alias("distinct_customers")
).display()

# COMMAND ----------

from pyspark.sql import Row
from datetime import date

campaign_data = [
    # Organic Search
    Row("CAMPAIGN_001", "Organic SEO - Product Pages", "Organic Search", "SEO", date(2016, 8, 1), date(2018, 9, 30), 0.00, "Product researchers", 4, 1, 7, 1.20, 1.10),
    Row("CAMPAIGN_002", "Organic SEO - Buying Guides", "Organic Search", "SEO", date(2016, 8, 1), date(2018, 9, 30), 0.00, "Research-driven shoppers", 4, 2, 10, 1.15, 1.08),
    Row("CAMPAIGN_003", "Organic SEO - Brand Content", "Organic Search", "SEO", date(2016, 8, 1), date(2018, 9, 30), 0.00, "Brand-aware researchers", 3, 1, 8, 1.05, 1.00),

    # Direct
    Row("CAMPAIGN_004", "Direct - Brand Traffic", "Direct", "Brand", date(2016, 8, 1), date(2018, 9, 30), 0.00, "Brand-aware shoppers", 4, 0, 2, 1.25, 1.12),

    # Paid Search
    Row("CAMPAIGN_005", "Paid Search - Brand Terms", "Paid Search", "Search Ads", date(2016, 9, 1), date(2018, 8, 31), 30000.00, "High-intent brand searchers", 5, 0, 2, 1.40, 1.20),
    Row("CAMPAIGN_006", "Paid Search - Shopping Ads", "Paid Search", "Shopping Ads", date(2016, 9, 1), date(2018, 8, 31), 40000.00, "Product-ready shoppers", 4, 1, 5, 1.15, 1.05),
    Row("CAMPAIGN_007", "Paid Search - Category Keywords", "Paid Search", "Search Ads", date(2016, 9, 1), date(2018, 8, 31), 25000.00, "Category researchers", 3, 2, 8, 0.95, 0.95),
    Row("CAMPAIGN_008", "Paid Search - Competitor Terms", "Paid Search", "Search Ads", date(2016, 9, 1), date(2018, 8, 31), 20000.00, "Comparison shoppers", 2, 3, 10, 0.85, 0.90),

    # Email
    Row("CAMPAIGN_009", "Email - Welcome Series", "Email", "Lifecycle Email", date(2016, 10, 1), date(2018, 8, 31), 8000.00, "New subscribers", 4, 3, 10, 1.25, 1.15),
    Row("CAMPAIGN_010", "Email - Promotional Newsletter", "Email", "Promotional Email", date(2016, 10, 1), date(2018, 8, 31), 7000.00, "Deal-seeking subscribers", 3, 2, 14, 1.00, 1.00),
    Row("CAMPAIGN_011", "Email - Winback Series", "Email", "Lifecycle Email", date(2017, 1, 1), date(2018, 8, 31), 9000.00, "Inactive subscribers", 3, 5, 21, 0.95, 0.95),

    # Referral
    Row("CAMPAIGN_012", "Referral - Rewards Program", "Referral", "Referral", date(2016, 9, 15), date(2018, 9, 30), 15000.00, "Referred customers", 5, 0, 3, 1.60, 1.30),
    Row("CAMPAIGN_013", "Referral - Affiliate Partners", "Referral", "Affiliate", date(2016, 10, 1), date(2018, 9, 30), 12000.00, "Affiliate-referred shoppers", 4, 1, 5, 1.35, 1.15),

    # Paid Social
    Row("CAMPAIGN_014", "Paid Social - Prospecting", "Paid Social", "Social Ads", date(2016, 11, 1), date(2018, 8, 31), 30000.00, "Cold social audiences", 2, 5, 21, 0.75, 0.85),
    Row("CAMPAIGN_015", "Paid Social - Retargeting", "Paid Social", "Social Ads", date(2016, 11, 1), date(2018, 8, 31), 25000.00, "Recent site visitors", 4, 1, 7, 1.20, 1.10),
    Row("CAMPAIGN_016", "Paid Social - Lookalike Audiences", "Paid Social", "Social Ads", date(2017, 1, 1), date(2018, 8, 31), 22000.00, "Lookalike shoppers", 3, 3, 14, 0.95, 0.95),
    Row("CAMPAIGN_017", "Paid Social - Seasonal Promotion", "Paid Social", "Social Ads", date(2017, 11, 1), date(2018, 1, 31), 18000.00, "Holiday deal seekers", 3, 1, 10, 1.05, 1.00),
]

campaign_columns = [
    "campaign_id",
    "campaign_name",
    "acquisition_channel",
    "campaign_type",
    "campaign_start_date",
    "campaign_end_date",
    "campaign_budget",
    "target_audience",
    "quality_score",
    "conversion_days_min",
    "conversion_days_max",
    "ltv_multiplier",
    "repeat_customer_multiplier"
]

silver_marketing_campaigns = spark.createDataFrame(campaign_data, campaign_columns)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct

silver_marketing_campaigns.agg(
    count("*").alias("total_rows"),
    countDistinct("campaign_id").alias("distinct_campaigns")
).display()

# COMMAND ----------

# Write the silver marketing campaigns DataFrame to a table

silver_marketing_campaigns.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("dbw_nikao_learning.ecommerce_medallion.silver_marketing_campaigns")

# COMMAND ----------

from pyspark.sql.functions import (
    col, rand, when, expr, date_sub, floor
)

customer_metrics = spark.table(
    "dbw_nikao_learning.ecommerce_medallion.gold_customer_metrics"
)

campaigns = spark.table(
    "dbw_nikao_learning.ecommerce_medallion.silver_marketing_campaigns"
)

# COMMAND ----------

customer_channel_assignment = (
    customer_metrics
    .select("customer_unique_id", "first_purchase_date")
    .withColumn("channel_rand", rand(seed=42))
    .withColumn(
        "acquisition_channel",
        when(col("channel_rand") < 0.35, "Organic Search")
        .when(col("channel_rand") < 0.60, "Direct")
        .when(col("channel_rand") < 0.75, "Paid Search")
        .when(col("channel_rand") < 0.85, "Email")
        .when(col("channel_rand") < 0.93, "Referral")
        .otherwise("Paid Social")
    )
    .drop("channel_rand")
)

# COMMAND ----------

customer_campaign_candidates = (
    customer_channel_assignment.alias("c")
    .join(
        campaigns.alias("m"),
        on="acquisition_channel",
        how="left"
    )
    .withColumn("campaign_rand", rand(seed=99))
)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

campaign_window = Window.partitionBy("customer_unique_id").orderBy("campaign_rand")

customer_campaign_assignment = (
    customer_campaign_candidates
    .withColumn("rn", row_number().over(campaign_window))
    .filter(col("rn") == 1)
    .drop("rn", "campaign_rand")
)

# COMMAND ----------

silver_customer_acquisition = (
    customer_campaign_assignment
    .withColumn(
        "days_to_first_purchase",
        floor(
            rand(seed=123) * (
                col("conversion_days_max") - col("conversion_days_min") + 1
            )
        ) + col("conversion_days_min")
    )
    .withColumn(
        "acquisition_date",
        date_sub(
            col("first_purchase_date"),
            col("days_to_first_purchase").cast("int")
        )
    )
    .withColumn(
        "device_type",
        when(rand(seed=456) < 0.65, "Mobile")
        .when(rand(seed=456) < 0.95, "Desktop")
        .otherwise("Tablet")
    )
    .select(
        "customer_unique_id",
        "acquisition_channel",
        "campaign_id",
        "acquisition_date",
        "days_to_first_purchase",
        "device_type"
    )
)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct

silver_customer_acquisition.agg(
    count("*").alias("total_rows"),
    countDistinct("customer_unique_id").alias("distinct_customers")
).display()

# COMMAND ----------

invalid_acquisition_dates = (
    silver_customer_acquisition.alias("a")
    .join(
        customer_metrics.select("customer_unique_id", "first_purchase_date").alias("c"),
        on="customer_unique_id",
        how="left"
    )
    .filter(col("acquisition_date") > col("first_purchase_date"))
)

print("Invalid acquisition dates:", invalid_acquisition_dates.count())
display(invalid_acquisition_dates)

# COMMAND ----------

display(
    silver_customer_acquisition
    .groupBy("acquisition_channel")
    .agg(count("*").alias("customers"))
    .orderBy(col("customers").desc())
)

# COMMAND ----------

# Write silver_customer_acquisition to table

silver_customer_acquisition.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "dbw_nikao_learning.ecommerce_medallion.silver_customer_acquisition"
    )

# COMMAND ----------

spark.sql("""
SHOW TABLES IN dbw_nikao_learning.ecommerce_medallion
""").display()

# COMMAND ----------

display(
    spark.table(
        "dbw_nikao_learning.ecommerce_medallion.silver_customer_acquisition"
    ).limit(20)
)