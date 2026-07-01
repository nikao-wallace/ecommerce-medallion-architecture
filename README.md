# E-Commerce Analytics Platform Using Medallion Architecture

An end-to-end analytics engineering project built using Databricks, Delta Lake, and Medallion Architecture principles to transform raw e-commerce transactions into analytics-ready business datasets.

To create a more realistic business environment, the project extends the original dataset with synthetic customer acquisition and campaign attribution data to support customer lifecycle and marketing performance analysis.

---

## Project Objectives

* Build an end-to-end analytics pipeline using Medallion Architecture.
* Implement data quality checks and validation throughout the pipeline.
* Create analytics-ready business entities and dimensional models.
* Simulate realistic customer acquisition and campaign attribution scenarios.
* Produce business-facing metrics for customer, revenue, and marketing analysis.

---

## Technology Stack

### Languages

* SQL
* Python
* PySpark

### Data Platform

* Databricks
* Delta Lake
* Unity Catalog

### Cloud

* Azure Databricks
* Unity Catalog Volumes

### Analytics Concepts

* Medallion Architecture
* Dimensional Modeling
* Customer Analytics
* Marketing Attribution
* Data Quality Validation

---

## Architecture

```text
Raw CSV Files
    ↓
Bronze Layer
    ↓
Silver Business Entities
    ↓
Gold Business Models
    ↓
Synthetic Marketing Dimensions
    ↓
Customer Acquisition & Attribution Analytics
```

---

## Bronze Layer

The Bronze layer ingests raw source files from Unity Catalog Volumes and stores them as Delta tables with ingestion metadata.

### Responsibilities

* Raw data ingestion
* Schema inference
* Metadata tracking
* Delta table creation

### Metadata Columns

* `_ingested_at`
* `_source_file`

### Bronze Tables

* `bronze_customers`
* `bronze_orders`
* `bronze_order_items`
* `bronze_products`
* `bronze_order_payments`

---

## Silver Layer

The Silver layer standardizes and validates business entities while introducing business-friendly attributes for downstream analytics.

### Responsibilities

* Duplicate removal
* Referential integrity checks
* Date validation
* Business rule validation
* Entity enrichment

### Silver Tables

* `silver_customers`
* `silver_orders`
* `silver_order_items`
* `silver_products`
* `silver_order_payments`
* `silver_marketing_campaigns`
* `silver_customer_acquisition`

### Enrichment Examples

* `purchase_date`
* `purchase_month`
* `purchase_year`
* `purchase_month_name`
* `days_to_deliver`
* `is_late_delivery`

### Validation Checks

* Duplicate detection
* Missing foreign keys
* Invalid delivery dates
* Invalid approval dates
* Row count verification

---

## Gold Layer

The Gold layer produces analytics-ready business models optimized for reporting and decision-making.

### Current Gold Models

* `gold_order_facts`
* `gold_customer_metrics`

### Planned Gold Models

* Customer Acquisition Metrics
* Campaign Performance Metrics
* Customer Lifetime Value Analysis
* Channel Attribution Metrics

---

## Synthetic Marketing Data

The original Olist dataset does not contain customer acquisition or campaign attribution information.

To create a more realistic business environment, synthetic marketing dimensions were generated using observed customer purchasing behavior and customer value metrics derived from the Gold layer.

This approach allows the project to model:

* Multi-channel acquisition
* Campaign attribution
* Device segmentation
* Conversion windows
* Customer lifetime value differences by channel
* Repeat purchase behavior

### Acquisition Channels

* Organic Search
* Direct
* Paid Search
* Email
* Referral
* Paid Social

### Campaign Attributes

* Campaign budgets
* Quality scores
* Conversion windows
* Target audiences
* Lifetime value multipliers
* Repeat customer multipliers

---

## Repository Structure

```text
ecommerce-medallion-architecture/
│
├── notebooks/
│   ├── 01_ingestion_bronze.py
│   ├── 02_bronze_to_silver.py
│   ├── 03_synthetic_marketing_data.py
│   └── 04_silver_to_gold.py
│
├── docs/
├── diagrams/
├── images/
│
└── README.md
```

---

## Key Concepts Demonstrated

* Medallion Architecture
* Delta Lake
* Data Quality Validation
* Dimensional Modeling
* Customer Analytics
* Marketing Attribution
* Analytics Engineering Workflows
* Synthetic Data Generation
* Business Metric Development

---

## Future Enhancements

* Incremental processing
* Slowly changing dimensions
* dbt implementation
* Automated data quality testing
* Dashboard layer
* CI/CD deployment pipeline
