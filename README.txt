AWS Incremental Batch ETL Pipeline
Project Overview

This project demonstrates an incremental batch ETL pipeline using AWS services. The pipeline processes retail order CSV files uploaded to Amazon S3, transforms and validates the data using AWS Glue PySpark, stores clean records in a Silver layer, creates Gold summary tables, and queries the final output using Amazon Athena.

Architecture
```text
CSV Source Files
    ↓
S3 Landing Zone
    ↓
AWS Glue PySpark ETL Job
    ↓
Silver Layer in S3
    ↓
Gold Summary Tables in S3
    ↓
Glue Crawler
    ↓
Glue Data Catalog
    ↓
Athena SQL Queries
```

AWS Services Used
Amazon S3
AWS Glue
AWS Glue PySpark
AWS Glue Crawler
AWS Glue Data Catalog
Amazon Athena
IAM
CloudWatch Logs

S3 Folder Structure
```text
landing/orders/
silver/orders/
gold/daily_sales_summary/
gold/region_sales_summary/
gold/category_sales_summary/
control/processed_files/
quarantine/orders/
scripts/
logs/
```

ETL Process

Extract
Retail order CSV files are uploaded into the S3 landing zone:
```text
landing/orders/
```
Example input files:
```text
orders_batch_001.csv
orders_batch_002.csv
orders_batch_003.csv
orders_batch_004.csv
```

Transform
AWS Glue PySpark performs the transformation logic:
Reads CSV files from S3 Landing
Applies schema
Converts order dates into standard format
Calculates `total_amount`
Adds ETL processing timestamp
Validates data quality rules
Separates valid and invalid records
Creates Gold summary tables

Load

The transformed data is loaded into:
```text
silver/orders/
gold/daily_sales_summary/
gold/region_sales_summary/
gold/category_sales_summary/
control/processed_files/
quarantine/orders/
```

Incremental Processing Logic
The pipeline uses a processed-file control table stored in:
```text
control/processed_files/
```
The Glue job compares files in the landing folder with files already recorded in the control table.
It uses a left anti join to process only new files:
```python
unprocessed_df = landing_df.join(
    processed_df,
    on="file_name",
    how="left_anti"
)
```
This ensures already processed files are skipped.
Example:
```text
Batch 1 processed -> orders_batch_001.csv added to control table
Batch 2 uploaded -> only orders_batch_002.csv is processed
Batch 3 uploaded -> only orders_batch_003.csv is processed
Batch 4 uploaded -> only orders_batch_004.csv is processed
```

Data Quality Rules
The pipeline validates:
`order_id` is not null
`customer_id` is not null
`order_date` is valid
`quantity` is greater than 0
`unit_price` is greater than or equal to 0
`order_status` is one of Pending, Shipped, or Cancelled
`batch_id` is not null
Invalid records are written to:
```text
quarantine/orders/
```

Silver Layer
The Silver layer stores clean, detailed order records in Parquet format.
Key columns include:
```text
order_id
customer_id
order_date
product_category
region
quantity
unit_price
total_amount
order_status
batch_id
source_file
etl_processed_at
```

Gold Layer
The Gold layer stores business-ready summary tables:
```text
daily_sales_summary
region_sales_summary
category_sales_summary
```
These tables are used for analytics and reporting.

Athena Validation Queries

Validate rows by batch
```sql
SELECT 
  source_file,
  batch_id,
  COUNT(*) AS row_count
FROM silver
GROUP BY source_file, batch_id
ORDER BY source_file;
```

Check processing time by batch
```sql
SELECT 
  source_file,
  batch_id,
  COUNT(*) AS row_count,
  MIN(etl_processed_at) AS first_processed_time,
  MAX(etl_processed_at) AS last_processed_time
FROM silver
GROUP BY source_file, batch_id
ORDER BY first_processed_time;
```

Daily sales summary
```sql
SELECT *
FROM daily_sales_summary
ORDER BY order_date;
```

Region sales summary
```sql
SELECT *
FROM region_sales_summary
ORDER BY total_sales DESC;
```

Category sales summary
```sql
SELECT *
FROM category_sales_summary
ORDER BY total_sales DESC;
```

Project Validation
The project was validated using Athena queries to confirm:
Each batch was processed correctly
Incremental processing worked
New batches were appended to Silver
Gold summary tables were updated
Processing timestamps were captured
Data was queryable through Athena

Key Learning Outcomes
This project helped me understand:
Incremental batch ETL design
S3 landing, Silver, Gold, and quarantine layers
AWS Glue PySpark jobs
Processed-file tracking
Glue Crawlers and Data Catalog
Athena SQL validation
Real-world data lake ETL patterns

Project Status
Completed.