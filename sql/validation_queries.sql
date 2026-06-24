-- Check invalid records captured by the quarantine table
SELECT *
FROM retail_dlt_catalog.sales_quality.quarantine_sales_bad_records;

-- Check Gold revenue summary by product category
SELECT *
FROM retail_dlt_catalog.sales_quality.gold_revenue_by_category;

-- Check Gold daily sales summary
SELECT *
FROM retail_dlt_catalog.sales_quality.gold_daily_sales_summary;

-- Check clean Silver records
SELECT *
FROM retail_dlt_catalog.sales_quality.silver_sales_clean;

-- Check raw Bronze records
SELECT *
FROM retail_dlt_catalog.sales_quality.bronze_sales_raw;
