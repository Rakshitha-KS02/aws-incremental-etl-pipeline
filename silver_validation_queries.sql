-- =====================================================
-- Silver Layer Validation Queries
-- Database: retail_incremental_etl_db
-- Table: silver
-- =====================================================


-- 1. View all clean Silver records
SELECT *
FROM silver
ORDER BY order_date, order_id;


-- 2. Count total Silver records
SELECT COUNT(*) AS total_silver_records
FROM silver;


-- 3. Validate row count by source file and batch
SELECT 
    source_file,
    batch_id,
    COUNT(*) AS row_count
FROM silver
GROUP BY source_file, batch_id
ORDER BY source_file;


-- 4. Check processing time by batch
SELECT 
    source_file,
    batch_id,
    COUNT(*) AS row_count,
    MIN(etl_processed_at) AS first_processed_time,
    MAX(etl_processed_at) AS last_processed_time
FROM silver
GROUP BY source_file, batch_id
ORDER BY first_processed_time;


-- 5. Check detailed Silver records
SELECT 
    order_id,
    customer_id,
    order_date,
    product_category,
    region,
    quantity,
    unit_price,
    total_amount,
    order_status,
    batch_id,
    source_file,
    etl_processed_at
FROM silver
ORDER BY order_date, order_id;


-- 6. Validate total_amount calculation
SELECT 
    order_id,
    quantity,
    unit_price,
    total_amount,
    quantity * unit_price AS expected_total_amount
FROM silver
WHERE total_amount <> quantity * unit_price;


-- 7. Check records by order status
SELECT 
    order_status,
    COUNT(*) AS order_count,
    SUM(total_amount) AS total_sales
FROM silver
GROUP BY order_status
ORDER BY order_count DESC;


-- 8. Check records by order date
SELECT 
    order_date,
    COUNT(*) AS order_count,
    SUM(quantity) AS total_quantity,
    SUM(total_amount) AS total_sales
FROM silver
GROUP BY order_date
ORDER BY order_date;