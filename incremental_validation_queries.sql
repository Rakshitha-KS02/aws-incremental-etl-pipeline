-- =====================================================
-- Incremental ETL Validation Queries
-- Database: retail_incremental_etl_db
-- Tables:
-- silver
-- processed_files
-- =====================================================


-- 1. Prove each source file was loaded into Silver
SELECT 
    source_file,
    batch_id,
    COUNT(*) AS row_count
FROM silver
GROUP BY source_file, batch_id
ORDER BY source_file;


-- 2. Show processing timestamp by batch from Silver
SELECT 
    source_file,
    batch_id,
    COUNT(*) AS row_count,
    MIN(etl_processed_at) AS first_processed_time,
    MAX(etl_processed_at) AS last_processed_time
FROM silver
GROUP BY source_file, batch_id
ORDER BY first_processed_time;


-- 3. Show latest processed batch
SELECT 
    source_file,
    batch_id,
    MAX(etl_processed_at) AS latest_processed_time
FROM silver
GROUP BY source_file, batch_id
ORDER BY latest_processed_time DESC
LIMIT 1;


-- 4. Check processed file control table
-- Note: If your crawler created a different table name, replace processed_files with that table name.
SELECT 
    file_name,
    batch_id,
    status,
    processed_at
FROM processed_files
ORDER BY processed_at;


-- 5. Count processed files in control table
SELECT 
    COUNT(DISTINCT file_name) AS total_processed_files
FROM processed_files;


-- 6. Compare Silver source files with control table
SELECT 
    s.source_file,
    s.batch_id,
    COUNT(*) AS silver_row_count,
    p.status,
    p.processed_at
FROM silver s
LEFT JOIN processed_files p
    ON s.source_file = p.file_name
GROUP BY 
    s.source_file,
    s.batch_id,
    p.status,
    p.processed_at
ORDER BY s.source_file;


-- 7. Check if any file exists in Silver but not in control table
SELECT DISTINCT
    s.source_file
FROM silver s
LEFT JOIN processed_files p
    ON s.source_file = p.file_name
WHERE p.file_name IS NULL;


-- 8. Check if any file exists in control table but not in Silver
SELECT DISTINCT
    p.file_name
FROM processed_files p
LEFT JOIN silver s
    ON p.file_name = s.source_file
WHERE s.source_file IS NULL;


-- 9. Batch-level sales validation
SELECT 
    source_file,
    batch_id,
    COUNT(order_id) AS total_orders,
    SUM(quantity) AS total_quantity,
    SUM(total_amount) AS total_sales,
    MIN(etl_processed_at) AS processed_time
FROM silver
GROUP BY source_file, batch_id
ORDER BY source_file;


-- 10. Full incremental proof
-- This query shows that each batch has its own source file, row count, and processing time.
SELECT 
    source_file,
    batch_id,
    COUNT(*) AS row_count,
    MIN(order_date) AS min_order_date,
    MAX(order_date) AS max_order_date,
    MIN(etl_processed_at) AS first_processed_time,
    MAX(etl_processed_at) AS last_processed_time
FROM silver
GROUP BY source_file, batch_id
ORDER BY first_processed_time;