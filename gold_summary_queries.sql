-- =====================================================
-- Gold Layer Summary Queries
-- Database: retail_incremental_etl_db
-- Tables:
-- daily_sales_summary
-- region_sales_summary
-- category_sales_summary
-- =====================================================


-- 1. View daily sales summary
SELECT *
FROM daily_sales_summary
ORDER BY order_date;


-- 2. View region sales summary
SELECT *
FROM region_sales_summary
ORDER BY total_sales DESC;


-- 3. View category sales summary
SELECT *
FROM category_sales_summary
ORDER BY total_sales DESC;


-- 4. Validate daily sales summary from Silver
SELECT 
    order_date,
    COUNT(order_id) AS total_orders,
    SUM(quantity) AS total_quantity,
    SUM(total_amount) AS total_sales
FROM silver
GROUP BY order_date
ORDER BY order_date;


-- 5. Validate region sales summary from Silver
SELECT 
    region,
    COUNT(order_id) AS total_orders,
    SUM(total_amount) AS total_sales
FROM silver
GROUP BY region
ORDER BY total_sales DESC;


-- 6. Validate category sales summary from Silver
SELECT 
    product_category,
    COUNT(order_id) AS total_orders,
    SUM(total_amount) AS total_sales
FROM silver
GROUP BY product_category
ORDER BY total_sales DESC;


-- 7. Sales by region and category
SELECT 
    region,
    product_category,
    COUNT(order_id) AS total_orders,
    SUM(quantity) AS total_quantity,
    SUM(total_amount) AS total_sales
FROM silver
GROUP BY region, product_category
ORDER BY region, total_sales DESC;


-- 8. Highest sales day
SELECT 
    order_date,
    total_orders,
    total_quantity,
    total_sales
FROM daily_sales_summary
ORDER BY total_sales DESC
LIMIT 1;


-- 9. Highest sales region
SELECT 
    region,
    total_orders,
    total_sales
FROM region_sales_summary
ORDER BY total_sales DESC
LIMIT 1;


-- 10. Highest sales category
SELECT 
    product_category,
    total_orders,
    total_sales
FROM category_sales_summary
ORDER BY total_sales DESC
LIMIT 1;