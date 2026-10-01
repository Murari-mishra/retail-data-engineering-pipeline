-- Check total number of records

SELECT COUNT(*) AS total_records
FROM retail_orders;


-- Check missing order IDs

SELECT COUNT(*) AS missing_order_ids
FROM retail_orders
WHERE order_id IS NULL
   OR TRIM(order_id) = '';


-- Check duplicate order IDs

SELECT
    COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_order_ids
FROM retail_orders;


-- Show sample records

SELECT *
FROM retail_orders
LIMIT 10;