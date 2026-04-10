-- Purpose: stg_sales.sql cleans and standardises raw sales data for downstream use
-- Solves: Inconsistent formatting and data types from manual CSV uploads
-- Source: sales (raw table loaded by Airbyte from S3)
-- Grain: One row per order
-- Excluded: _ab_source_file_url, _ab_source_file_last_modified, _airbyte_raw_id,
--           _airbyte_extracted_at, _airbyte_meta as they have no analytical value.
--           row_id dropped because order_id already uniquely identifies each sale.
--           sales > 0 filter applied because zero values indicate typos or missing
--           figures which would corrupt business analysis calculations.


SELECT
    order_id,
    customer_id,
    product_id,
    TRIM(customer_name)                   AS customer_name,
    TRIM(product_name)                    AS product_name,
    CAST(quantity AS INTEGER)             AS quantity,
    CAST(sales AS DECIMAL(10,2))          AS sales_amount,
    CAST(profit AS DECIMAL(10,2))         AS profit,
    CAST(discount AS DECIMAL(4,2))        AS discount,
    LOWER(TRIM(category))                 AS category,
    LOWER(TRIM(sub_category))             AS sub_category,
    LOWER(TRIM(segment))                  AS customer_segment,
    LOWER(TRIM(ship_mode))                AS ship_mode,
    LOWER(TRIM(region))                   AS region,
    TRIM(city)                            AS city,
    LOWER(TRIM(state))                    AS state,
    TRIM(country)                         AS country,
    CAST(order_date AS DATE)              AS order_date,
    CAST(ship_date AS DATE)               AS ship_date

FROM sales
WHERE order_id IS NOT NULL
AND sales > 0