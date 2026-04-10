-- Purpose: stg_products.sql cleans and standardises raw product data for downstream use
-- Solves: Inconsistent formatting and data types from manual CSV uploads
-- Source: products (raw table loaded by Airbyte from S3)
-- Grain: One row per product
-- Excluded: Airbyte metadata columns, product_description, product_dimensions,
--           color_size_variations as they have no analytical value

select 
    product_id,
    TRIM(sku) as sku,
    TRIM(product_name) as product_name,
    LOWER(TRIM(product_category)) as product_category,
    CAST(price as DECIMAL(10,2)) as price,
    CAST(stock_quantity as INTEGER) as stock_quantity,
    CAST(product_ratings as DECIMAL(3,1)) as product_ratings,
    TRIM(warranty_period) as warranty_period,
    TRIM(product_tags) as product_tags,
    CAST(expiration_date as DATE) as expiration_date,
    CAST(manufacturing_date as DATE) as manufacturing_date
from products 
where product_id is not NULL