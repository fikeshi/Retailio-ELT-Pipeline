-- Purpose: int_enriched_sales joins clean sales data with customer details into one enriched table
-- Solves: Data silos by combining branch sales with customer information
-- Source: stg_sales, stg_customers
-- Grain: One row per order
-- Note: stg_products table excluded due to incompatible product_id formats
--       between sales and products datasets (T-C-0134 vs P000001)

select 
--sales details
    s.order_id,
    s.order_date,
    s.sales_amount,
    s.product_name,
    s.profit,
    s.discount,
    s.quantity,
    s.category,
    s.sub_category,
    s.customer_segment,
    s.ship_mode,
    s.city,
    s.state,
    s.region,
    s.country,
    s.ship_date,

--customer details
    c.customer_id,
    c.customer_name,
    c.gender


from {{ ref('stg_sales') }} s
left join {{ ref('stg_customers') }} c on s.customer_id = c.customer_id
