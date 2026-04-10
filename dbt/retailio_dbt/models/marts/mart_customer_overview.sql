-- Purpose:-- mart_customer_overview identifies and classifies customers by value for executive decision making
-- Solves: Lack of central repository for understanding customer performance
-- Source: int_enriched_sales
-- Grain: One row per customer, region and category combination

SELECT
    customer_id,
    customer_name,
    gender,
    region,
    category,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(sales_amount) as total_revenue,
    SUM(profit) as total_profit,
    CASE
        WHEN SUM(sales_amount) > AVG(SUM(sales_amount)) over() THEN 'high value'
        ELSE 'low value'
    END as customer_value_tier
FROM
    {{ ref('int_enriched_sales') }}
GROUP by 1,2,3,4,5
ORDER by total_revenue DESC






