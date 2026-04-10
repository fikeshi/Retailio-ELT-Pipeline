-- Purpose: mart_sales_overview provides monthly sales performance overview for management visibility
-- Solves: Retailio's challenge of limited visibility into sales performance
-- Source: int_enriched_sales
-- Grain: One row per month, region and category combination


SELECT 
    DATE_TRUNC('month', order_date) as month,
    region,
    category,
    COUNT(DISTINCT(order_id)) as total_order,
    SUM(sales_amount) as total_revenue,
    SUM(profit) as total_profit,
    ROUND((SUM(profit)/SUM(sales_amount))*100,2) as profit_margin_perct,
    ROUND(AVG(discount)*100,2) as average_discount_perct,
    ROUND(SUM(sales_amount)/COUNT(DISTINCT order_id),2) as average_order_value,
FROM 
    {{ ref('int_enriched_sales') }}
GROUP BY 1,2,3
ORDER BY month