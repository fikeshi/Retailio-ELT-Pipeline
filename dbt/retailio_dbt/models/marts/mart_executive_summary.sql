-- Purpose: mart_executive_summary proovides high level monthly business overview for executive decision making
-- Solves: Lack of central repository for executive decision making
-- Source: int_enriched_sales
-- Grain: One row per month representing entire business performance

SELECT
    DATE_TRUNC('month', order_date) as month,
    STRFTIME(order_date, '%B %Y') as month_name,
    COUNT(DISTINCT order_id) as total_order,
    COUNT(DISTINCT customer_id) as total_customer,
    SUM(sales_amount) as total_revenue,
    SUM(profit) as total_profit,
    ROUND((SUM(profit)/SUM(sales_amount))*100,2) as profit_margin_perct,
    ROUND(AVG(discount)*100, 2) as average_discount_perct,
    ROUND((SUM(sales_amount)/ COUNT(DISTINCT order_id)) * 100, 2) as average_order_value
FROM
    {{ ref('int_enriched_sales') }}
GROUP BY 1,2
ORDER BY month