WITH sales_margin_data AS (
    SELECT 
        s.orders_id,
        s.date_date,
        s.revenue,
        s.quantity,
        s.purchase_cost,
        s.margin
    FROM {{ ref('int_sales_margin') }} s
)

SELECT
    orders_id,
    -- For 'date_date', aggregate by taking the MIN, MAX, or any other suitable logic
    MIN(date_date) AS date_date,  -- Aggregating the 'date_date' column
    SUM(revenue) AS revenue,
    SUM(quantity) AS quantity,
    SUM(purchase_cost) AS purchase_cost,
    SUM(margin) AS margin
FROM sales_margin_data
GROUP BY orders_id 