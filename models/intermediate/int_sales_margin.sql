WITH sales_data AS (
    SELECT * 
    FROM {{ ref('stg_raw__sales') }}
),

product_data AS (
    SELECT *
    FROM {{ ref('stg_raw__product') }}
)

SELECT
    s.date_date,
    s.orders_id,
    s.products_id,
    s.quantity,
    s.revenue,
  -- First calculate the purchase_cost
    CAST(s.quantity AS INT64) * CAST(p.purchase_price AS FLOAT64) AS purchase_cost,
    -- Then calculate margin using the purchase_cost
    (s.revenue - (s.quantity * p.purchase_price)) AS margin
FROM sales_data s
JOIN product_data p
  ON s.products_id = p.products_id