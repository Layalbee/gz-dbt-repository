WITH sales_margin_data AS (
    SELECT 
        s.orders_id,
        s.date_date,
        s.margin
    FROM {{ ref('int_sales_margin') }} s
),

shipping_data AS (
    SELECT 
        sh.orders_id,
        sh.shipping_fee,
        CAST(sh.ship_cost AS FLOAT64) AS ship_cost  -- Cast ship_cost to a numeric format
    FROM{{ ref('stg_raw__ship') }} sh
)

SELECT
    sm.orders_id,
    sm.date_date,
    -- Calculate the operational margin
    (sm.margin + sh.shipping_fee - sm.margin - sh.ship_cost) AS operational_margin
FROM sales_margin_data sm
JOIN shipping_data sh
  ON sm.orders_id = sh.orders_id