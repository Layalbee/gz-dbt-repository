SELECT
  m.date_date,
  ROUND(COUNT(m.orders_id), 2) AS nb_transactions,
  ROUND(SUM(m.revenue), 2) AS revenue,
  ROUND(SAFE_DIVIDE(SUM(m.revenue), COUNT(m.orders_id)), 2) AS average_basket,
  ROUND(SUM(m.margin), 2) AS margin,
  ROUND(SUM(o.operational_margin), 2) AS operational_margin,
  ROUND(SUM(m.purchase_cost), 2) AS purchase_cost,
  ROUND(SUM(s.shipping_fee), 2) AS shipping_fee,
  ROUND(SUM(s.logcost), 2) AS logcost,
  ROUND(SUM(m.quantity), 2) AS quantity

FROM {{ ref('int_orders_margin') }} AS m
LEFT JOIN {{ ref("int_orders_operational") }} AS o
  ON m.orders_id = o.orders_id
LEFT JOIN {{ ref("stg_raw__ship") }} AS s
  ON m.orders_id = s.orders_id

GROUP BY m.date_date
ORDER BY m.date_date DESC



