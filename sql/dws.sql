-- DWS: light summary at user-day / category-day. GMV uses net amounts after refunds.
CREATE OR REPLACE TEMP VIEW dws_user_order_1d AS
SELECT
  user_id,
  dt,
  COUNT(DISTINCT order_id) AS order_cnt,
  ROUND(SUM(CASE WHEN is_paid = 1 THEN net_gmv ELSE 0 END), 2) AS gmv,
  MAX(is_paid) AS is_paid_buyer
FROM dwd_fact_order
GROUP BY user_id, dt;

CREATE OR REPLACE TEMP VIEW dws_category_gmv_1d AS
SELECT
  category,
  dt,
  ROUND(SUM(CASE WHEN is_paid = 1 THEN net_amount ELSE 0 END), 2) AS gmv,
  SUM(CASE WHEN is_paid = 1 THEN qty ELSE 0 END) AS qty
FROM dwd_fact_order_item
GROUP BY category, dt;
