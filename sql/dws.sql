-- DWS: light summary at user-day / category-day. GMV uses net amounts after refunds.
-- order_cnt = all orders; paid_order_cnt = is_paid=1 only.
-- qty metrics: gross_qty / refund_qty / net_qty with net_qty = gross_qty - refund_qty.
CREATE OR REPLACE TEMP VIEW dws_user_order_1d AS
SELECT
  user_id,
  dt,
  COUNT(DISTINCT order_id) AS order_cnt,
  COUNT(DISTINCT CASE WHEN is_paid = 1 THEN order_id END) AS paid_order_cnt,
  ROUND(SUM(CASE WHEN is_paid = 1 THEN net_gmv ELSE 0 END), 2) AS gmv,
  MAX(is_paid) AS is_paid_buyer
FROM dwd_fact_order
GROUP BY user_id, dt;

CREATE OR REPLACE TEMP VIEW dws_category_gmv_1d AS
SELECT
  category,
  dt,
  ROUND(SUM(CASE WHEN is_paid = 1 THEN net_amount ELSE 0 END), 2) AS gmv,
  SUM(CASE WHEN is_paid = 1 THEN gross_qty ELSE 0 END) AS gross_qty,
  SUM(CASE WHEN is_paid = 1 THEN refund_qty ELSE 0 END) AS refund_qty,
  SUM(CASE WHEN is_paid = 1 THEN net_qty ELSE 0 END) AS net_qty,
  SUM(CASE WHEN is_paid = 1 THEN net_qty ELSE 0 END) AS qty
FROM dwd_fact_order_item
GROUP BY category, dt;
