-- DWD: typed, cleaned fact/dim. One grain per table. No business aggregation.
-- dim_user is full-refreshed each pipeline run (no SCD2).
CREATE OR REPLACE TEMP VIEW dwd_dim_user AS
SELECT
  user_id,
  to_date(register_dt) AS register_dt,
  city,
  gender,
  channel
FROM ods_users;

CREATE OR REPLACE TEMP VIEW dwd_fact_order AS
SELECT
  order_id,
  user_id,
  to_timestamp(order_ts) AS order_ts,
  status,
  CAST(pay_amount AS DOUBLE) AS pay_amount,
  CAST(refund_amount AS DOUBLE) AS refund_amount,
  ROUND(CAST(pay_amount AS DOUBLE) - CAST(refund_amount AS DOUBLE), 2) AS net_gmv,
  NULLIF(pay_channel, '') AS pay_channel,
  to_date(dt) AS dt,
  CASE WHEN status IN ('paid', 'shipped', 'completed') THEN 1 ELSE 0 END AS is_paid
FROM ods_orders;

CREATE OR REPLACE TEMP VIEW dwd_fact_order_item AS
SELECT
  i.order_item_id,
  i.order_id,
  o.user_id,
  o.dt,
  i.sku_id,
  i.sku_name,
  i.category,
  CAST(i.qty AS INT) AS qty,
  CAST(i.qty AS INT) AS gross_qty,
  CAST(i.unit_price AS DOUBLE) AS unit_price,
  CAST(i.amount AS DOUBLE) AS amount,
  CAST(i.refund_qty AS INT) AS refund_qty,
  CAST(i.refund_amount AS DOUBLE) AS refund_amount,
  CAST(i.qty AS INT) - CAST(i.refund_qty AS INT) AS net_qty,
  ROUND(CAST(i.amount AS DOUBLE) - CAST(i.refund_amount AS DOUBLE), 2) AS net_amount,
  o.is_paid
FROM ods_order_items i
JOIN dwd_fact_order o ON i.order_id = o.order_id;
