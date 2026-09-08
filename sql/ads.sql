-- ADS: 7-day repurchase rate + supporting GMV (net of refunds).
-- Definition: among users with a paid order on day D (is_paid=1; refunded is NOT is_paid),
-- share who place another paid order on any day in (D, D+7]. Same-day multi-order is NOT repurchase.
-- Refunds only reduce GMV via net amounts; a refund-only day does not create repurchase buyers.
-- window_complete=0 means D+7 is beyond the sample max date — do not trend those days.
CREATE OR REPLACE TEMP VIEW ads_repurchase_7d AS
WITH params AS (
  SELECT MAX(dt) AS max_dt FROM dws_user_order_1d
),
buyers AS (
  SELECT user_id, dt AS cohort_dt
  FROM dws_user_order_1d
  WHERE is_paid_buyer = 1
),
rep AS (
  SELECT b.cohort_dt, b.user_id
  FROM buyers b
  JOIN dws_user_order_1d n
    ON b.user_id = n.user_id
   AND n.is_paid_buyer = 1
   AND n.dt > b.cohort_dt
   AND n.dt <= date_add(b.cohort_dt, 7)
  GROUP BY b.cohort_dt, b.user_id
)
SELECT
  b.cohort_dt AS dt,
  COUNT(DISTINCT b.user_id) AS buyers,
  COUNT(DISTINCT r.user_id) AS repurchase_users,
  ROUND(COUNT(DISTINCT r.user_id) / COUNT(DISTINCT b.user_id), 4) AS repurchase_rate_7d,
  g.gmv,
  CASE WHEN date_add(b.cohort_dt, 7) <= p.max_dt THEN 1 ELSE 0 END AS window_complete
FROM buyers b
LEFT JOIN rep r
  ON b.cohort_dt = r.cohort_dt AND b.user_id = r.user_id
CROSS JOIN params p
JOIN (
  SELECT dt, ROUND(SUM(gmv), 2) AS gmv
  FROM dws_user_order_1d
  GROUP BY dt
) g ON g.dt = b.cohort_dt
GROUP BY b.cohort_dt, g.gmv, p.max_dt;

CREATE OR REPLACE TEMP VIEW ads_kpi_overview AS
SELECT
  (SELECT COUNT(*) FROM dwd_fact_order WHERE is_paid = 1) AS paid_orders,
  (SELECT COUNT(DISTINCT user_id) FROM dwd_fact_order WHERE is_paid = 1) AS paid_users,
  (SELECT ROUND(SUM(gmv), 2) FROM dws_user_order_1d) AS gmv_total,
  SUM(CASE WHEN window_complete = 1 THEN buyers ELSE 0 END) AS buyers_complete_window,
  SUM(CASE WHEN window_complete = 1 THEN repurchase_users ELSE 0 END) AS repurchase_users_complete_window,
  ROUND(
    SUM(CASE WHEN window_complete = 1 THEN repurchase_users ELSE 0 END)
    / SUM(CASE WHEN window_complete = 1 THEN buyers ELSE 0 END),
    4
  ) AS repurchase_rate_7d_weighted
FROM ads_repurchase_7d;
