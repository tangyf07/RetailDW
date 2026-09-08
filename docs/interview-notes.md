# Interview notes (optional)

Material moved out of the README so the first screen stays product-focused.
Use only if you need a short verbal walkthrough.

## 30-second summary

- Synthetic e-commerce orders (48 users / 123 orders / 188 items, 2026-07-01–2026-08-15), fully offline CSV.
- Layers: ODS → quality gates → DWD → DWS → ADS.
- Hard gates: PK, nulls, enums, FK, order amount = item sum; fail ⇒ exit 2.
- KPI: among paid buyers on day D, share who pay again in (D, D+7]; incomplete windows flagged.

## Why this layering

| Layer | Why separate |
| --- | --- |
| ODS | Trace back to files; do not bake cleaning into source |
| DWD | One grain per table; freeze `is_paid` / net GMV here |
| DWS | Avoid ADS repeatedly scanning item grain |
| ADS | Product metrics + window completeness as product rules |

## Design choices you may be asked about

- No Hive/Iceberg: local overwrite + dt partitions enough for the demo; production would swap storage and scheduling.
- No dimension zipper: sample has no user attribute change.
- Repurchase uses order day, not payment-callback day (sample only has `order_ts`).
- Quality between ODS and DWD so dirty rows never enter the subject area.
- Small sample on purpose: finish in minutes on a laptop; bottlenecks at scale are partitions/skew/files, not the SQL shape.

## 2-minute talk track

1. Runnable four-layer warehouse, not an empty diagram — clone and get 7-day repurchase + net GMV.
2. ODS ingest only; gates cover PK/FK/amounts; DWD freezes `is_paid`; DWS supplies user-day for ADS.
3. Repurchase denominator = paid users that day; numerator = paid again within 7 days; same day does not count; last 7 sample days incomplete.
4. Point at `ads_kpi_overview.csv` and `gmv_reconcile_report.csv` PASS.

## Soft quality (not blocking in this demo)

Same-user same-second duplicate orders, city values outside a fixed enum — in production these would go to a quality report rather than killing the job.
