#!/usr/bin/env python3
"""Reconcile DWD / DWS / ADS net GMV. Writes report CSV; FAIL exits 2."""
from __future__ import annotations

import csv
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
REPORT = ROOT / "warehouse" / "ads" / "gmv_reconcile_report.csv"
TOL = 0.01


def reconcile(spark: SparkSession, report_path: Path | None = None) -> int:
    """Compare layer GMV totals and per-day keys. Return 0 PASS / 2 FAIL."""
    report_path = report_path or REPORT
    report_path.parent.mkdir(parents=True, exist_ok=True)

    dwd_total = (
        spark.table("dwd_fact_order")
        .agg(
            F.round(
                F.sum(F.when(F.col("is_paid") == 1, F.col("net_gmv")).otherwise(0.0)),
                2,
            ).alias("gmv")
        )
        .collect()[0]["gmv"]
        or 0.0
    )
    dws_total = (
        spark.table("dws_user_order_1d")
        .agg(F.round(F.sum("gmv"), 2).alias("gmv"))
        .collect()[0]["gmv"]
        or 0.0
    )
    ads_row = spark.table("ads_kpi_overview").collect()
    ads_total = float(ads_row[0]["gmv_total"]) if ads_row else 0.0

    dwd_day = {
        r["dt"].isoformat() if hasattr(r["dt"], "isoformat") else str(r["dt"]): float(
            r["gmv"] or 0.0
        )
        for r in (
            spark.table("dwd_fact_order")
            .groupBy("dt")
            .agg(
                F.round(
                    F.sum(
                        F.when(F.col("is_paid") == 1, F.col("net_gmv")).otherwise(0.0)
                    ),
                    2,
                ).alias("gmv")
            )
            .collect()
        )
    }
    dws_day = {
        r["dt"].isoformat() if hasattr(r["dt"], "isoformat") else str(r["dt"]): float(
            r["gmv"] or 0.0
        )
        for r in (
            spark.table("dws_user_order_1d")
            .groupBy("dt")
            .agg(F.round(F.sum("gmv"), 2).alias("gmv"))
            .collect()
        )
    }
    ads_day = {
        r["dt"].isoformat() if hasattr(r["dt"], "isoformat") else str(r["dt"]): float(
            r["gmv"] or 0.0
        )
        for r in spark.table("ads_repurchase_7d").select("dt", "gmv").collect()
    }

    rows: list[dict] = []
    ok = True

    def check(scope: str, left: str, right: str, a: float, b: float) -> None:
        nonlocal ok
        diff = abs(float(a) - float(b))
        status = "PASS" if diff <= TOL else "FAIL"
        if status == "FAIL":
            ok = False
        rows.append(
            {
                "scope": scope,
                "left_layer": left,
                "right_layer": right,
                "left_gmv": f"{float(a):.2f}",
                "right_gmv": f"{float(b):.2f}",
                "abs_diff": f"{diff:.4f}",
                "tolerance": f"{TOL:.2f}",
                "status": status,
            }
        )

    check("total", "dwd", "dws", dwd_total, dws_total)
    check("total", "dws", "ads", dws_total, ads_total)
    check("total", "dwd", "ads", dwd_total, ads_total)

    # Per-day: DWD ≡ DWS for every dt present in either; ADS day only where buyers exist
    all_dt = sorted(set(dwd_day) | set(dws_day))
    for dt in all_dt:
        check(f"day:{dt}", "dwd", "dws", dwd_day.get(dt, 0.0), dws_day.get(dt, 0.0))
    for dt, g in sorted(ads_day.items()):
        check(f"day:{dt}", "dws", "ads", dws_day.get(dt, 0.0), g)

    with report_path.open("w", encoding="utf-8", newline="") as f:
        fields = [
            "scope",
            "left_layer",
            "right_layer",
            "left_gmv",
            "right_gmv",
            "abs_diff",
            "tolerance",
            "status",
        ]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    summary = "PASS" if ok else "FAIL"
    print(f"[reconcile] {summary}  report={report_path}")
    for r in rows:
        if r["status"] == "FAIL":
            print(
                f"  FAIL {r['scope']} {r['left_layer']}={r['left_gmv']} "
                f"{r['right_layer']}={r['right_gmv']} diff={r['abs_diff']}"
            )
    return 0 if ok else 2


if __name__ == "__main__":
    # Standalone requires existing spark tables; prefer pipeline entry.
    print("Use via jobs/pipeline.py (creates temp views then reconciles).", file=sys.stderr)
    sys.exit(1)
