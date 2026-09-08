"""Core RetailDW semantics: refund, net GMV, idempotency, reconcile, quality."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobs"))
sys.path.insert(0, str(ROOT / "tests"))

from helpers import (  # noqa: E402
    build_layers,
    load_ods_dir,
    partition_dir,
    partition_fingerprint,
    prepare_temp_repo,
)
from quality import QualityError, run_ods_gates  # noqa: E402
from reconcile_gmv import reconcile  # noqa: E402


def test_refunded_does_not_enter_repurchase(spark, good_ods):
    """status=refunded => is_paid=0; that day does not create a repurchase cohort buyer."""
    build_layers(spark, good_ods)

    refunded = (
        spark.table("dwd_fact_order")
        .filter(F.col("status") == "refunded")
        .select("order_id", "user_id", "dt", "is_paid", "net_gmv")
        .collect()
    )
    assert len(refunded) == 1
    row = refunded[0]
    assert row["order_id"] == "O3"
    assert row["is_paid"] == 0
    assert abs(float(row["net_gmv"]) - 0.0) <= 0.01

    u2_d1 = (
        spark.table("dws_user_order_1d")
        .filter((F.col("user_id") == "U2") & (F.col("dt") == "2026-07-01"))
        .collect()
    )
    assert len(u2_d1) == 1
    assert u2_d1[0]["is_paid_buyer"] == 0

    buyers = (
        spark.table("ads_repurchase_7d")
        .filter(F.col("dt") == "2026-07-01")
        .collect()
    )
    assert len(buyers) == 1
    assert buyers[0]["buyers"] == 1
    assert buyers[0]["repurchase_users"] == 1


def test_net_gmv_equals_pay_minus_refund(spark, good_ods):
    """DWD net_gmv = pay_amount - refund_amount for every order."""
    build_layers(spark, good_ods)
    rows = spark.table("dwd_fact_order").collect()
    assert len(rows) == 5
    for r in rows:
        expected = round(float(r["pay_amount"]) - float(r["refund_amount"]), 2)
        assert abs(float(r["net_gmv"]) - expected) <= 0.01

    o4 = [r for r in rows if r["order_id"] == "O4"][0]
    assert abs(float(o4["net_gmv"]) - 180.0) <= 0.01


def test_same_dt_rerun_is_idempotent(spark, good_ods, tmp_path, monkeypatch):
    """Running pipeline twice for the same --dt yields identical partition + KPI CSV."""
    import pipeline

    # pipeline.main() would stop the shared session; keep it alive for other tests
    monkeypatch.setattr(pipeline.SparkSession, "stop", lambda self: None)

    tmp_repo = prepare_temp_repo(tmp_path / "run1", good_ods)
    monkeypatch.setattr(pipeline, "ROOT", tmp_repo)
    monkeypatch.setattr(pipeline, "WAREHOUSE", tmp_repo / "warehouse")

    assert pipeline.main([]) == 0

    # Windows CSV fallback: part-00000.csv; Linux: *.parquet under dt=...
    part = partition_dir(tmp_repo, "dwd/fact_order", "2026-07-01")
    first = partition_fingerprint(part)

    assert pipeline.main(["--dt", "2026-07-01"]) == 0
    second = partition_fingerprint(part)
    assert first == second

    assert pipeline.main(["--dt", "2026-07-01"]) == 0
    third = partition_fingerprint(part)
    assert second == third

    kpi = tmp_repo / "warehouse" / "ads" / "ads_kpi_overview.csv"
    kpi_a = kpi.read_text(encoding="utf-8")
    assert pipeline.main(["--dt", "2026-07-01"]) == 0
    kpi_b = kpi.read_text(encoding="utf-8")
    assert kpi_a == kpi_b


def test_dwd_dws_ads_gmv_reconcile_pass(spark, good_ods, tmp_path):
    """DWD ≡ DWS ≡ ADS net GMV reconcile returns PASS (exit 0)."""
    build_layers(spark, good_ods)
    report = tmp_path / "gmv_reconcile_report.csv"
    rc = reconcile(spark, report_path=report)
    assert rc == 0
    lines = [ln for ln in report.read_text(encoding="utf-8").splitlines()[1:] if ln.strip()]
    assert lines
    for ln in lines:
        assert ln.rstrip().endswith("PASS")


def test_quality_bad_fixture_exits_nonzero(spark, bad_ods, tmp_path, monkeypatch):
    """Bad ODS (negative pay_amount) must fail quality and pipeline exit 2."""
    users, orders, items = load_ods_dir(spark, bad_ods)
    with pytest.raises(QualityError) as ei:
        run_ods_gates(users, orders, items)
    assert "QUALITY FAIL" in str(ei.value)

    import pipeline

    monkeypatch.setattr(pipeline.SparkSession, "stop", lambda self: None)

    tmp_repo = prepare_temp_repo(tmp_path / "bad", bad_ods)
    monkeypatch.setattr(pipeline, "ROOT", tmp_repo)
    monkeypatch.setattr(pipeline, "WAREHOUSE", tmp_repo / "warehouse")
    rc = pipeline.main([])
    assert rc == 2
