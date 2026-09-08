"""Core RetailDW semantics: refund, net GMV, idempotency, reconcile, quality, windows."""
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


def test_quality_bad_fk_fails(spark, bad_fk_ods):
    """Orphan order.user_id must fail FK gate."""
    users, orders, items = load_ods_dir(spark, bad_fk_ods)
    with pytest.raises(QualityError) as ei:
        run_ods_gates(users, orders, items)
    assert "fk_user" in str(ei.value) or "QUALITY FAIL" in str(ei.value)


def test_quality_bad_amount_fails(spark, bad_amount_ods):
    """item.amount != qty*unit_price must fail quality."""
    users, orders, items = load_ods_dir(spark, bad_amount_ods)
    with pytest.raises(QualityError) as ei:
        run_ods_gates(users, orders, items)
    assert "amount_eq_qty_price" in str(ei.value) or "qty * unit_price" in str(ei.value)


def test_gross_refund_net_qty_and_order_cnt_split(spark, good_ods):
    """gross_qty/refund_qty/net_qty consistent; order_cnt vs paid_order_cnt split."""
    build_layers(spark, good_ods)
    items = spark.table("dwd_fact_order_item").collect()
    for r in items:
        assert int(r["gross_qty"]) == int(r["qty"])
        assert int(r["net_qty"]) == int(r["gross_qty"]) - int(r["refund_qty"])

    # O3 refunded: user-day still has order_cnt>=1 but paid_order_cnt=0 / is_paid_buyer=0
    u2 = (
        spark.table("dws_user_order_1d")
        .filter((F.col("user_id") == "U2") & (F.col("dt") == "2026-07-01"))
        .collect()[0]
    )
    assert int(u2["order_cnt"]) >= 1
    assert int(u2["paid_order_cnt"]) == 0

    u1 = (
        spark.table("dws_user_order_1d")
        .filter((F.col("user_id") == "U1") & (F.col("dt") == "2026-07-01"))
        .collect()[0]
    )
    assert int(u1["order_cnt"]) == int(u1["paid_order_cnt"]) == 1

    cats = spark.table("dws_category_gmv_1d").collect()
    for r in cats:
        assert int(r["net_qty"]) == int(r["gross_qty"]) - int(r["refund_qty"])
        assert int(r["qty"]) == int(r["net_qty"])


def test_same_day_two_orders_not_repurchase(spark, window_ods):
    """Two paid orders on the same calendar day do not count as repurchase."""
    build_layers(spark, window_ods)
    row = (
        spark.table("ads_repurchase_7d")
        .filter(F.col("dt") == "2026-07-01")
        .collect()[0]
    )
    # U1 and U2 are buyers on 07-01; U1 repurchases on 07-02 (D+1); U2's next is D+8
    assert int(row["buyers"]) == 2
    assert int(row["repurchase_users"]) == 1  # only U1

    # U1 same-day: order_cnt=2 on 07-01 but still one buyer
    u1 = (
        spark.table("dws_user_order_1d")
        .filter((F.col("user_id") == "U1") & (F.col("dt") == "2026-07-01"))
        .collect()[0]
    )
    assert int(u1["order_cnt"]) == 2
    assert int(u1["paid_order_cnt"]) == 2


def test_repurchase_d1_yes_d8_no(spark, window_ods):
    """Repurchase on D+1 counts; return on D+8 does not (window is (D, D+7])."""
    build_layers(spark, window_ods)
    d1 = (
        spark.table("ads_repurchase_7d")
        .filter(F.col("dt") == "2026-07-01")
        .collect()[0]
    )
    assert int(d1["repurchase_users"]) == 1  # U1 at D+1 only

    # U2 cohort 07-01 should NOT include 07-09 (D+8)
    # buyers on 07-09: U2 only; incomplete window
    d9 = (
        spark.table("ads_repurchase_7d")
        .filter(F.col("dt") == "2026-07-09")
        .collect()[0]
    )
    assert int(d9["buyers"]) == 1
    assert int(d9["repurchase_users"]) == 0
    assert int(d9["window_complete"]) == 0


def test_incomplete_window_flag(spark, window_ods):
    """When D+7 > max_dt, window_complete=0."""
    build_layers(spark, window_ods)
    rows = {str(r["dt"]): r for r in spark.table("ads_repurchase_7d").collect()}
    # max_dt = 2026-07-09; cohort 07-02 => D+7=07-09 complete; 07-09 incomplete
    assert int(rows["2026-07-02"]["window_complete"]) == 1
    assert int(rows["2026-07-09"]["window_complete"]) == 0
    assert int(rows["2026-07-01"]["window_complete"]) == 1


def test_dt_bad_format_exits_one(spark, good_ods, tmp_path, monkeypatch):
    """Bad --dt format (not ISO date) exits 1 with clear error."""
    import pipeline

    monkeypatch.setattr(pipeline.SparkSession, "stop", lambda self: None)
    tmp_repo = prepare_temp_repo(tmp_path / "bad_dt", good_ods)
    monkeypatch.setattr(pipeline, "ROOT", tmp_repo)
    monkeypatch.setattr(pipeline, "WAREHOUSE", tmp_repo / "warehouse")
    assert pipeline.main(["--dt", "2026/07/01"]) == 1
    assert pipeline.main(["--dt", "not-a-date"]) == 1


def test_empty_partition_fail_and_allow(spark, good_ods, tmp_path, monkeypatch):
    """Empty ODS for --dt fails by default; --allow-empty-partition overwrites empty."""
    import pipeline

    monkeypatch.setattr(pipeline.SparkSession, "stop", lambda self: None)
    tmp_repo = prepare_temp_repo(tmp_path / "empty_part", good_ods)
    monkeypatch.setattr(pipeline, "ROOT", tmp_repo)
    monkeypatch.setattr(pipeline, "WAREHOUSE", tmp_repo / "warehouse")

    assert pipeline.main([]) == 0
    part = partition_dir(tmp_repo, "dwd/fact_order", "2026-07-01")
    assert part.exists()
    before = partition_fingerprint(part)

    # No ODS for this future dt
    assert pipeline.main(["--dt", "2099-01-01"]) == 2

    # Allow empty: create empty partition dir for 2099-01-01; 07-01 untouched
    assert pipeline.main(["--dt", "2099-01-01", "--allow-empty-partition"]) == 0
    empty_part = partition_dir(tmp_repo, "dwd/fact_order", "2099-01-01")
    assert empty_part.exists()
    assert partition_fingerprint(part) == before
    fp1 = partition_fingerprint(empty_part)
    # Idempotent empty overwrite (parquet/CSV identical semantics)
    assert pipeline.main(["--dt", "2099-01-01", "--allow-empty-partition"]) == 0
    fp2 = partition_fingerprint(empty_part)
    assert fp1 == fp2
