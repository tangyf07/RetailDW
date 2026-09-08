#!/usr/bin/env python3
"""Spark retail DW: ODS CSV -> quality gates -> DWD -> DWS -> ADS (local mode).

Supports optional --dt YYYY-MM-DD for partition-scoped re-runs with dynamic
partition overwrite (idempotent for the same dt).
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import shutil
import sys
from pathlib import Path

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "jobs"))
from quality import QualityError, run_ods_gates  # noqa: E402
from reconcile_gmv import reconcile  # noqa: E402

WAREHOUSE = ROOT / "warehouse"
DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="RetailDW local pipeline")
    p.add_argument(
        "--dt",
        default=None,
        help="Partition date YYYY-MM-DD; default=full overwrite of all partitions",
    )
    return p.parse_args(argv)


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName("spark-retail-dw")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def exec_sql_file(spark: SparkSession, path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    buf: list[str] = []
    for raw in text.splitlines():
        line = raw.split("--", 1)[0].rstrip()
        if line.strip():
            buf.append(line)
    body = "\n".join(buf)
    for stmt in body.split(";"):
        stmt = stmt.strip()
        if stmt:
            spark.sql(stmt)


def load_ods(spark: SparkSession):
    ods = ROOT / "data" / "ods"
    users = (
        spark.read.option("header", True)
        .option("nullValue", "")
        .csv(str(ods / "users.csv"))
    )
    orders = (
        spark.read.option("header", True)
        .option("nullValue", "")
        .csv(str(ods / "orders.csv"))
        .withColumn("pay_amount", F.col("pay_amount").cast("double"))
        .withColumn("refund_amount", F.col("refund_amount").cast("double"))
    )
    items = (
        spark.read.option("header", True)
        .option("nullValue", "")
        .csv(str(ods / "order_items.csv"))
        .withColumn("qty", F.col("qty").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("refund_qty", F.col("refund_qty").cast("int"))
        .withColumn("refund_amount", F.col("refund_amount").cast("double"))
    )
    return users, orders, items


def _write_csv_rows(df: DataFrame, path: Path) -> None:
    rows = df.collect()
    fields = df.columns
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r[k] for k in fields})
    print(f"[write] csv     {path}  ({len(rows)} rows)")


def save_csv(df: DataFrame, rel: str) -> None:
    path = WAREHOUSE / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    _write_csv_rows(df, path)


def save_table(df: DataFrame, rel: str) -> None:
    """Whole-table overwrite (dims / non-partitioned)."""
    path = WAREHOUSE / rel
    if os.name != "nt":
        try:
            if path.exists():
                shutil.rmtree(path)
            df.write.mode("overwrite").parquet(str(path))
            print(f"[write] parquet {path}")
            return
        except Exception as e:
            print(f"[write] parquet failed ({e.__class__.__name__}); csv fallback")
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    _write_csv_rows(df, path / "part-00000.csv")


def save_partitioned(df: DataFrame, rel: str, dt: str | None) -> None:
    """Write fact/dws tables partitioned by dt with dynamic overwrite semantics."""
    path = WAREHOUSE / rel
    if "dt" not in df.columns:
        raise RuntimeError(f"{rel}: expected dt column for partitioned write")

    # Ensure dt is string yyyy-MM-dd for stable partition paths
    out = df.withColumn("dt", F.date_format(F.col("dt").cast("date"), "yyyy-MM-dd"))

    if os.name != "nt":
        try:
            if dt is None and path.exists():
                shutil.rmtree(path)
            (
                out.write.mode("overwrite")
                .option("partitionOverwriteMode", "dynamic")
                .partitionBy("dt")
                .parquet(str(path))
            )
            print(f"[write] parquet {path}  (partitionBy=dt, dt={dt or 'ALL'})")
            return
        except Exception as e:
            print(f"[write] parquet failed ({e.__class__.__name__}); csv fallback")

    # CSV partition layout: warehouse/.../dt=YYYY-MM-DD/part-00000.csv
    if dt is None:
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)
        dts = [r["dt"] for r in out.select("dt").distinct().collect()]
        for d in dts:
            part_df = out.filter(F.col("dt") == d).drop("dt")
            part_dir = path / f"dt={d}"
            part_dir.mkdir(parents=True, exist_ok=True)
            _write_csv_rows(part_df, part_dir / "part-00000.csv")
    else:
        path.mkdir(parents=True, exist_ok=True)
        part_dir = path / f"dt={dt}"
        if part_dir.exists():
            shutil.rmtree(part_dir)
        part_dir.mkdir(parents=True, exist_ok=True)
        part_df = out.filter(F.col("dt") == dt).drop("dt")
        _write_csv_rows(part_df, part_dir / "part-00000.csv")
    print(f"[write] csv-parts {path}  (dt={dt or 'ALL'})")


def _is_parquet_dir(path: Path) -> bool:
    if not path.exists():
        return False
    return any(path.rglob("*.parquet"))


def read_partitioned(spark: SparkSession, rel: str) -> DataFrame:
    """Read all dt partitions from warehouse (parquet or csv-parts)."""
    path = WAREHOUSE / rel
    if not path.exists():
        raise FileNotFoundError(path)
    if _is_parquet_dir(path):
        return spark.read.parquet(str(path))
    # CSV parts: merge partition folders, inject dt from path
    frames: list[DataFrame] = []
    for part in sorted(path.glob("dt=*")):
        if not part.is_dir():
            continue
        dt_val = part.name.split("=", 1)[1]
        csv_files = list(part.glob("*.csv"))
        if not csv_files:
            continue
        # read each part file
        for cf in csv_files:
            df = (
                spark.read.option("header", True)
                .option("nullValue", "")
                .csv(str(cf))
                .withColumn("dt", F.lit(dt_val))
            )
            frames.append(df)
    if not frames:
        raise FileNotFoundError(f"no partitions under {path}")
    merged = frames[0]
    for f in frames[1:]:
        merged = merged.unionByName(f, allowMissingColumns=True)
    return merged


def read_table(spark: SparkSession, rel: str) -> DataFrame:
    path = WAREHOUSE / rel
    if _is_parquet_dir(path):
        return spark.read.parquet(str(path))
    csv_path = path / "part-00000.csv"
    if csv_path.exists():
        return spark.read.option("header", True).option("nullValue", "").csv(str(csv_path))
    raise FileNotFoundError(path)


def cast_dwd_order(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("pay_amount", F.col("pay_amount").cast("double"))
        .withColumn("refund_amount", F.col("refund_amount").cast("double"))
        .withColumn("net_gmv", F.col("net_gmv").cast("double"))
        .withColumn("is_paid", F.col("is_paid").cast("int"))
        .withColumn("dt", F.to_date(F.col("dt")))
    )


def cast_dwd_item(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("qty", F.col("qty").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("refund_qty", F.col("refund_qty").cast("int"))
        .withColumn("refund_amount", F.col("refund_amount").cast("double"))
        .withColumn("net_amount", F.col("net_amount").cast("double"))
        .withColumn("is_paid", F.col("is_paid").cast("int"))
        .withColumn("dt", F.to_date(F.col("dt")))
    )


def cast_dws_user(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("order_cnt", F.col("order_cnt").cast("long"))
        .withColumn("gmv", F.col("gmv").cast("double"))
        .withColumn("is_paid_buyer", F.col("is_paid_buyer").cast("int"))
        .withColumn("dt", F.to_date(F.col("dt")))
    )


def cast_dws_cat(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("gmv", F.col("gmv").cast("double"))
        .withColumn("qty", F.col("qty").cast("long"))
        .withColumn("dt", F.to_date(F.col("dt")))
    )


def rebuild_ads_from_warehouse(spark: SparkSession) -> None:
    """After a dt-scoped write, rebuild ADS from all on-disk DWD/DWS partitions.

    Choice: ADS is always full-refresh from warehouse so day/total GMV stay consistent.
    """
    dwd_user = read_table(spark, "dwd/dim_user")
    dwd_order = cast_dwd_order(read_partitioned(spark, "dwd/fact_order"))
    dwd_item = cast_dwd_item(read_partitioned(spark, "dwd/fact_order_item"))
    dws_user = cast_dws_user(read_partitioned(spark, "dws/user_order_1d"))
    dws_cat = cast_dws_cat(read_partitioned(spark, "dws/category_gmv_1d"))

    dwd_user.createOrReplaceTempView("dwd_dim_user")
    dwd_order.createOrReplaceTempView("dwd_fact_order")
    dwd_item.createOrReplaceTempView("dwd_fact_order_item")
    dws_user.createOrReplaceTempView("dws_user_order_1d")
    dws_cat.createOrReplaceTempView("dws_category_gmv_1d")
    exec_sql_file(spark, ROOT / "sql" / "ads.sql")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    dt = args.dt
    if dt is not None and not DT_RE.match(dt):
        print(f"Invalid --dt {dt!r}; expected YYYY-MM-DD", file=sys.stderr)
        return 1

    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        users, orders, items = load_ods(spark)
        if dt:
            orders = orders.filter(F.col("dt") == dt)
            items = items.join(orders.select("order_id"), on="order_id", how="inner")
            print(f"[ods] filtered to dt={dt}")

        users.createOrReplaceTempView("ods_users")
        orders.createOrReplaceTempView("ods_orders")
        items.createOrReplaceTempView("ods_order_items")
        print(
            f"[ods] users={users.count()} orders={orders.count()} items={items.count()}"
        )

        run_ods_gates(users, orders, items)

        exec_sql_file(spark, ROOT / "sql" / "dwd.sql")
        exec_sql_file(spark, ROOT / "sql" / "dws.sql")
        exec_sql_file(spark, ROOT / "sql" / "ads.sql")

        dwd_user = spark.table("dwd_dim_user")
        dwd_order = spark.table("dwd_fact_order")
        dwd_item = spark.table("dwd_fact_order_item")
        dws_user = spark.table("dws_user_order_1d")
        dws_cat = spark.table("dws_category_gmv_1d")

        # Persist dims / facts. Partitioned facts use dynamic overwrite by dt.
        if dt is None:
            save_table(dwd_user, "dwd/dim_user")
        else:
            # Keep existing dim on dt re-run; refresh only if missing
            dim_path = WAREHOUSE / "dwd" / "dim_user"
            if not dim_path.exists():
                save_table(dwd_user, "dwd/dim_user")

        save_partitioned(dwd_order, "dwd/fact_order", dt)
        save_partitioned(dwd_item, "dwd/fact_order_item", dt)
        save_partitioned(dws_user, "dws/user_order_1d", dt)
        save_partitioned(dws_cat, "dws/category_gmv_1d", dt)

        # ADS: on dt-scoped runs, rebuild from all warehouse partitions for consistency
        if dt is not None:
            print("[ads] rebuilding from full warehouse partitions after dt overwrite")
            rebuild_ads_from_warehouse(spark)

        ads_day = spark.table("ads_repurchase_7d").orderBy("dt")
        ads_kpi = spark.table("ads_kpi_overview")
        save_csv(ads_kpi, "ads/ads_kpi_overview.csv")
        save_csv(ads_day, "ads/ads_repurchase_7d.csv")
        save_table(ads_day, "ads/repurchase_7d")

        print("\n=== ADS kpi overview ===")
        ads_kpi.show(truncate=False)
        print("=== ADS 7-day repurchase (window_complete=1, head) ===")
        ads_day.filter("window_complete = 1").show(15, truncate=False)

        rc = reconcile(spark)
        print(f"Done. Open {WAREHOUSE / 'ads' / 'ads_kpi_overview.csv'}")
        print(f"Reconcile report: {WAREHOUSE / 'ads' / 'gmv_reconcile_report.csv'}")
        return rc
    except QualityError as e:
        print(str(e), file=sys.stderr)
        return 2
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
