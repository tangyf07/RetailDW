"""Helpers to load fixture ODS and build DWD/DWS/ADS temp views."""
from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]


def load_ods_dir(spark: SparkSession, ods_dir: Path):
    users = (
        spark.read.option("header", True)
        .option("nullValue", "")
        .csv(str(ods_dir / "users.csv"))
    )
    orders = (
        spark.read.option("header", True)
        .option("nullValue", "")
        .csv(str(ods_dir / "orders.csv"))
        .withColumn("pay_amount", F.col("pay_amount").cast("double"))
        .withColumn("refund_amount", F.col("refund_amount").cast("double"))
    )
    items = (
        spark.read.option("header", True)
        .option("nullValue", "")
        .csv(str(ods_dir / "order_items.csv"))
        .withColumn("qty", F.col("qty").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("refund_qty", F.col("refund_qty").cast("int"))
        .withColumn("refund_amount", F.col("refund_amount").cast("double"))
    )
    return users, orders, items


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


def build_layers(spark: SparkSession, ods_dir: Path, run_quality: bool = True):
    from quality import run_ods_gates

    users, orders, items = load_ods_dir(spark, ods_dir)
    users.createOrReplaceTempView("ods_users")
    orders.createOrReplaceTempView("ods_orders")
    items.createOrReplaceTempView("ods_order_items")
    if run_quality:
        run_ods_gates(users, orders, items)
    exec_sql_file(spark, ROOT / "sql" / "dwd.sql")
    exec_sql_file(spark, ROOT / "sql" / "dws.sql")
    exec_sql_file(spark, ROOT / "sql" / "ads.sql")
    return users, orders, items


def prepare_temp_repo(tmp: Path, ods_src: Path) -> Path:
    """Copy sql + fixture ODS into a temp tree for pipeline.main monkeypatch."""
    (tmp / "data" / "ods").mkdir(parents=True)
    (tmp / "sql").mkdir(parents=True)
    (tmp / "jobs").mkdir(parents=True)
    (tmp / "warehouse").mkdir(parents=True)
    for name in ("users.csv", "orders.csv", "order_items.csv"):
        shutil.copy(ods_src / name, tmp / "data" / "ods" / name)
    for name in ("dwd.sql", "dws.sql", "ads.sql"):
        shutil.copy(ROOT / "sql" / name, tmp / "sql" / name)
    return tmp


def partition_dir(repo: Path, rel: str, dt: str) -> Path:
    """warehouse/<rel>/dt=<dt> partition folder."""
    return repo / "warehouse" / Path(rel) / f"dt={dt}"


def partition_fingerprint(part_dir: Path) -> str:
    """Stable fingerprint of a dt partition for csv or parquet layouts.

    Windows CSV fallback writes part-00000.csv; Linux writes *.parquet with
    Spark-generated names. Fingerprint ignores ephemeral filenames for parquet
    by hashing file bytes (sorted), and uses UTF-8 text for csv.
    """
    if not part_dir.is_dir():
        raise AssertionError(f"missing partition directory: {part_dir}")

    csv_files = sorted(p for p in part_dir.glob("*.csv") if p.is_file())
    if csv_files:
        return "csv\n" + "\n---\n".join(
            p.read_text(encoding="utf-8") for p in csv_files
        )

    pq_files = [p for p in part_dir.glob("*.parquet") if p.is_file()]
    if not pq_files:
        listing = sorted(p.name for p in part_dir.iterdir())
        raise AssertionError(
            f"no csv/parquet artifacts under {part_dir}; contents={listing}"
        )

    digests = sorted(
        hashlib.sha256(p.read_bytes()).hexdigest() for p in pq_files
    )
    return "parquet\n" + "\n".join(digests)
