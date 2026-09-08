"""Hard quality gates. Any violation fails the job (non-zero exit)."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class QualityError(RuntimeError):
    pass


def _fail(name: str, n: int, hint: str = "") -> None:
    extra = f" | {hint}" if hint else ""
    raise QualityError(f"QUALITY FAIL [{name}]: {n} bad row(s){extra}")


def assert_not_null(df: DataFrame, cols: list[str], name: str) -> None:
    cond = None
    for c in cols:
        piece = F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
        cond = piece if cond is None else (cond | piece)
    n = df.filter(cond).count()
    if n:
        _fail(name, n, f"null/empty in {cols}")


def assert_unique(df: DataFrame, cols: list[str], name: str) -> None:
    n = df.groupBy(*cols).count().filter(F.col("count") > 1).count()
    if n:
        _fail(name, n, f"duplicate PK {cols}")


def assert_values_in(df: DataFrame, col: str, allowed: set[str], name: str) -> None:
    n = df.filter(~F.col(col).isin(list(allowed)) | F.col(col).isNull()).count()
    if n:
        _fail(name, n, f"{col} not in {sorted(allowed)}")


def assert_non_negative(df: DataFrame, col: str, name: str) -> None:
    n = df.filter(F.col(col).isNull() | (F.col(col) < 0)).count()
    if n:
        _fail(name, n, f"{col} < 0 or null")


def assert_positive(df: DataFrame, col: str, name: str) -> None:
    n = df.filter(F.col(col).isNull() | (F.col(col) <= 0)).count()
    if n:
        _fail(name, n, f"{col} <= 0 or null")


def assert_fk(child: DataFrame, parent: DataFrame, key: str, name: str) -> None:
    n = child.join(parent, on=key, how="left_anti").count()
    if n:
        _fail(name, n, f"orphan {key}")


def assert_amount_match(orders: DataFrame, items: DataFrame, name: str) -> None:
    """Paid / refunded headers: pay_amount must equal sum(item.amount) within 0.01."""
    checked = orders.filter(
        F.col("status").isin("paid", "shipped", "completed", "refunded")
    )
    summed = items.groupBy("order_id").agg(F.round(F.sum("amount"), 2).alias("item_sum"))
    bad = (
        checked.join(summed, "order_id", "left")
        .filter(
            F.col("item_sum").isNull()
            | (F.abs(F.col("pay_amount") - F.col("item_sum")) > 0.01)
        )
    )
    n = bad.count()
    if n:
        _fail(name, n, "pay_amount != sum(order_items.amount)")


def assert_item_amount_eq_qty_price(items: DataFrame, name: str) -> None:
    """Each line: amount == qty * unit_price within 0.01."""
    bad = items.filter(
        F.col("amount").isNull()
        | F.col("qty").isNull()
        | F.col("unit_price").isNull()
        | (F.abs(F.col("amount") - F.col("qty") * F.col("unit_price")) > 0.01)
    )
    n = bad.count()
    if n:
        _fail(name, n, "amount != qty * unit_price")


def assert_order_ts_dt_parseable_and_aligned(orders: DataFrame) -> None:
    """order_ts and dt must parse; DATE(order_ts) must equal dt."""
    ts = F.to_timestamp(F.col("order_ts"))
    dt = F.to_date(F.col("dt"))
    n = orders.filter(ts.isNull()).count()
    if n:
        _fail("ods_orders.order_ts_parseable", n, "order_ts not parseable as timestamp")
    n = orders.filter(dt.isNull()).count()
    if n:
        _fail("ods_orders.dt_parseable", n, "dt not parseable as date")
    n = orders.filter(F.to_date(ts) != dt).count()
    if n:
        _fail("ods_orders.order_ts_dt_align", n, "DATE(order_ts) != dt")


def assert_refund_rules(orders: DataFrame, items: DataFrame) -> None:
    """refund_amount/qty >= 0, refund <= original, header matches item sum, refunded is full."""
    assert_non_negative(orders, "refund_amount", "ods_orders.refund_amount")
    assert_non_negative(items, "refund_qty", "ods_items.refund_qty")
    assert_non_negative(items, "refund_amount", "ods_items.refund_amount")

    n = items.filter(F.col("refund_qty") > F.col("qty")).count()
    if n:
        _fail("ods_items.refund_qty_lte_qty", n, "refund_qty > qty")

    n = items.filter(F.col("refund_amount") - F.col("amount") > 0.01).count()
    if n:
        _fail("ods_items.refund_amount_lte_amount", n, "refund_amount > amount")

    n = orders.filter(F.col("refund_amount") - F.col("pay_amount") > 0.01).count()
    if n:
        _fail("ods_orders.refund_lte_pay", n, "refund_amount > pay_amount")

    refund_sum = items.groupBy("order_id").agg(
        F.round(F.sum("refund_amount"), 2).alias("item_refund_sum")
    )
    bad = (
        orders.join(refund_sum, "order_id", "left")
        .fillna({"item_refund_sum": 0.0})
        .filter(F.abs(F.col("refund_amount") - F.col("item_refund_sum")) > 0.01)
    )
    n = bad.count()
    if n:
        _fail(
            "ods_orders.refund_vs_items",
            n,
            "refund_amount != sum(order_items.refund_amount)",
        )

    # Fully refunded status: refund_amount must equal original pay_amount
    bad_full = orders.filter(
        (F.col("status") == "refunded")
        & (F.abs(F.col("pay_amount") - F.col("refund_amount")) > 0.01)
    )
    n = bad_full.count()
    if n:
        _fail("ods_orders.refunded_full", n, "refunded requires refund_amount == pay_amount")


def run_ods_gates(users: DataFrame, orders: DataFrame, items: DataFrame) -> None:
    assert_not_null(users, ["user_id", "register_dt", "city", "gender", "channel"], "ods_users.required")
    assert_unique(users, ["user_id"], "ods_users.pk")
    assert_values_in(users, "gender", {"M", "F"}, "ods_users.gender")
    assert_values_in(users, "channel", {"app", "wechat", "web"}, "ods_users.channel")

    assert_not_null(
        orders,
        ["order_id", "user_id", "order_ts", "status", "pay_amount", "refund_amount", "dt"],
        "ods_orders.required",
    )
    assert_unique(orders, ["order_id"], "ods_orders.pk")
    assert_values_in(
        orders,
        "status",
        {"unpaid", "paid", "shipped", "completed", "cancelled", "refunded"},
        "ods_orders.status",
    )
    assert_non_negative(orders, "pay_amount", "ods_orders.pay_amount")
    assert_order_ts_dt_parseable_and_aligned(orders)
    assert_fk(orders, users, "user_id", "ods_orders.fk_user")

    assert_not_null(
        items,
        [
            "order_item_id",
            "order_id",
            "sku_id",
            "category",
            "qty",
            "unit_price",
            "amount",
            "refund_qty",
            "refund_amount",
        ],
        "ods_items.required",
    )
    assert_unique(items, ["order_item_id"], "ods_items.pk")
    assert_positive(items, "qty", "ods_items.qty")
    assert_non_negative(items, "unit_price", "ods_items.unit_price")
    assert_non_negative(items, "amount", "ods_items.amount")
    assert_item_amount_eq_qty_price(items, "ods_items.amount_eq_qty_price")
    assert_fk(items, orders, "order_id", "ods_items.fk_order")
    assert_amount_match(orders, items, "ods_orders.amount_vs_items")
    assert_refund_rules(orders, items)

    print("[quality] ODS gates passed")
