#!/usr/bin/env python3
"""Generate a small synthetic ODS sample for the Spark retail DW demo (offline)."""
from __future__ import annotations

import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 42
START = date(2026, 7, 1)
END = date(2026, 8, 15)

CITIES = ["哈尔滨", "北京", "上海", "深圳", "杭州", "成都"]
CHANNELS = ["app", "wechat", "web"]
PAY_CHANNELS = ["alipay", "wechat", "card"]
STATUSES_PAID = ["paid", "shipped", "completed"]
STATUSES_OTHER = ["unpaid", "cancelled"]

SKUS = [
    ("SKU01", "蓝牙耳机", "数码", 129.00),
    ("SKU02", "机械键盘", "数码", 299.00),
    ("SKU03", "纯棉T恤", "服饰", 79.00),
    ("SKU04", "牛仔裤", "服饰", 189.00),
    ("SKU05", "坚果礼盒", "食品", 68.00),
    ("SKU06", "咖啡豆", "食品", 45.00),
    ("SKU07", "香薰蜡烛", "家居", 39.00),
    ("SKU08", "四件套", "家居", 199.00),
    ("SKU09", "面霜", "美妆", 159.00),
    ("SKU10", "口红", "美妆", 99.00),
]


def main() -> None:
    rng = random.Random(SEED)
    out = Path(__file__).resolve().parents[1] / "data" / "ods"
    out.mkdir(parents=True, exist_ok=True)

    users = []
    for i in range(1, 49):
        uid = f"U{i:04d}"
        reg = START - timedelta(days=rng.randint(10, 120))
        users.append(
            {
                "user_id": uid,
                "register_dt": reg.isoformat(),
                "city": rng.choice(CITIES),
                "gender": rng.choice(["M", "F"]),
                "channel": rng.choice(CHANNELS),
            }
        )
        users[-1]["_persona"] = 0 if i <= 14 else (1 if i <= 30 else 2)

    orders = []
    items = []
    oid = 1
    iid = 1

    loyal = [u for u in users if u["_persona"] == 0]
    occasional = [u for u in users if u["_persona"] == 1]
    oneshot = [u for u in users if u["_persona"] == 2]

    for u in loyal:
        n = rng.randint(3, 5)
        t = START + timedelta(days=rng.randint(0, 12))
        for k in range(n):
            if k > 0:
                t = t + timedelta(days=rng.randint(2, 6))
            if t > END:
                break
            oid, iid = add_order(rng, u, t, oid, iid, orders, items, paid=True)

    for u in occasional:
        n = rng.randint(2, 3)
        t = START + timedelta(days=rng.randint(0, 20))
        for k in range(n):
            if k > 0:
                t = t + timedelta(days=rng.randint(8, 18))
            if t > END:
                break
            oid, iid = add_order(rng, u, t, oid, iid, orders, items, paid=True)

    for u in oneshot:
        t = START + timedelta(days=rng.randint(0, 40))
        oid, iid = add_order(rng, u, t, oid, iid, orders, items, paid=True)

    noise_users = rng.sample(users, 10)
    for u in noise_users:
        t = START + timedelta(days=rng.randint(0, 40))
        oid, iid = add_order(rng, u, t, oid, iid, orders, items, paid=False)

    apply_refunds(rng, orders, items)

    write_csv(
        out / "users.csv",
        ["user_id", "register_dt", "city", "gender", "channel"],
        [{k: v for k, v in u.items() if not k.startswith("_")} for u in users],
    )
    write_csv(
        out / "orders.csv",
        [
            "order_id",
            "user_id",
            "order_ts",
            "status",
            "pay_amount",
            "refund_amount",
            "pay_channel",
            "dt",
        ],
        orders,
    )
    write_csv(
        out / "order_items.csv",
        [
            "order_item_id",
            "order_id",
            "sku_id",
            "sku_name",
            "category",
            "qty",
            "unit_price",
            "amount",
            "refund_qty",
            "refund_amount",
        ],
        items,
    )
    n_refunded = sum(1 for o in orders if o["status"] == "refunded")
    n_partial = sum(
        1
        for o in orders
        if o["status"] in STATUSES_PAID and float(o["refund_amount"]) > 0
    )
    print(
        f"users={len(users)} orders={len(orders)} items={len(items)} "
        f"refunded={n_refunded} partial_refund={n_partial} -> {out}"
    )


def add_order(rng, user, d, oid, iid, orders, items, paid: bool):
    order_id = f"O{oid:04d}"
    hour = rng.randint(8, 22)
    minute = rng.randint(0, 59)
    ts = datetime(d.year, d.month, d.day, hour, minute, rng.randint(0, 59))
    n_items = rng.choices([1, 2, 3], weights=[0.55, 0.35, 0.10])[0]
    skus = rng.sample(SKUS, n_items)
    total = 0.0
    for sku_id, name, cat, price in skus:
        qty = rng.choice([1, 1, 1, 2])
        amount = round(qty * price, 2)
        total += amount
        items.append(
            {
                "order_item_id": f"I{iid:04d}",
                "order_id": order_id,
                "sku_id": sku_id,
                "sku_name": name,
                "category": cat,
                "qty": qty,
                "unit_price": f"{price:.2f}",
                "amount": f"{amount:.2f}",
                "refund_qty": 0,
                "refund_amount": "0.00",
            }
        )
        iid += 1
    total = round(total, 2)
    if paid:
        status = rng.choice(STATUSES_PAID)
        pay_channel = rng.choice(PAY_CHANNELS)
        pay_amount = f"{total:.2f}"
    else:
        status = rng.choice(STATUSES_OTHER)
        pay_channel = "" if status == "unpaid" else rng.choice(PAY_CHANNELS)
        pay_amount = "0.00"
    orders.append(
        {
            "order_id": order_id,
            "user_id": user["user_id"],
            "order_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "status": status,
            "pay_amount": pay_amount,
            "refund_amount": "0.00",
            "pay_channel": pay_channel,
            "dt": d.isoformat(),
        }
    )
    return oid + 1, iid


def apply_refunds(rng, orders, items) -> None:
    """Inject full refunds (status=refunded) and partial refunds on paid orders."""
    by_oid = {o["order_id"]: o for o in orders}
    items_by_oid: dict[str, list] = {}
    for it in items:
        items_by_oid.setdefault(it["order_id"], []).append(it)

    paid_oids = [
        o["order_id"]
        for o in orders
        if o["status"] in STATUSES_PAID and float(o["pay_amount"]) > 0
    ]
    rng.shuffle(paid_oids)

    # Full refunds: status -> refunded; is_paid will be 0; net_gmv = 0
    full_n = min(6, max(1, len(paid_oids) // 12))
    full_oids = set(paid_oids[:full_n])
    for oid in full_oids:
        o = by_oid[oid]
        o["status"] = "refunded"
        pay = float(o["pay_amount"])
        o["refund_amount"] = f"{pay:.2f}"
        for it in items_by_oid[oid]:
            it["refund_qty"] = int(it["qty"])
            it["refund_amount"] = it["amount"]

    # Partial refunds: keep paid/shipped/completed; reduce net GMV
    remain = [oid for oid in paid_oids if oid not in full_oids]
    partial_n = min(8, max(1, len(remain) // 10))
    for oid in remain[:partial_n]:
        o = by_oid[oid]
        its = items_by_oid[oid]
        target = its[0]
        qty = int(target["qty"])
        unit = float(target["unit_price"])
        # refund at least one unit when possible, else partial amount
        if qty >= 1:
            rq = 1 if qty == 1 else rng.choice([1] + ([1, 2] if qty >= 2 else [1]))
            rq = min(rq, qty)
            ra = round(rq * unit, 2)
        else:
            rq = 0
            ra = round(float(target["amount"]) * 0.5, 2)
        # ensure refund < amount for partial (if single-item full would equal pay)
        if ra >= float(o["pay_amount"]) and len(its) == 1 and qty == 1:
            ra = round(float(target["amount"]) * 0.5, 2)
            rq = 0
        target["refund_qty"] = rq
        target["refund_amount"] = f"{ra:.2f}"
        o["refund_amount"] = f"{ra:.2f}"


def write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)


if __name__ == "__main__":
    main()
