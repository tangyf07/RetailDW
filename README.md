# RetailDW

本地 PySpark 零售数仓演示：**ODS → DWD → DWS → ADS**，硬质量门禁、退款净 GMV、按 dt 幂等重跑、跨层 GMV 对账。

## Why

离线订单 CSV 需要可复现的分层仓：清洗定型一次（DWD）、轻汇总（DWS）、发布 7 日复购与净 GMV（ADS），脏数据失败退出，并对 DWD≡DWS≡ADS 净 GMV 可证明一致。

## Architecture

```
data/ods/*.csv          ODS  源 CSV（合成样例）
        │  quality.py   硬门禁 — 失败 exit 2，不写 DWD
        ▼
warehouse/dwd/          DWD  事实/维表；is_paid；net_gmv = pay - refund
        ▼
warehouse/dws/          DWS  user×day / category×day（净 GMV）
        ▼
warehouse/ads/          ADS  repurchase_7d + KPI + gmv_reconcile_report
```

| Layer | Role |
| --- | --- |
| ODS | 可追溯源文件 |
| DWD | 定型事实；仅 paid/shipped/completed 为 `is_paid=1`（`refunded` 不算付费） |
| DWS | KPI 用轻汇总（`order_cnt` / `paid_order_cnt`；`gross_qty`/`refund_qty`/`net_qty`） |
| ADS | 7 日复购 + GMV；`gmv_reconcile_report.csv` |

SQL 在 `sql/`；`jobs/pipeline.py` 执行。可选 `--dt YYYY-MM-DD`（`date.fromisoformat`）只覆盖该分区（`partitionOverwriteMode=dynamic`）。`dim_user` 每跑全量刷新（无 SCD2）。

## Guarantees

1. **Quality（ODS→DWD）**：PK/非空、枚举、非负金额、FK；`order_ts`/`dt` 可解析且 `DATE(order_ts)==dt`；行 `amount==qty*unit_price`（容差 0.01）；订单↔明细金额与退款校验。任一失败 → exit 2。
2. **Refund**：`net_gmv = pay_amount - refund_amount`；`status=refunded` ⇒ `is_paid=0`；明细 `net_qty = gross_qty - refund_qty`。
3. **空分区**：`--dt` 下 ODS 0 单默认 **FAIL**（exit 2）；`--allow-empty-partition` 对 parquet/CSV 同等空覆盖 `warehouse/.../dt=<dt>`。
4. **幂等 dt 重跑**：同 `--dt` 两次分区指纹一致（格式无关）；scoped 跑后 ADS 从全部仓分区重建。
5. **GMV reconcile**：DWD 付费净 GMV ≡ DWS user-day 汇总 ≡ ADS（容差 0.01）；FAIL → exit 2。
6. **Tests + CI**：pytest（质量/退款复购/窗口/幂等/对账/空分区）+ GitHub Actions（pytest → pipeline → reconcile PASS）。

## Quickstart

**Requirements:** Python 3.9+、JDK 17+（`java -version`）、约 4GB RAM。Windows 下 DWD/DWS 落 CSV 分区（无需 winutils）。

```bash
git clone https://github.com/tangyf07/RetailDW.git
cd RetailDW

# Linux / macOS
bash scripts/run_local.sh

# Windows PowerShell
.\scripts\run_local.ps1
```

成功标志：控制台 `[quality] ODS gates passed`、ADS KPI、`[reconcile] PASS`；报告 `warehouse/ads/gmv_reconcile_report.csv`。

分区重跑 / 空分区：

```bash
DT=2026-07-13 bash scripts/run_local.sh
# Windows: $env:DT='2026-07-13'; .\scripts\run_local.ps1
python jobs/pipeline.py --dt 2026-07-13
python jobs/pipeline.py --dt 2099-01-01 --allow-empty-partition   # 否则空 ODS FAIL
```

```bash
pytest
# Windows: .\.venv\Scripts\python.exe -m pytest
```

样例：`data/ods/`（users 49 / orders 124 / order_items 189 含表头；`scripts/gen_sample_data.py` 固定种子）。

**7 日复购：** `buyers(D)` = 当日 ≥1 付费单用户；`repurchase(D)` = 在 `(D, D+7]` 另有付费日；同日不计；`D+7 > max_dt` → `window_complete=0`，不进加权总览。

## Evidence

| Evidence | File |
| --- | --- |
| Terminal reconcile PASS | [`docs/evidence/reconcile-pass.png`](docs/evidence/reconcile-pass.png) |
| `gmv_reconcile_report` PASS | [`docs/evidence/gmv_reconcile_report.png`](docs/evidence/gmv_reconcile_report.png) |

![reconcile PASS terminal](docs/evidence/reconcile-pass.png)

![GMV reconcile report table](docs/evidence/gmv_reconcile_report.png)

CI：`.github/workflows/ci.yml` → `pytest` → `python jobs/pipeline.py` → 断言 reconcile 全 PASS。

## Design decisions

- **分层边界**：清洗与 `is_paid`/净 GMV 冻在 DWD；DWS 避免 ADS 反复扫明细；ADS 只表达产品口径（含窗口完整性）。
- **无 Hive/Iceberg/Airflow**：本地文件 + Spark SQL + dt 动态覆盖即可演示；生产换存储与调度。
- **dim_user 全量刷新**：样例无用户属性变更，不做 SCD2 zipper。
- **业务日取自 `order_ts`**：样例无独立支付回调时间。
- **门禁只挡 ODS→DWD**：脏行不进主题域；ADS 不静默丢行。

## Limitations

- 非生产调度/湖表；无 K8s / 多租户 / 实时链路。
- 样例体量小（笔记本可跑）；规模瓶颈在分区/倾斜/小文件，不在 SQL 形状。
- 软质量（同秒重复单、城市枚举外值等）本演示不阻断，生产宜进质量报告。

## Docs

| Doc | Path |
| --- | --- |
| Interview talking points（已移出 README） | [`docs/interview-notes.md`](docs/interview-notes.md) |
| Reconcile screenshots | [`docs/evidence/`](docs/evidence/) |
| Pipeline / quality / reconcile | `jobs/pipeline.py`, `jobs/quality.py`, `jobs/reconcile_gmv.py` |
| Spark SQL | `sql/dwd.sql`, `sql/dws.sql`, `sql/ads.sql` |
| Tests | `tests/` |
| License | [LICENSE](LICENSE) (MIT) |