# spark-retail-dw

秋招数据开发 Demo：用 **PySpark local** 把一份离线合成零售订单跑通 **ODS → DWD → DWS → ADS**，质量门失败即停，核心指标是 **7 日复购率**（附 GMV）。

克隆后按下面命令跑，几分钟内应打印 `ads_kpi_overview` 并写出 `warehouse/ads/*.csv`。

## 30 秒怎么讲

- 数据：合成电商订单（48 用户 / 123 单 / 188 明细，2026-07-01～2026-08-15），仓库内 CSV，可离线。
- 分层：贴源 ODS → 清洗事实/维度 DWD → 用户日/类目日轻度汇总 DWS → 指标 ADS。
- 质量：主键、非空、枚举、外键、订单金额=明细合计，任一失败 exit 2。
- 指标：观察日 D 有已支付订单的用户中，在 (D, D+7] 再次支付的比例；不完整窗口单独标记。

## 环境

- Python 3.9+
- JDK 17+（`java -version` 能跑；PySpark 需要）
- Windows：Spark 写 parquet 需要 Hadoop winutils + native IO。本 Demo 在 Windows 上把 DWD/DWS 落成 CSV part 文件，ADS KPI 始终用 Python 写出，clone 即可跑通。
- 内存 4GB 足够（local 小样本）
- 首次 `pip install pyspark` 会下载 Spark（约 400MB），之后跑 pipeline 约 1–2 分钟

## 怎么跑

```bash
git clone https://github.com/tangyf07/RetailDW.git
cd spark-retail-dw

# Linux / macOS
bash scripts/run_local.sh

# Windows PowerShell
.\scripts\run_local.ps1
```

或手动：

```bash
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# Windows: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python jobs/pipeline.py
```

成功时看：

- 控制台：`[quality] ODS gates passed`，然后 `ADS kpi overview`
- 文件：`warehouse/ads/ads_kpi_overview.csv`、`warehouse/ads/ads_repurchase_7d.csv`

## 数据从哪来、多大规模

| 表 | 路径 | 行数（含表头） | 粒度 |
| --- | --- | --- | --- |
| 用户 | `data/ods/users.csv` | 49 | 用户 |
| 订单 | `data/ods/orders.csv` | 124 | 一单一行 |
| 明细 | `data/ods/order_items.csv` | 189 | 一行一个 SKU |

- **来源**：`scripts/gen_sample_data.py` 用固定种子生成的合成数据（城市含哈尔滨及北上深杭成），**不是**业务库导出，也不是课程 IoT 数据。
- **为什么合成**：面试可复现、无隐私、口径可控；样本刻意做了「7 日内复购 / 7 日外再购 / 只买一次 / 取消单」几种人，方便讲清楚指标。
- 需要重生成：`python scripts/gen_sample_data.py`（会覆盖 `data/ods/`）。

## 分层为什么这样切

```
data/ods/*.csv          ODS  原样接入，字段名与文件一致
        │  quality.py   硬门槛，不过不写下游
        ▼
warehouse/dwd/          DWD  类型、支付标记 is_paid、维表
        ▼
warehouse/dws/          DWS  user×day、category×day，避免 ADS 重复扫明细
        ▼
warehouse/ads/          ADS  复购率 + KPI 总览（CSV 给面试官直接打开）
```

| 层 | 职责 | 为什么独立 |
| --- | --- | --- |
| ODS | 贴源、可追溯 | 出了问题能对回文件，不把清洗写死在源头 |
| DWD | 清洗 + 统一粒度 | 事实表一单一行、明细一行一件；`is_paid` 在这层定死，下游口径一致 |
| DWS | 轻度汇总 | 复购、GMV、类目排行都吃 user-day / category-day，不必每次 join 明细 |
| ADS | 面向应用的指标 | 面试/看板只看这一张；窗口是否完整是产品口径，不属于 DWD |

SQL 在 `sql/dwd.sql`、`sql/dws.sql`、`sql/ads.sql`，`jobs/pipeline.py` 按文件执行，方便对着 SQL 讲，而不是翻 Python。

## 质量门怎么工作

`jobs/quality.py` 在 **写 DWD 之前** 跑，全部是硬规则（失败即停）：

1. 主键非空且唯一：`user_id` / `order_id` / `order_item_id`
2. 必填字段非空
3. 枚举：`gender`、`channel`、`status`
4. 范围：`pay_amount/unit_price/amount >= 0`，`qty > 0`
5. 外键：订单用户必须在用户表；明细订单必须在订单表
6. 金额勾稽：已支付订单 `pay_amount` 必须等于明细 `amount` 之和（误差 0.01）

演示失败：把 `data/ods/orders.csv` 某行 `pay_amount` 改成负数再跑，应看到 `QUALITY FAIL` 且不写 ADS。

软规则（本 Demo 没做成阻断）：同一用户同一秒重复单、城市维值未在枚举里——面试可以补一句「生产会入质量报表而不是杀任务」。

## 核心指标口径

**7 日复购率（按观察日 D）**

```
buyers(D)      = 当天有 ≥1 笔已支付订单的用户
repurchase(D)  = buyers(D) 中，在 D+1～D+7 任意一天再次有已支付订单的用户
rate(D)        = repurchase(D) / buyers(D)
```

- **已支付**：`status ∈ {paid, shipped, completed}`；`unpaid/cancelled` 不进 GMV、不进复购。
- **同一天多单不算复购**（间隔必须跨日）。这是常见零售口径，避免「拆单」抬高复购。
- **不完整窗口**：样本最大日为 `max_dt`，若 `D+7 > max_dt` 则 `window_complete=0`。总览 KPI 只用完整窗口做加权：`sum(repurchase_users) / sum(buyers)`。
- **GMV**：已支付订单 `pay_amount` 之和（与明细勾稽过）。

总览字段见 `warehouse/ads/ads_kpi_overview.csv`：`paid_orders`、`paid_users`、`gmv_total`、`repurchase_rate_7d_weighted`。

## 做了哪些取舍（面试追问）

- 不用 Hive/Iceberg：local 文件 + parquet 足够演示分层和幂等 `overwrite`；生产会换成分区表 + 任务调度。
- 不用维度拉链：样本无用户属性变更，用户维是切片。
- 复购看「订单日」不看「支付回调日」：样本只有 `order_ts`。
- 质量放 ODS→DWD 之间：脏数据不允许进主题层；ADS 不再做一次静默 drop。
- 样本故意偏小：保证笔记本几分钟跑完；逻辑与日千万级订单相同，瓶颈会变成分区、倾斜和文件大小，而不是 SQL。

## 目录

```
data/ods/                 离线样本 CSV
jobs/pipeline.py          入口
jobs/quality.py           质量门
sql/                      DWD / DWS / ADS Spark SQL
scripts/run_local.sh      Linux/macOS
scripts/run_local.ps1     Windows
scripts/gen_sample_data.py
warehouse/                运行产出（git 忽略）
```

## 面试 2 分钟讲法

1. 「这是一个可跑的四层数仓，不是空架构图。数据在仓库里，clone 就能出 7 日复购率。」
2. 「ODS 只负责接入；质量门卡主键、外键和订单-明细金额；DWD 固化 is_paid；DWS 出用户日，避免 ADS 重复扫明细。」
3. 「复购率分母是当天支付用户，分子是 7 日内再次支付；同一天不算；最后 7 天窗口不完整所以不算进总 KPI。」
4. 打开 `ads_kpi_overview.csv` 指一行加权复购率和 GMV。
