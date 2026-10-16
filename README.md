# db-cnflow-0 — A股增量维度数据仓 (北向资金 + 两融)

## 数据源 (均为东财 datacenter-web 公开报表接口)
| 数据集 | 接口 | 字段映射 | 覆盖 |
|---|---|---|---|
| 北向历史 | `datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPT_MUTUAL_DEAL_HISTORY` MUTUAL_TYPE=001(沪股通)/002(深股通) | TRADE_DATE→date, FUND_INFLOW→fund_inflow, NET_DEAL_AMT→net_deal_amt, QUOTA_BALANCE→quota_balance, BUY_AMT/SELL_AMT, HOLD_MARKET_CAP→hold_market_cap (单位: 亿元, 持仓市值元) | 2014-11-17 起, 全史 5500 行 (沪 2769 + 深 2731) |
| 两融合计 | 同上 reportName=RPTA_RZRQ_LSHJ | DIM_DATE→date, NEW→index_close, RZYE→rzye(融资余额), RZMRE→rzmre(融资买入额), RZCHE→rzche(融资偿还额) | 2010-03-31 起, 全史 4015 行 |

## 已知数据空洞 (源端如此)
- 北向 `fund_inflow/quota_balance`: 2024-08 之后交易所停止披露 → 源端为 null (覆盖率: 2014-11~2024-07 ~99%, 2024-08 起 0)。
- 北向 `net_deal_amt/buy_amt/sell_amt/hold_market_cap`: 持续可用至 2026-10-15。
- 两融 RZCHE 2010 年早期部分缺失。

## 目录
- `cnflow_worker.py` — 全量刷新 worker (每次跑拉全史并 upsert)
- `wh/north_hist/<year>.parquet` — type(north_sh/north_sz), date, fund_inflow, net_deal_amt, quota_balance, buy_amt, sell_amt, hold_market_cap
- `wh/margin_hist/<year>.parquet` — date, index_close, rzye, rzmre, rzche
- `.github/workflows/backfill.yml` — 每 60 分钟全量刷新一次
