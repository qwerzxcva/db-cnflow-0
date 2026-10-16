#!/usr/bin/env python3
"""cnflow_worker.py — A股增量维度 worker: 北向资金历史 + 两融市场合计历史
数据源: 东方财富 datacenter-web (公开报表)
  北向: RPT_MUTUAL_DEAL_HISTORY, MUTUAL_TYPE=001(沪股通)/002(深股通), 全史 2014-11 起
  两融: RPTA_RZRQ_LSHJ, 沪深两市合计, 全史 2010-03 起
模式: 全量刷新 (每源几千行, 一次拉完), upsert 到 parquet。
输出:
  wh/north_hist/<year>.parquet  字段: type,date,fund_inflow,net_deal_amt,quota_balance,buy_amt,sell_amt,hold_market_cap
  wh/margin_hist/<year>.parquet 字段: date,index_close,rzye,rzmre,rzche
"""
import json, os, time, pathlib
import urllib.request

UA = {"User-Agent": "Mozilla/5.0", "Referer": "https://data.eastmoney.com/"}
NORTH_COLS = ["type","date","fund_inflow","net_deal_amt","quota_balance","buy_amt","sell_amt","hold_market_cap"]
MARGIN_COLS = ["date","index_close","rzye","rzmre","rzche"]

def get(url, timeout=30):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "ignore")

def fetch_pages(base, pages):
    out = []
    for p in range(1, pages + 1):
        d = json.loads(get(base.format(p=p)))
        r0 = d.get("result") or {}
        out += r0.get("data") or []
        if p == 1:
            pages = min(pages, r0.get("pages") or 1)
        time.sleep(0.2)
    return out

def write_partition(sub, cols, year, rows):
    import pyarrow.parquet as pq, pyarrow as pa
    pathlib.Path(f"wh/{sub}").mkdir(parents=True, exist_ok=True)
    path = f"wh/{sub}/{year}.parquet"
    old = {}
    if os.path.exists(path):
        t = pq.read_table(path)
        for row in zip(*[t.column(c).to_pylist() for c in t.column_names]):
            old[(row[0], row[1])] = list(row)
    for r in rows:
        old[(r[0], r[1])] = r
    rows = sorted(old.values(), key=lambda r: (r[0], r[1]))
    pq.write_table(pa.table({c: [r[i] for r in rows] for i, c in enumerate(cols)}),
                   path, compression="zstd")

def fnum(x):
    try: return float(x)
    except Exception: return None

def north():
    base = ("https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPT_MUTUAL_DEAL_HISTORY"
            "&columns=ALL&pageSize=500&pageNumber={p}&filter=(MUTUAL_TYPE%3D%22{mt}%22)"
            "&sortColumns=TRADE_DATE&sortTypes=-1&source=WEB&client=WEB")
    rows = []
    for mt, name in [("001", "north_sh"), ("002", "north_sz")]:
        b = base.replace("{mt}", mt)
        d = json.loads(get(b.format(p=1)))
        r0 = d.get("result") or {}
        pages = r0.get("pages") or 1
        arr = r0.get("data") or []
        for p in range(2, pages + 1):
            d2 = json.loads(get(b.format(p=p)))
            arr += (d2.get("result") or {}).get("data") or []
            time.sleep(0.2)
        for a in arr:
            if a.get("MUTUAL_TYPE") != mt: continue
            dt = (a.get("TRADE_DATE") or "")[:10]
            if not dt: continue
            rows.append([name, dt, fnum(a.get("FUND_INFLOW")), fnum(a.get("NET_DEAL_AMT")),
                         fnum(a.get("QUOTA_BALANCE")), fnum(a.get("BUY_AMT")), fnum(a.get("SELL_AMT")),
                         fnum(a.get("HOLD_MARKET_CAP"))])
    return rows

def margin():
    base = ("https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPTA_RZRQ_LSHJ"
            "&columns=ALL&pageSize=800&pageNumber={p}&sortColumns=DIM_DATE&sortTypes=-1&source=WEB&client=WEB")
    d = json.loads(get(base.format(p=1)))
    r0 = d.get("result") or {}
    arr = r0.get("data") or []
    pages = r0.get("pages") or 1
    for p in range(2, pages + 1):
        d2 = json.loads(get(base.format(p=p)))
        arr += (d2.get("result") or {}).get("data") or []
        time.sleep(0.2)
    rows = []
    for a in arr:
        dt = (a.get("DIM_DATE") or "")[:10]
        if not dt: continue
        rows.append([dt, fnum(a.get("NEW")), fnum(a.get("RZYE")), fnum(a.get("RZMRE")), fnum(a.get("RZCHE"))])
    return rows

def main():
    import pyarrow  # fail fast
    n = north()
    print(f"north rows: {len(n)}", flush=True)
    for y in sorted({r[1][:4] for r in n}):
        write_partition("north_hist", NORTH_COLS, y, [r for r in n if r[1].startswith(y)])
    m = margin()
    print(f"margin rows: {len(m)}", flush=True)
    for y in sorted({r[0][:4] for r in m}):
        write_partition("margin_hist", MARGIN_COLS, y, [r for r in m if r[0].startswith(y)])

if __name__ == "__main__":
    main()
