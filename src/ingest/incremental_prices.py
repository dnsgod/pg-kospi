from __future__ import annotations
from datetime import date, timedelta
import pandas as pd
from sqlalchemy import text
from src.db.conn import get_engine
from src.db.io import ensure_schema, upsert_prices

pd.options.mode.copy_on_write = True

def _all_tickers() -> list[str]:
    eng = get_engine()
    with eng.connect() as c:
        df = pd.read_sql("SELECT ticker FROM tickers ORDER BY 1", c)
    return df["ticker"].tolist()

def _last_date_map() -> dict[str, date]:
    eng = get_engine()
    with eng.connect() as c:
        df = pd.read_sql("SELECT ticker, MAX(date) AS max_d FROM prices GROUP BY ticker", c)
    if df.empty: return {}
    df["max_d"] = pd.to_datetime(df["max_d"]).dt.date
    return dict(zip(df["ticker"], df["max_d"]))

def _fetch_prices_api(ticker: str, start: date, end: date) -> pd.DataFrame:
    # 필요 시 FDR/pykrx로 교체 가능. 여기선 pykrx 사용(휴장 자동 처리).
    from pykrx import stock
    fmt = "%Y%m%d"
    df = stock.get_market_ohlcv_by_date(start.strftime(fmt), end.strftime(fmt), ticker)
    if df is None or df.empty:
        return pd.DataFrame(columns=["date","open","high","low","close","volume"])
    df = df.reset_index().rename(columns={
        "날짜":"date","시가":"open","고가":"high","저가":"low","종가":"close","거래량":"volume"
    })
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[["date","open","high","low","close","volume"]].copy()
    df["adj_close"] = None
    return df

def run(limit: int | None = None):
    ensure_schema()
    tickers = _all_tickers()
    if limit:
        tickers = tickers[:int(limit)]

    last_map = _last_date_map()
    today = date.today()
    total = 0

    for t in tickers:
        start = last_map.get(t)
        if start is None:
            # 처음이면 3년 치 수집(필요 시 조정)
            start = today - timedelta(days=365*3)
        else:
            start = start + timedelta(days=1)
        if start > today:
            continue

        df = _fetch_prices_api(t, start, today)
        if df.empty:
            continue

        # change 계산(전일 종가 대비)
        prev_close = None
        if t in last_map:
            eng = get_engine()
            with eng.connect() as c:
                q = text("SELECT close FROM prices WHERE ticker=:t ORDER BY date DESC LIMIT 1")
                r = c.execute(q, {"t": t}).first()
                if r:
                    prev_close = float(r[0])

        rows = []
        for _, r in df.sort_values("date").iterrows():
            close = float(r["close"]) if r["close"] is not None else None
            chg = None
            if prev_close and close:
                chg = (close - prev_close) / prev_close
            if close:
                prev_close = close
            rows.append({
                "date": r["date"], "ticker": t,
                "open": r["open"], "high": r["high"], "low": r["low"],
                "close": r["close"], "adj_close": r["adj_close"],
                "volume": int(r["volume"]) if pd.notna(r["volume"]) else None,
                "change": chg,
            })
        total += upsert_prices(rows)
        print(f"[ingest] {t} rows={len(rows)}")

    print(f"[ingest] total upserted={total}")

if __name__ == "__main__":
    run()
