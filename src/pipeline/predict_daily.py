# project/src/pipeline/predict_daily.py
from __future__ import annotations
import pandas as pd
from sqlalchemy import text
from src.db.conn import get_engine
from src.db.load_predictions import upsert_predictions
from src.models.baseline_safe import ma_next_day_series, ses_next_day_series

H = 1
MIN_HISTORY = 20
pd.options.mode.copy_on_write = True

def _tickers():
    eng = get_engine()
    with eng.connect() as conn:
        df = pd.read_sql("SELECT DISTINCT ticker FROM prices ORDER BY 1", conn)
    return df["ticker"].tolist()

def _load_prices(ticker: str) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        df = pd.read_sql(text("SELECT date, close FROM prices WHERE ticker=:t ORDER BY date"),
                         conn, params={"t": ticker})
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").dropna().reset_index(drop=True)

def _predict_one(ticker: str) -> pd.DataFrame:
    df = _load_prices(ticker)
    if len(df) < MIN_HISTORY:
        return pd.DataFrame(columns=["date","ticker","model_name","horizon","y_pred"])

    out = []
    for w in [5,10,20]:
        r = ma_next_day_series(df["close"], window=w)
        if not r.empty:
            asof = df.loc[r["asof_idx"], "date"].to_numpy()
            out.append(pd.DataFrame({
                "date": asof, "ticker": ticker,
                "model_name": f"safe_ma_w{w}", "horizon": H,
                "y_pred": r["y_pred"].to_numpy()
            }))
    for a in [0.3, 0.5]:
        r = ses_next_day_series(df["close"], alpha=a)
        if not r.empty:
            asof = df.loc[r["asof_idx"], "date"].to_numpy()
            out.append(pd.DataFrame({
                "date": asof, "ticker": ticker,
                "model_name": f"safe_ses_a{a}", "horizon": H,
                "y_pred": r["y_pred"].to_numpy()
            }))
    if not out:
        return pd.DataFrame(columns=["date","ticker","model_name","horizon","y_pred"])
    full = pd.concat(out, ignore_index=True)
    full["date"] = pd.to_datetime(full["date"]).dt.date
    return full[["date","ticker","model_name","horizon","y_pred"]]

def run(limit: int | None = None):
    tickers = _tickers()
    if limit: tickers = tickers[:limit]
    parts = []
    for t in tickers:
        try:
            df = _predict_one(t)
            if not df.empty:
                parts.append(df)
        except Exception as e:
            print(f"[WARN] {t} fail: {e}")
    if not parts:
        print("[INFO] no predictions created"); return
    pred = pd.concat(parts, ignore_index=True)
    upsert_predictions(pred)
    print(f"[INFO] predictions upserted: {len(pred)} rows")

if __name__ == "__main__":
    run()
