from __future__ import annotations
import argparse
from datetime import date
from typing import Optional, Tuple
import pandas as pd
from sqlalchemy import text
from src.db.conn import get_engine
from src.db.io import upsert_predictions
from src.models.baseline_safe import ma_next_day_series, ses_next_day_series

try:
    from src.models.dl_lstm import predict_next_day_close, DLNotAvailable  # type: ignore
    _DL_OK = True
except Exception:
    _DL_OK = False

H = 1
MIN_SAFE = 20
MIN_DL = 120
DL_PARAMS = {"window": 20, "epochs": 12, "batch_size": 32, "patience": 3}
pd.options.mode.copy_on_write = True

def _all_tickers() -> list[str]:
    eng = get_engine()
    with eng.connect() as c:
        df = pd.read_sql("SELECT ticker FROM tickers ORDER BY 1", c)
    return df["ticker"].tolist()

def _prices(ticker: str) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as c:
        df = pd.read_sql(text("SELECT date, close FROM prices WHERE ticker=:t ORDER BY date"),
                         c, params={"t": ticker})
    if df.empty: return df
    df["date"] = pd.to_datetime(df["date"])
    return df.dropna().reset_index(drop=True)

def _last_map(ticker: str) -> dict[str, date]:
    eng = get_engine()
    with eng.connect() as c:
        df = pd.read_sql(text("""
            SELECT model_name, MAX(date) AS last_date
            FROM predictions
            WHERE ticker=:t AND horizon=:h
            GROUP BY model_name
        """), c, params={"t": ticker, "h": H})
    if df.empty: return {}
    df["last_date"] = pd.to_datetime(df["last_date"]).dt.date
    return dict(zip(df["model_name"], df["last_date"]))

def _safe_frames(df: pd.DataFrame, ticker: str) -> list[pd.DataFrame]:
    frames = []
    for w in (5, 10, 20):
        res = ma_next_day_series(df["close"], window=w)
        if res.empty: continue
        asof_dates = df.loc[res["asof_idx"], "date"].dt.date.to_numpy()
        frames.append(pd.DataFrame({
            "date": asof_dates, "ticker": ticker,
            "model_name": f"safe_ma_w{w}", "horizon": H,
            "y_pred": res["y_pred"].to_numpy()
        }))
    for a in (0.3, 0.5):
        res = ses_next_day_series(df["close"], alpha=a)
        if res.empty: continue
        asof_dates = df.loc[res["asof_idx"], "date"].dt.date.to_numpy()
        frames.append(pd.DataFrame({
            "date": asof_dates, "ticker": ticker,
            "model_name": f"safe_ses_a{a}", "horizon": H,
            "y_pred": res["y_pred"].to_numpy()
        }))
    return frames

def _dl_frame(df: pd.DataFrame, ticker: str) -> Optional[pd.DataFrame]:
    if not _DL_OK or len(df) < MIN_DL:
        return None
    try:
        yhat = predict_next_day_close(df["close"].to_numpy(), **DL_PARAMS)
        asof = df["date"].iloc[-1].date()
        return pd.DataFrame([{
            "date": asof, "ticker": ticker,
            "model_name": "safe_dl_lstm_v1", "horizon": H,
            "y_pred": float(yhat)
        }])
    except Exception as e:
        print(f"[DL warn] {ticker}: {e}")
        return None

def build_no_leak(ticker: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = _prices(ticker)
    if df.empty or len(df) < MIN_SAFE:
        return (pd.DataFrame(columns=["date","ticker","model_name","horizon","y_pred"]), pd.DataFrame())
    frames = _safe_frames(df, ticker)
    dl = _dl_frame(df, ticker)
    if dl is not None and not dl.empty:
        frames.append(dl)
    if not frames:
        return (pd.DataFrame(columns=["date","ticker","model_name","horizon","y_pred"]), pd.DataFrame())
    full = pd.concat(frames, ignore_index=True)
    last = _last_map(ticker)
    if last:
        mask = []
        for _, r in full.iterrows():
            d0 = last.get(r["model_name"])
            mask.append(True if d0 is None else (r["date"] > d0))
        full = full[pd.Series(mask, index=full.index)]
    return full[["date","ticker","model_name","horizon","y_pred"]], full

def run(limit: Optional[int] = None, no_dl: bool = False):
    global _DL_OK
    if no_dl:
        _DL_OK = False

    tickers = _all_tickers()
    if limit: tickers = tickers[:int(limit)]

    to_save = []
    for t in tickers:
        try:
            s, _ = build_no_leak(t)
            if not s.empty:
                to_save.extend(s.to_dict("records"))
        except Exception as e:
            print(f"[predict warn] {t}: {e}")

    n = upsert_predictions(to_save)
    print(f"[predict] upserted rows={n}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--no-dl", action="store_true")
    args = ap.parse_args()
    run(limit=args.limit, no_dl=args.no_dl)
