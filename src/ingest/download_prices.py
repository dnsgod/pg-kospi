import os
from datetime import datetime
import pandas as pd
import FinanceDataReader as fdr
from pykrx import stock

RAW_DIR = "data/raw"

def get_name(ticker: str) -> str:
    try:
        return stock.get_market_ticker_name(ticker)
    except:
        return None

def fetch_ohlcv_fdr(ticker, start="2015-01-01", end=None):
    df = fdr.DataReader(ticker, start, end)
    df = df.rename(columns={
        "Open":"open","High":"high","Low":"low",
        "Close":"close","Volume":"volume","Change":"change"
    })
    df["ticker"] = ticker
    df["name"] = get_name(ticker)
    df = df.reset_index().rename(columns={"Date": "date"})
    if "Adj Close" in df.columns:
        df = df.rename(columns={"Adj Close": "adj_close"})
    else:
        df["adj_close"] = None
    return df[["date","ticker","open","high","low","close","adj_close","volume","change"]]

def fetch_ohlcv_pykrx(ticker, start="20180101", end=None):
    if end is None:
        end = datetime.today().strftime("%Y%m%d")
    df = stock.get_market_ohlcv(start, end, ticker)  # index=날짜
    if df is None or df.empty:
        return pd.DataFrame(columns=["date","ticker","open","high","low","close","adj_close","volume","change"])
    df = df.rename(columns={
        "시가":"open","고가":"high","저가":"low","종가":"close","거래량":"volume"
    })
    df["ticker"] = ticker
    df["name"] = get_name(ticker)
    df["adj_close"] = None
    df["change"] = None
    df = df.reset_index().rename(columns={"날짜":"date"})
    return df[["date","ticker","open","high","low","close","adj_close","volume","change"]]

def save_parquet(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(path, index=False)
