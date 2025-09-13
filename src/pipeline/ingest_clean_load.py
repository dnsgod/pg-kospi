from datetime import date
import os, pandas as pd
from src.ingest.get_kospi100 import get_kospi100_tickers
from src.ingest.download_prices import fetch_ohlcv_fdr, fetch_ohlcv_pykrx, save_parquet
from src.clean.clean_prices import clean_one
from src.db.load_prices import upsert_prices

RAW_DIR = "data/raw"
CLEAN_DIR = "data/clean"

def run_day1(start="2018-01-01", end=None, limit=None):
    # 1) 티커
    tickers = get_kospi100_tickers()
    if not tickers:
        print("[ERROR] KOSPI100 tickers empty. Check get_kospi100_tickers().")
        return
    if limit:
        tickers = tickers[:limit]
    print(f"[INFO] tickers={len(tickers)} (e.g., {tickers[:5]})")

    # 2) 수집 (FDR 우선, 실패시 pykrx 대체)
    all_raw = []
    for t in tickers:
        try:
            df = fetch_ohlcv_fdr(t, start, end)
            if df.empty:
                raise RuntimeError("FDR returned empty")
        except Exception as e:
            print(f"[WARN] FDR failed {t}: {e} -> fallback pykrx")
            # pykrx는 YYYYMMDD
            start_krx = start.replace("-", "")
            end_krx = None if end is None else end.replace("-", "")
            df = fetch_ohlcv_pykrx(t, start_krx, end_krx)

        if df is None or df.empty:
            print(f"[WARN] no data for {t} (both FDR and pykrx)")
            continue

        save_parquet(df, f"{RAW_DIR}/{t}.parquet")
        all_raw.append(df)

    if not all_raw:
        print("No data fetched.")
        return

    # 3) 정제
    os.makedirs(CLEAN_DIR, exist_ok=True)
    all_clean = []
    for t in {d['ticker'].iloc[0] for d in all_raw if not d.empty}:
        path = f"{RAW_DIR}/{t}.parquet"
        dfc = clean_one(path)
        dfc.to_parquet(f"{CLEAN_DIR}/{t}.parquet", index=False)
        all_clean.append(dfc)
    full_clean = pd.concat(all_clean, ignore_index=True)
    full_clean.to_parquet(f"{CLEAN_DIR}/KOSPI100_all.parquet", index=False)

    # 4) DB 적재
    upsert_prices(full_clean)
    print(f"Done. rows={len(full_clean)}")
