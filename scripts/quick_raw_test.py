# scripts/quick_raw_test.py
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from src.ingest.get_kospi100 import get_kospi100_tickers
from src.ingest.download_prices import fetch_ohlcv_fdr, fetch_ohlcv_pykrx
from src.clean.clean_prices import clean_one
from src.db.load_prices import upsert_prices
import pandas as pd, os

# 1) 티커 5개만 샘플
tickers = get_kospi100_tickers()[:5]
print("[INFO] sample tickers:", tickers)

# 2) 수집 (FDR 우선, 실패 시 pykrx 대체) → data/raw/*.parquet 저장
os.makedirs("data/raw", exist_ok=True)
raws = []
for t in tickers:
    try:
        df = fetch_ohlcv_fdr(t, "2018-01-01", None)
        if df.empty:
            raise RuntimeError("FDR returned empty")
        print(f"[OK] FDR {t} rows={len(df)}")
    except Exception as e:
        print(f"[WARN] FDR failed for {t}: {e} -> fallback pykrx")
        df = fetch_ohlcv_pykrx(t, "20180101", None)
        print(f"[OK] pykrx {t} rows={len(df)}")
    df.to_parquet(f"data/raw/{t}.parquet", index=False)
    raws.append(df)

assert raws, "no raw frames written"

# 3) 정제 → data/clean/*.parquet 및 합본
os.makedirs("data/clean", exist_ok=True)
cleans = []
for t in tickers:
    dfc = clean_one(f"data/raw/{t}.parquet")
    print(f"[CLEAN] {t} rows={len(dfc)}")
    dfc.to_parquet(f"data/clean/{t}.parquet", index=False)
    cleans.append(dfc)
full_clean = pd.concat(cleans, ignore_index=True)
full_clean.to_parquet("data/clean/KOSPI100_all.parquet", index=False)
print("[FULL_CLEAN] rows:", len(full_clean), "cols:", list(full_clean.columns))

# 4) DB UPSERT
upsert_prices(full_clean)
print("[UPSERT] done.")
