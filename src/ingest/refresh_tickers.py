# src/ingest/refresh_tickers.py
from __future__ import annotations
import time
import pandas as pd
from sqlalchemy import text
from src.db.conn import get_engine

# pykrx 의존
from pykrx import stock as krx

pd.options.mode.copy_on_write = True

def _fetch_kospi200_codes() -> list[str]:
    # KOSPI200: 1028
    return list(krx.get_index_portfolio_deposit_file("1028"))

def _name_safe(code: str) -> str:
    # 이름 조회 실패 시 코드로 대체
    try:
        return krx.get_market_ticker_name(code) or code
    except Exception:
        return code

def run() -> None:
    t0 = time.time()
    print("[refresh] start")

    codes = _fetch_kospi200_codes()
    print(f"[refresh] codes={len(codes)} fetched in {time.time()-t0:.2f}s")

    # 이름 조회 (개별 호출이지만 보통 수초~수십초 내 끝남)
    t1 = time.time()
    names = [_name_safe(c) for c in codes]
    print(f"[refresh] names via pykrx in {time.time()-t1:.2f}s")

    df = pd.DataFrame({"ticker": codes, "name": names})
    df["market"] = "KOSPI"
    df["sector"] = None

    eng = get_engine()
    sql = """
    INSERT INTO tickers (ticker, name, market, sector, updated_at)
    VALUES (:ticker, :name, :market, :sector, now())
    ON CONFLICT (ticker) DO UPDATE
      SET name = EXCLUDED.name,
          market = COALESCE(EXCLUDED.market, tickers.market),
          sector = COALESCE(EXCLUDED.sector, tickers.sector),
          updated_at = now();
    """

    rows = df.to_dict("records")
    t2 = time.time()
    with eng.begin() as conn:
        # 배치 업서트
        B = 1000
        for i in range(0, len(rows), B):
            conn.execute(text(sql), rows[i:i+B])
    print(f"[refresh] upserted {len(rows)} tickers in {time.time()-t2:.2f}s")

    print(f"[refresh] done in {time.time()-t0:.2f}s")

if __name__ == "__main__":
    run()
