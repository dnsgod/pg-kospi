# project/src/pipeline/ingest_daily.py
from __future__ import annotations
import argparse, pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from src.db.conn import get_engine
from src.ingest.get_kospi100 import get_kospi100_tickers
from src.ingest.download_prices import fetch_ohlcv_fdr, fetch_ohlcv_pykrx
from src.clean.clean_prices import clean_one
from src.db.load_prices import upsert_prices

def _load_targets() -> list[str]:
    # 정책: watchlist 있으면 우선, 없으면 KOSPI100
    eng = get_engine()
    with eng.connect() as conn:
        wl = pd.read_sql("SELECT ticker FROM watchlist ORDER BY created_at DESC", conn)
    if not wl.empty:
        return wl["ticker"].tolist()
    return get_kospi100_tickers()

def _load_last_date_map(tickers: list[str]) -> dict[str, datetime | None]:
    eng = get_engine()
    sql = text("""
      SELECT ticker, MAX(date) AS last_date
      FROM prices
      WHERE ticker = ANY(:arr)
      GROUP BY ticker
    """)
    with eng.connect() as conn:
        rows = conn.execute(sql, {"arr": tickers}).fetchall()
    mp = {t: None for t in tickers}
    for t, d in rows:
        mp[t] = pd.Timestamp(d).date() if d else None
    return mp

def _fetch_incremental(ticker: str, start: str, end: str) -> pd.DataFrame:
    # 1차: FDR, 실패/빈 DF면 pykrx 폴백
    try:
        df = fetch_ohlcv_fdr(ticker, start=start, end=end)
        if df is None or df.empty:
            raise RuntimeError("empty from FDR")
    except Exception:
        s_krx = start.replace("-", "")
        e_krx = end.replace("-", "")
        df = fetch_ohlcv_pykrx(ticker, start=s_krx, end=e_krx)
    return df if df is not None else pd.DataFrame()

def main(since: str | None, dry_run: bool, only: list[str] | None):
    targets = only or _load_targets()
    if not targets:
        print("[END] no target tickers")
        return

    # 각 티커의 마지막 적재일 + 1일부터 오늘까지
    last_map = _load_last_date_map(targets)
    today = pd.Timestamp.today(tz="Asia/Seoul").date()
    total_rows = 0; ok = skip = fail = 0

    for t in targets:
        # 시작일 결정
        if since:
            start = pd.to_datetime(since).date()
        elif last_map[t] is None:
            # 최초 실행 시 오늘 하루만 (원하면 과거부터 수집하도록 옵션 확장 가능)
            start = today
        else:
            start = last_map[t] + timedelta(days=1)

        if start > today:
            print(f"[SKIP] {t} no new trading day")
            skip += 1; continue

        s = start.strftime("%Y-%m-%d"); e = today.strftime("%Y-%m-%d")
        try:
            raw = _fetch_incremental(t, s, e)
            if raw.empty:
                print(f"[SKIP] {t} fetched=0")
                skip += 1; continue

            # 증분 데이터만 정제: clean_one은 parquet 경로 입력이므로 여기선 간단 정제를 직접 호출 대체
            # => clean_one을 경로 없이도 사용할 수 있도록 바꾸지 않고, 최소한의 필터만 적용
            raw = raw.drop_duplicates(subset=["date","ticker"])
            raw["date"] = pd.to_datetime(raw["date"]).dt.date
            raw = raw[raw["volume"].fillna(0) >= 0]

            if dry_run:
                print(f"[TICKER] {t} plan={s}..{e} fetched={len(raw)} dry-run")
                ok += 1; continue

            ins_before = total_rows
            upsert_prices(raw)      # (ticker,date) UPSERT
            total_rows += len(raw)
            print(f"[TICKER] {t} plan={s}..{e} upserted≈{len(raw)}")
            ok += 1
        except Exception as ex:
            print(f"[FAIL] {t} plan={s}..{e} err={ex}")
            fail += 1

    print(f"[SUMMARY] tickers={len(targets)} ok={ok} skip={skip} fail={fail} rows≈{total_rows}")
    print("[END] ingest_daily done")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", type=str, default=None, help="YYYY-MM-DD (override start date)")
    ap.add_argument("--tickers", type=str, default=None, help="comma separated tickers")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    only = [x.strip() for x in args.tickers.split(",")] if args.tickers else None
    main(args.since, args.dry_run, only)
