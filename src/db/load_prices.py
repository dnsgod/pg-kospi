import pandas as pd
from sqlalchemy import Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from .conn import get_engine

def upsert_prices(df: pd.DataFrame, chunk=5000):
    engine = get_engine()
    md = MetaData()
    prices = Table("prices", md, autoload_with=engine)

    if "name" not in df.columns:
        try:
            from pykrx import stock
            name_map = {t: stock.get_market_ticker_name(t) for t in df["ticker"].dropna().unique()}
            df = df.copy()
            df["name"] = df["ticker"].map(name_map)
        except Exception:
            # pykrx 조회 실패 등 예외 시 None으로 채우고 계속 진행
            df = df.copy()
            df["name"] = None

    cols = ["date","ticker","name","open","high","low","close","adj_close","volume","change"]
    df = df[cols].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date

    with engine.begin() as conn:
        for i in range (0, len(df), chunk):
            part = df.iloc[i:i+chunk]
            if part.empty:
                continue
            recs = part.to_dict(orient="records")
            stmt = insert(prices).values(recs)
            stmt = stmt.on_conflict_do_update(
                index_elements=["ticker", "date"],
                set_={"name": stmt.excluded.name,"open": stmt.excluded.open,"high": stmt.excluded.high,"low": stmt.excluded.low,"close": stmt.excluded.close,"adj_close": stmt.excluded.adj_close,"volume": stmt.excluded.volume,"change": stmt.excluded.change,})
            conn.execute(stmt)