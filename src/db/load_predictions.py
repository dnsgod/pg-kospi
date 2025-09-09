from sqlalchemy import Table, MetaData
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
from .conn import get_engine

def upsert_predictions(df: pd.DataFrame, chunk = 5000):
    """
    df columns: ['date', 'ticker', 'model_name', 'horizon', 'y_pred']"""

    if df is None or df.empty:
        print("No data to upsert.")
        return
    df = df.copy()
    df['date'] = pd.to_datetime(df['date']).dt.date

    engine = get_engine()
    md = MetaData()
    preds = Table('predictions', md, autoload_with=engine)

    with engine.begin() as conn:
        for i in range(0, len(df), chunk):
            part = df.iloc[i:i+chunk]
            if part.empty:
                continue
            recs = part.to_dict(orient='records')
            stmt = insert(preds).values(recs)
            stmt = stmt.on_conflict_do_update(
                index_elements=['date', 'ticker', 'model_name', 'horizon'],
                set_={'y_pred': stmt.excluded.y_pred}
            )
            conn.execute(stmt)