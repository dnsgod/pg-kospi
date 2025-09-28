from __future__ import annotations
from datetime import date
import os
import pandas as pd
from sqlalchemy import text
from src.db.conn import get_engine

REPORT_DIR = os.path.join(os.getenv("PROJECT_DIR", "/opt/project"), "reports")
os.makedirs(REPORT_DIR, exist_ok=True)

def _latest_pred_date() -> date | None:
    eng = get_engine()
    with eng.connect() as c:
        r = c.execute(text("SELECT MAX(date) FROM predictions_clean")).scalar()
    return pd.to_datetime(r).date() if r else None

def run():
    asof = _latest_pred_date()
    if not asof:
        print("[report] no predictions")
        return
    eng = get_engine()
    with eng.connect() as c:
        q = """
        WITH best AS (
          SELECT p.date, p.ticker, p.model_name, p.y_pred,
                 RANK() OVER (PARTITION BY p.date, p.ticker ORDER BY p.y_pred DESC) AS rk
          FROM predictions_clean p
          WHERE p.date = :d
        )
        SELECT b.date, b.ticker, t.name, b.model_name, b.y_pred
        FROM best b
        LEFT JOIN tickers t ON t.ticker = b.ticker
        WHERE b.rk = 1
        ORDER BY b.ticker;
        """
        df = pd.read_sql(text(q), c, params={"d": asof})
    if df.empty:
        print("[report] empty for", asof)
        return
    out = os.path.join(REPORT_DIR, f"signal_report_{asof}.csv")
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[report] saved: {out}")

if __name__ == "__main__":
    run()
