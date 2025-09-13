# project/src/pipeline/eval_daily.py
from __future__ import annotations
import pandas as pd
from sqlalchemy import text
from src.db.conn import get_engine

def _nextday_truth() -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        px = pd.read_sql(text("SELECT ticker, date, close FROM prices ORDER BY ticker, date"), conn)
    px["date"] = pd.to_datetime(px["date"])
    px = px.sort_values(["ticker","date"]).reset_index(drop=True)
    px["target_date"]  = px.groupby("ticker")["date"].shift(-1)
    px["close_asof"]   = px["close"]
    px["close_target"] = px.groupby("ticker")["close"].shift(-1)
    seq = px.dropna(subset=["target_date","close_target"]).copy()
    seq["date"]        = pd.to_datetime(seq["date"]).dt.date
    seq["target_date"] = pd.to_datetime(seq["target_date"]).dt.date
    return seq[["ticker","date","target_date","close_asof","close_target"]]

def run(h=1):
    eng = get_engine()
    # (1) 예측 로드 (safe_* + safe_ens_*)
    with eng.connect() as conn:
        preds = pd.read_sql(text("""
            SELECT date, ticker, model_name, horizon, y_pred
            FROM predictions
            WHERE horizon=:h
              AND (model_name LIKE 'safe_%' OR model_name LIKE 'safe_ens_%')
        """), conn, params={"h": h})
    if preds.empty:
        print("[INFO] predictions empty"); return
    preds["date"] = pd.to_datetime(preds["date"]).dt.date

    # (2) 정답 시퀀스 매핑
    seq = _nextday_truth()
    df = preds.merge(seq, on=["ticker","date"], how="inner")
    if df.empty:
        print("[INFO] no matches"); return
    df["y_true"] = df["close_target"]
    df["abs_err"] = (df["y_true"] - df["y_pred"]).abs()
    df["dir_correct"] = (
        ((df["y_true"] - df["close_asof"]) * (df["y_pred"] - df["close_asof"])) > 0
    ) | (
        ((df["y_true"] - df["close_asof"]) == 0) & ((df["y_pred"] - df["close_asof"]) == 0)
    )
    df = df.dropna(subset=["y_pred","y_true","close_asof"])

    # (3) prediction_eval UPSERT
    sql = text("""
      INSERT INTO prediction_eval (date,ticker,model_name,horizon,y_pred,y_true,abs_err,dir_correct)
      VALUES (:date,:ticker,:model_name,:horizon,:y_pred,:y_true,:abs_err,:dir_correct)
      ON CONFLICT (date, ticker, model_name, horizon)
      DO UPDATE SET
        y_pred=EXCLUDED.y_pred,
        y_true=EXCLUDED.y_true,
        abs_err=EXCLUDED.abs_err,
        dir_correct=EXCLUDED.dir_correct
    """)
    recs = df[["date","ticker","model_name","horizon","y_pred","y_true","abs_err","dir_correct"]].to_dict("records")
    with eng.begin() as conn:
        for r in recs:
            conn.execute(sql, r)
    print(f"[INFO] eval upserted: {len(recs)} rows")

if __name__ == "__main__":
    run(h=1)
