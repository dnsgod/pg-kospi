from __future__ import annotations
import math
import pandas as pd
from sqlalchemy import text
from src.db.conn import get_engine
from src.db.io import upsert_predictions, upsert_evals

SAFE_MODELS = ["safe_ma_w5","safe_ma_w10","safe_ma_w20","safe_ses_a0.3","safe_ses_a0.5"]
H = 1

def _fetch_pred_panel():
    eng = get_engine()
    with eng.connect() as c:
        q = """
        SELECT date, ticker, model_name, y_pred
        FROM predictions
        WHERE horizon=:h AND model_name = ANY(:ms)
        """
        df = pd.read_sql(text(q), c, params={"h": H, "ms": SAFE_MODELS})
    return df

def _fetch_actuals():
    eng = get_engine()
    with eng.connect() as c:
        df = pd.read_sql("SELECT date, ticker, close FROM prices", c)
    return df

def run():
    preds = _fetch_pred_panel()
    if preds.empty:
        print("[eval] no preds")
        return
    actuals = _fetch_actuals()
    preds["date"] = pd.to_datetime(preds["date"])
    actuals["date"] = pd.to_datetime(actuals["date"])

    # horizon=1: asof date의 다음 영업일 종가를 y_true로 맞춤
    actuals["date_prev"] = actuals.groupby("ticker")["date"].shift(1)
    # y_true 테이블: (asof_date=prev_date, ticker, y_true=close)
    ytrue = actuals.dropna(subset=["date_prev"])[["date_prev","ticker","close"]].rename(
        columns={"date_prev":"date","close":"y_true"}
    )

    # 앙상블용 피벗
    pv = preds.pivot_table(index=["date","ticker"], columns="model_name", values="y_pred")
    pv = pv.dropna(how="all")
    ens_mean = pv.mean(axis=1)
    ens_median = pv.median(axis=1)

    rows_pred = []
    for (d,t), v in ens_mean.items():
        rows_pred.append({"date": d.date(), "ticker": t, "model_name":"safe_ens_mean","horizon":H,"y_pred": float(v)})
    for (d,t), v in ens_median.items():
        rows_pred.append({"date": d.date(), "ticker": t, "model_name":"safe_ens_median","horizon":H,"y_pred": float(v)})

    n_pred = upsert_predictions(rows_pred)
    print(f"[eval] ensemble upserted={n_pred}")

    # 평가: 모델별 예측을 ytrue와 조인(예측 기준일 기준)
    merged = preds.merge(ytrue, on=["date","ticker"], how="inner")
    if merged.empty:
        print("[eval] no pairs to score")
        return

    def _metrics(g):
        err = g["y_pred"] - g["y_true"]
        mae = err.abs().mean()
        mape = (err.abs() / g["y_true"]).replace([math.inf, -math.inf], math.nan).dropna().mean()
        rmse = math.sqrt((err**2).mean())
        d = g["date"].iloc[0].date()
        return pd.Series({"date": d, "ticker": g["ticker"].iloc[0], "mae": float(mae), "mape": float(mape), "rmse": float(rmse)})

    scored = merged.groupby(["date","ticker","model_name"]).apply(_metrics).reset_index()
    rows_eval = []
    for _, r in scored.iterrows():
        rows_eval.append({
            "date": r["date"], "ticker": r["ticker"], "model_name": r["model_name"],
            "horizon": H, "mae": r["mae"], "mape": r["mape"], "rmse": r["rmse"]
        })
    n_eval = upsert_evals(rows_eval)
    print(f"[eval] metrics upserted={n_eval}")

if __name__ == "__main__":
    run()
