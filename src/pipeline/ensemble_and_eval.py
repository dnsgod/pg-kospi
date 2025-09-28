# src/pipeline/ensemble_and_eval.py
from __future__ import annotations
import numpy as np
import pandas as pd
from sqlalchemy import text
from datetime import datetime
from src.db.conn import get_engine

pd.options.mode.copy_on_write = True

HORIZON = 1
BATCH_SIZE = 50_000

SAFE_BASE_PREFIXES = ("safe_ma_", "safe_ses_", "safe_dl_")  # 앙상블 입력에 사용할 안전 계열

def _fetch_base_predictions(eng) -> pd.DataFrame:
    """앙상블의 재료가 될 안전 계열 예측만 가져온다."""
    sql = """
        SELECT date, ticker, model_name, y_pred
        FROM predictions
        WHERE horizon = :h
          AND (
                model_name LIKE 'safe_ma_%'
             OR model_name LIKE 'safe_ses_%'
             OR model_name LIKE 'safe_dl_%'
          )
    """
    with eng.connect() as c:
        df = pd.read_sql(text(sql), c, params={"h": HORIZON})
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df

def _upsert_predictions(eng, df: pd.DataFrame):
    """predictions 테이블에 (date, ticker, model_name, horizon) 키로 UPSERT."""
    if df.empty:
        return 0
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date

    sql = """
        INSERT INTO predictions (date, ticker, model_name, horizon, y_pred)
        VALUES (:date, :ticker, :model_name, :horizon, :y_pred)
        ON CONFLICT (date, ticker, model_name, horizon)
        DO UPDATE SET y_pred = EXCLUDED.y_pred
    """
    total = 0
    with eng.begin() as c:
        rows = df.to_dict(orient="records")
        for i in range(0, len(rows), BATCH_SIZE):
            c.execute(text(sql), rows[i : i + BATCH_SIZE])
            total += len(rows[i : i + BATCH_SIZE])
    return total

def _build_ensembles(base_df: pd.DataFrame) -> pd.DataFrame:
    """동일 date/ticker에 대해 평균/중앙값 앙상블을 만든다."""
    if base_df.empty:
        return pd.DataFrame(columns=["date","ticker","model_name","horizon","y_pred"])

    g = base_df.groupby(["date","ticker"], observed=True)["y_pred"]
    ens_mean = g.mean().rename("y_pred").reset_index()
    ens_mean["model_name"] = "safe_ens_mean"
    ens_mean["horizon"] = HORIZON

    ens_median = g.median().rename("y_pred").reset_index()
    ens_median["model_name"] = "safe_ens_median"
    ens_median["horizon"] = HORIZON

    out = pd.concat([ens_mean, ens_median], ignore_index=True)
    # 컬럼 순서 정리
    return out[["date","ticker","model_name","horizon","y_pred"]]

def _fetch_truth_next_close(eng) -> pd.DataFrame:
    """
    H=1 평가용 정답: 다음 영업일 종가.
    WINDOW 함수로 prices에서 next_close를 바로 구해온다.
    """
    sql = """
        SELECT
            ticker,
            date,
            LEAD(close) OVER (PARTITION BY ticker ORDER BY date) AS y_true
        FROM prices
    """
    with eng.connect() as c:
        df = pd.read_sql(text(sql), c)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    # 마지막 날짜는 y_true가 NULL이므로 평가에서 자연히 제외됨
    return df

def _compute_metrics_frame(merged: pd.DataFrame) -> pd.DataFrame:
    """
    merged: [date, ticker, model_name, y_pred, y_true]
    그룹 키를 apply가 반환하지 않도록 '메트릭만' 반환한다 (중복 열 방지).
    """
    if merged.empty:
        return pd.DataFrame(columns=["date","ticker","model_name","mae","mape","rmse"])

    def _metrics(g: pd.DataFrame) -> pd.Series:
        err = g["y_pred"] - g["y_true"]
        mae = float(np.mean(np.abs(err)))
        denom = g["y_true"].replace(0, np.nan)
        mape = float(np.mean(np.abs(err / denom)) * 100)
        rmse = float(np.sqrt(np.mean(err * err)))
        return pd.Series({"mae": mae, "mape": mape, "rmse": rmse})

    # 키는 groupby가 보관하고, 반환은 메트릭만 → reset_index 시 중복 열 문제 없음
    scored = (
        merged
        .groupby(["date","ticker","model_name"], observed=True)
        .apply(_metrics)
        .reset_index()
    )
    return scored

def run() -> None:
    eng = get_engine()

    # 1) 안전 계열 예측 로딩 → 앙상블 생성/업서트
    base = _fetch_base_predictions(eng)
    ens = _build_ensembles(base)
    up_cnt = _upsert_predictions(eng, ens)
    print(f"[eval] ensemble upserted={up_cnt}")

    # 2) 평가용 데이터 조인 (예측 + 다음날 종가)
    #    predictions_clean 뷰가 있으면 우선 사용
    pred_tbl_candidates = ["predictions_clean", "predictions"]
    with eng.connect() as c:
        pred_tbl = None
        for t in pred_tbl_candidates:
            try:
                c.execute(text(f"SELECT 1 FROM {t} LIMIT 1"))
                pred_tbl = t
                break
            except Exception:
                continue
        if pred_tbl is None:
            print("[eval][WARN] predictions 테이블/뷰를 찾지 못했습니다.")
            return

    pred_sql = f"""
        SELECT date, ticker, model_name, y_pred
        FROM {pred_tbl}
        WHERE horizon = :h
          AND (
                model_name LIKE 'safe_ma_%'
             OR model_name LIKE 'safe_ses_%'
             OR model_name LIKE 'safe_dl_%'
             OR model_name IN ('safe_ens_mean','safe_ens_median')
          )
    """
    with eng.connect() as c:
        preds = pd.read_sql(text(pred_sql), c, params={"h": HORIZON})
    if preds.empty:
        print("[eval] no predictions to score")
        return
    preds["date"] = pd.to_datetime(preds["date"])

    truth = _fetch_truth_next_close(eng)
    # 조인: 예측은 t 의 D → 정답은 D+1 close(=truth.y_true)
    merged = preds.merge(
        truth, on=["ticker", "date"], how="inner", validate="many_to_one"
    ).dropna(subset=["y_true"])

    if merged.empty:
        print("[eval] nothing to evaluate after join")
        return

    # 3) 메트릭 산출(여기서 실패했던 부분 수정)
    scored = _compute_metrics_frame(merged)

    # 4) 일자별/모델별 요약(전종목 평균)도 만들어 저장(선택 사항)
    daily_model = (
        scored.groupby(["date","model_name"], observed=True)[["mae","mape","rmse"]]
        .mean()
        .reset_index()
    )

    # 5) 저장: 간단히 evaluations 테이블(없으면 생성)로 업서트
    with eng.begin() as c:
        c.execute(text("""
            CREATE TABLE IF NOT EXISTS evaluations (
                date        date        NOT NULL,
                ticker      varchar(6)  NOT NULL,
                model_name  text        NOT NULL,
                horizon     int         NOT NULL DEFAULT 1,
                mae         double precision,
                mape        double precision,
                rmse        double precision,
                PRIMARY KEY (date, ticker, model_name, horizon)
            )
        """))

    scored["horizon"] = HORIZON
    eval_up = _bulk_upsert_eval(eng, scored)
    print(f"[eval] evaluations upserted={eval_up}")

    # (옵션) 일자별-모델별 평균 테이블도 별도로 원하면 아래 사용
    with eng.begin() as c:
        c.execute(text("""
            CREATE TABLE IF NOT EXISTS evaluations_daily_model (
                date        date        NOT NULL,
                model_name  text        NOT NULL,
                horizon     int         NOT NULL,
                mae         double precision,
                mape        double precision,
                rmse        double precision,
                PRIMARY KEY (date, model_name, horizon)
            )
        """))
    daily_model["horizon"] = HORIZON
    dm_up = _bulk_upsert_daily_model(eng, daily_model)
    print(f"[eval] daily_model upserted={dm_up}")

def _bulk_upsert_eval(eng, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    df = df[["date","ticker","model_name","horizon","mae","mape","rmse"]].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    sql = """
        INSERT INTO evaluations (date, ticker, model_name, horizon, mae, mape, rmse)
        VALUES (:date, :ticker, :model_name, :horizon, :mae, :mape, :rmse)
        ON CONFLICT (date, ticker, model_name, horizon)
        DO UPDATE SET
            mae = EXCLUDED.mae,
            mape = EXCLUDED.mape,
            rmse = EXCLUDED.rmse
    """
    total = 0
    with eng.begin() as c:
        rows = df.to_dict(orient="records")
        for i in range(0, len(rows), BATCH_SIZE):
            c.execute(text(sql), rows[i : i + BATCH_SIZE])
            total += len(rows[i : i + BATCH_SIZE])
    return total

def _bulk_upsert_daily_model(eng, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    df = df[["date","model_name","horizon","mae","mape","rmse"]].copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    sql = """
        INSERT INTO evaluations_daily_model (date, model_name, horizon, mae, mape, rmse)
        VALUES (:date, :model_name, :horizon, :mae, :mape, :rmse)
        ON CONFLICT (date, model_name, horizon)
        DO UPDATE SET
            mae = EXCLUDED.mae,
            mape = EXCLUDED.mape,
            rmse = EXCLUDED.rmse
    """
    total = 0
    with eng.begin() as c:
        rows = df.to_dict(orient="records")
        for i in range(0, len(rows), BATCH_SIZE):
            c.execute(text(sql), rows[i : i + BATCH_SIZE])
            total += len(rows[i : i + BATCH_SIZE])
    return total

if __name__ == "__main__":
    run()
