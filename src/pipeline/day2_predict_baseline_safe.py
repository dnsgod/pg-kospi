import pandas as pd
from sqlalchemy import text
from src.db.conn import get_engine
from src.db.load_predictions import upsert_predictions
from src.models.baseline_safe import ma_next_day_series, ses_next_day_series

HORIZON = 1
MIN_HISTORY = 20  # 최소 이 정도 지난 뒤부터 예측 생성
pd.options.mode.copy_on_write = True

def fetch_all_tickers():
    eng = get_engine()
    with eng.connect() as conn:
        return pd.read_sql("SELECT DISTINCT ticker FROM prices ORDER BY 1", conn)["ticker"].tolist()

def fetch_prices(ticker: str) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        df = pd.read_sql(
            text("SELECT date, close FROM prices WHERE ticker=:t ORDER BY date"),
            conn, params={"t": ticker}
        )
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").dropna()
    df = df.reset_index(drop=True)            # 0..n-1 정수 인덱스 사용
    return df

def build_no_leak_predictions(ticker: str) -> pd.DataFrame:
    df = fetch_prices(ticker)
    if len(df) < MIN_HISTORY:
        return pd.DataFrame(columns=["date","ticker","model_name","horizon","y_pred"])

    # 이동평균들
    frames = []
    for w in [5, 10, 20]:
        res = ma_next_day_series(df["close"], window=w)
        if not res.empty:
            asof_dates   = df.loc[res["asof_idx"],   "date"].to_numpy()   # ← 핵심: to_numpy()
            # target_date는 저장 안 하지만, 디버깅/시각화용으로 쓰고 싶다면 이렇게 꺼내두세요
            # target_dates = df.loc[res["target_idx"], "date"].to_numpy()

            out = pd.DataFrame({
                "date":      asof_dates,                 # 배열 길이 == len(res)
                "ticker":    ticker,                     # 스칼라는 브로드캐스트 됨
                "model_name": f"safe_ma_w{w}",
                "horizon":   HORIZON,
                "y_pred":    res["y_pred"].to_numpy(),   # 배열로 통일
            })
            # 필요하면 날짜를 date 타입으로:
            out["date"] = pd.to_datetime(out["date"]).dt.date
            frames.append(out)

    # SES들
    for a in [0.3, 0.5]:
        res = ses_next_day_series(df["close"], alpha=a)
        if not res.empty:
            asof_dates = df.loc[res["asof_idx"], "date"].to_numpy()

            out = pd.DataFrame({
                "date":       asof_dates,
                "ticker":     ticker,
                "model_name": f"safe_ses_a{a}",
                "horizon":    HORIZON,
                "y_pred":     res["y_pred"].to_numpy(),
            })
            out["date"] = pd.to_datetime(out["date"]).dt.date
            frames.append(out)

    if not frames:
        return pd.DataFrame(columns=["date","ticker","model_name","horizon","y_pred"])

    full = pd.concat(frames, ignore_index=True)

    # DB 스키마는 target_date 칼럼이 없으므로, 저장은 as-of 기준으로 하고
    # target_date는 시각화에서 계산/조인으로 활용하자.
    save_cols = ["date","ticker","model_name","horizon","y_pred"]
    return full[save_cols], full  # (저장용, 시각화 보조용)

def run(limit: int | None = None):
    tickers = fetch_all_tickers()
    if limit:
        tickers = tickers[:limit]

    save_rows = []
    viz_rows = []  # 필요하면 parquet로 따로 저장해 디버깅 가능
    for t in tickers:
        try:
            save_df, viz_df = build_no_leak_predictions(t)
            if not save_df.empty:
                save_rows.append(save_df)
            if not viz_df.empty:
                viz_rows.append(viz_df)
        except Exception as e:
            print(f"[WARN] {t} fail: {e}")

    if not save_rows:
        print("[INFO] no predictions created")
        return

    pred_df = pd.concat(save_rows, ignore_index=True)
    upsert_predictions(pred_df)
    print(f"[INFO] predictions upserted: {len(pred_df)} rows")

    # (선택) 누수 점검용으로 parquet 저장
    # if viz_rows:
    #     pd.concat(viz_rows, ignore_index=True).to_parquet("data/pred_debug.parquet", index=False)

if __name__ == "__main__":
    run(limit=None)
