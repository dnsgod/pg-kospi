# --- Day3: 앙상블 + 평가 파이프라인 -------------------------------------
from sqlalchemy import text
from src.db.conn import get_engine
from src.db.load_predictions import upsert_predictions
import pandas as pd
import numpy as np
# [핵심 포인트]
# - 예측은 as-of(오늘까지 본 날)를 키로 저장돼 있음 (horizon=1)
# - 실제 정답은 다음 거래일(close)
# - 먼저 safe_% 모델만 모아 앙상블 산출 → predictions에 새로운 모델명으로 UPSERT
# - 이후 모든 predictions를 정답과 매칭해 prediction_eval에 저장

def _fetch_safe_predictions(horizon: int = 1) -> pd.DataFrame:
    """safe_% 모델만 불러와 앙상블 계산용 프레임으로 사용."""
    eng = get_engine()
    with eng.connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT date, ticker, model_name, horizon, y_pred
                FROM predictions
                WHERE horizon=:h AND model_name LIKE 'safe_%'
            """),
            conn, params={"h": horizon}
        )
    # date 타입 정규화
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    return df

def _compute_ensembles(df: pd.DataFrame) -> pd.DataFrame:
    """(ticker,date,horizon) 묶음 기준으로 mean/median 산출."""
    if df.empty:
        return pd.DataFrame(columns=["date","ticker","model_name","horizon","y_pred"])
    g = df.groupby(["date","ticker","horizon"])["y_pred"]
    mean_df = g.mean().reset_index().assign(model_name="safe_ens_mean")
    med_df  = g.median().reset_index().assign(model_name="safe_ens_median")
    out = pd.concat([mean_df, med_df], ignore_index=True)
    # 컬럼 순서 맞추기
    return out[["date","ticker","model_name","horizon","y_pred"]]

def _upsert_eval_rows(df_eval: pd.DataFrame):
    """prediction_eval 테이블에 UPSERT. pandas+SQLAlchemy로 일괄 처리."""
    if df_eval.empty:
        return
    eng = get_engine()
    # 빠르고 간단하게: 임시 테이블로 밀고 MERGE (여기서는 ON CONFLICT로 직접)
    # pandas의 to_sql + 원시 upsert도 가능하지만, 간단히 반복 실행.
    # 데이터 양이 커지면 청크 처리 권장.
    create_sql = """
    INSERT INTO prediction_eval (date,ticker,model_name,horizon,y_pred,y_true,abs_err,dir_correct)
    VALUES (:date,:ticker,:model_name,:horizon,:y_pred,:y_true,:abs_err,:dir_correct)
    ON CONFLICT (date, ticker, model_name, horizon)
    DO UPDATE SET
      y_pred = EXCLUDED.y_pred,
      y_true = EXCLUDED.y_true,
      abs_err = EXCLUDED.abs_err,
      dir_correct = EXCLUDED.dir_correct;
    """
    recs = df_eval.to_dict(orient="records")
    with eng.begin() as conn:
        for r in recs:
            conn.execute(text(create_sql), r)

def _fetch_next_day_truth() -> pd.DataFrame:
    """prices에서 (ticker, asof, target_date, close_asof, close_target) 시퀀스를 만든다."""
    eng = get_engine()
    with eng.connect() as conn:
        prices = pd.read_sql(text("SELECT ticker, date, close FROM prices ORDER BY ticker, date"), conn)

    prices["date"] = pd.to_datetime(prices["date"])
    prices = prices.sort_values(["ticker", "date"]).reset_index(drop=True)

    # ✅ groupby.shift 로 경고 없이 다음 거래일/정답 만들기
    prices["target_date"]  = prices.groupby("ticker")["date"].shift(-1)
    prices["close_asof"]   = prices["close"]
    prices["close_target"] = prices.groupby("ticker")["close"].shift(-1)

    # 마지막 as-of(다음날 없음)는 평가에서 제외
    seq = prices.dropna(subset=["target_date", "close_target"]).copy()

    # 타입 정리
    seq["date"]        = pd.to_datetime(seq["date"]).dt.date
    seq["target_date"] = pd.to_datetime(seq["target_date"]).dt.date

    return seq[["ticker", "date", "target_date", "close_asof", "close_target"]]

def run(horizon: int = 1):
    """Day3 전체 파이프라인 진입점."""
    # 1) safe_%만 모아 앙상블 산출
    safe_preds = _fetch_safe_predictions(horizon=horizon)
    if safe_preds.empty:
        print("[INFO] no safe_% predictions found. Run day2 safe pipeline first.")
        return

    ens = _compute_ensembles(safe_preds)
    upsert_predictions(ens)  # 앙상블도 predictions에 저장
    print(f"[INFO] ensembles upserted: {len(ens)} rows")

    # 2) 모든 predictions(=safe 개별 + 앙상블) 평가용 정답 매칭
    #    - as-of date 기준의 다음 거래일을 truth로 가져온다
    seq = _fetch_next_day_truth()

    # 3) predictions 전체 로드 (필요시 safe_% + safe_ens_%만으로 제한)
    eng = get_engine()
    with eng.connect() as conn:
        preds = pd.read_sql(
            text("""
                SELECT date, ticker, model_name, horizon, y_pred
                FROM predictions
                WHERE horizon=:h
                  AND (model_name LIKE 'safe_%' OR model_name LIKE 'safe_ens_%')
            """),
            conn, params={"h": horizon}
        )
    preds["date"] = pd.to_datetime(preds["date"]).dt.date

    # 4) as-of로 조인하여 y_true 매칭
    eval_df = preds.merge(seq, on=["ticker","date"], how="inner")
    if eval_df.empty:
        print("[INFO] no matching eval rows found.")
        return

    # ✅ y_true를 먼저 만든다
    eval_df["y_true"] = eval_df["close_target"]

    # 5) 평가 지표 산출
    eval_df["abs_err"] = (eval_df["y_true"] - eval_df["y_pred"]).abs()
    eval_df["dir_correct"] = (
        ((eval_df["y_true"] - eval_df["close_asof"]) * (eval_df["y_pred"] - eval_df["close_asof"])) > 0
    ) | (
        ((eval_df["y_true"] - eval_df["close_asof"]) == 0) & ((eval_df["y_pred"] - eval_df["close_asof"]) == 0)
    )

    eval_df = eval_df.dropna(subset=["y_pred", "y_true", "close_asof"])  # 결측치 제거

    # 6) 저장 컬럼만 추출하여 UPSERT
    save_cols = ["date","ticker","model_name","horizon","y_pred","y_true","abs_err","dir_correct"]
    _upsert_eval_rows(eval_df[save_cols])

if __name__ == "__main__":
    run(horizon=1)
