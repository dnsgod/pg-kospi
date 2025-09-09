# src/web/app.py

# 0) 기본 import ---------------------------------------------------------------
import pandas as pd
import streamlit as st
from sqlalchemy import text
from src.db.conn import get_engine

# 1) 페이지 설정 ---------------------------------------------------------------
st.set_page_config(page_title="KOSPI100 주가 예측 데모", layout="wide")
st.title("📈 KOSPI100 주가 예측 데모 (PostgreSQL)")

# 2) DB 엔진 준비 --------------------------------------------------------------
engine = get_engine()

# 3) 유틸/데이터 로더 ----------------------------------------------------------
@st.cache_data(ttl=300)
def load_tickers():
    """prices 테이블에서 전체 티커 목록 조회 (5분 캐시)."""
    with engine.connect() as conn:
        df = pd.read_sql("SELECT DISTINCT ticker, name FROM prices ORDER BY ticker", conn)
    df["display"] = df.apply(lambda r : f"{r['name']} ({r['ticker']})" if pd.notna(r['name'])and str(r['name']).strip() else r['ticker'], axis=1)
    return df

@st.cache_data(ttl=180)
def load_prices(ticker: str) -> pd.DataFrame:
    """특정 티커의 종가 시계열."""
    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT date, close FROM prices WHERE ticker=:t ORDER BY date"),
            conn, params={"t": ticker}
        )
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date")

@st.cache_data(ttl=180)
def load_predictions(ticker: str) -> pd.DataFrame:
    """horizon=1, safe_* 및 safe_ens_* 모델 예측만 로드."""
    with engine.connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT date, model_name, y_pred
                FROM predictions
                WHERE ticker=:t AND horizon=1
                  AND (model_name LIKE 'safe_%' OR model_name LIKE 'safe_ens_%')
                ORDER BY date
            """),
            conn, params={"t": ticker}
        )
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df

@st.cache_data(ttl=120)
def load_metrics_by_ticker(ticker: str, horizon: int = 1) -> pd.DataFrame:
    """
    prediction_metrics 뷰에서 티커별/호라이즌별 모델 성능 로드.
    새 스키마: mae_all, acc_all, mae_250d, acc_250d
    """
    with engine.connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT model_name, horizon, mae_all, acc_all, mae_250d, acc_250d
                FROM prediction_metrics
                WHERE ticker = :t AND horizon = :hz
                ORDER BY model_name
            """),
            conn, params={"t": ticker, "hz": horizon}
        )
    return df

@st.cache_data(ttl=120)
def load_leaderboard(horizon: int = 1) -> pd.DataFrame:
    with engine.connect() as conn:
        df = pd.read_sql(
            text("""
                SELECT model_name, horizon, mae_all, acc_all, mae_250d, acc_250d
                FROM prediction_leaderboard
                WHERE horizon = :hz
                ORDER BY COALESCE(mae_250d, mae_all)
            """),
            conn, params={"hz": horizon}
        )
    return df


def make_target_date_index(prices_df: pd.DataFrame) -> pd.DataFrame:
    """as-of 예측을 다음 거래일(target_date)로 이동시키는 인덱스 테이블."""
    seq = prices_df[["date"]].reset_index(drop=True).copy()
    seq["target_date"] = seq["date"].shift(-1)
    return seq.rename(columns={"date": "date"})


# 4) 탭 구성 -------------------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs(["📈 티커별 성능", "🏆 모델 리더보드", "🔬 모델 비교", "🚨 시그널 보드"])
show_recent = st.toggle("최근 1개월 데이터만 보기", value=True)

# ----------------------------- 탭 1: 티커별 성능 ------------------------------
with tab1:
    # 4-1) 티커 선택
    tickers = load_tickers()
    label_ticker = dict(zip(tickers["display"], tickers["ticker"]))
    label = st.selectbox("종목 선택", options=list(label_ticker.keys()))
    t = label_ticker[label]
    st.subheader(f"선택된 종목: {label}")

    # 4-2) 실제 종가
    df_price = load_prices(t)
    if show_recent:
        df_price = df_price.tail(22)
    st.subheader("실제 종가")
    st.line_chart(df_price.set_index("date")[["close"]])

    # 4-3) 예측 로드 및 as-of → target_date 정렬
    pred = load_predictions(t)
    if pred.empty:
        st.info("예측 데이터가 없습니다. Day2/Day3 파이프라인을 먼저 실행하세요.")
        st.stop()

    seq = make_target_date_index(df_price)
    pred = pred.merge(seq, on="date", how="left").dropna(subset=["target_date"])
    pred["target_date"] = pd.to_datetime(pred["target_date"])

    # 4-4) 모델 선택 멀티셀렉트 (기본: 앙상블 우선)
    all_models = sorted(pred["model_name"].unique().tolist())
    default_models = [m for m in all_models if m.startswith("safe_ens_")] or all_models
    sel_models = st.multiselect("표시할 모델 선택", all_models, default=default_models)

    # 4-5) 피벗 → 실제 vs 예측 오버레이
    pvt = (
        pred[pred["model_name"].isin(sel_models)]
        .pivot_table(index="target_date", columns="model_name", values="y_pred", aggfunc="last")
        .sort_index()
    )
    viz = df_price.set_index("date")[["close"]].join(pvt, how="outer")

    st.subheader("실제 vs 예측 (D+1, 거래일 기준)")
    st.line_chart(viz)

    # 4-6) 지표 표 (티커별)
    metrics = load_metrics_by_ticker(t)
    st.subheader("티커별 지표 (n / MAE / RMSE / 방향정확도)")
    st.dataframe(metrics, use_container_width=True)

    # 4-7) CSV 다운로드 (티커별 지표 & 시계열)
    c1, c2 = st.columns(2)
    with c1:
        metrics_out = metrics.copy()
        metrics_out.insert(0, "company", label)
        st.download_button(
            "지표 CSV 다운로드", metrics_out.to_csv(index=False).encode("utf-8"),
            file_name=f"metrics_{t}.csv", mime="text/csv"
        )
    with c2:
        out = viz.reset_index().rename(columns={"index": "date"})
        out.insert(0, "company", label)
        st.download_button(
            "시계열 CSV 다운로드 (실제+예측)", out.to_csv(index=False).encode("utf-8"),
            file_name=f"series_{t}.csv", mime="text/csv"
        )

# --------------------------- 탭 2: 모델 리더보드 ------------------------------
with tab2:
    st.subheader("모델별 리더보드 (전체/최근250)")
    hz = st.selectbox("호라이즌", [1], index=0)  # 필요 시 동적 로드로 확장
    leaderboard = load_leaderboard(hz)

    st.dataframe(leaderboard, use_container_width=True)

    # 막대그래프는 최근 250일 기준이 있으면 그걸 우선
    chart_df = leaderboard.set_index("model_name")[["mae_250d"]].rename(columns={"mae_250d":"MAE(250d)"})
    if chart_df["MAE(250d)"].isna().all():
        chart_df = leaderboard.set_index("model_name")[["mae_all"]].rename(columns={"mae_all":"MAE(all)"})
    st.markdown("**MAE 기준 막대그래프**")
    st.bar_chart(chart_df)

    st.download_button(
        "리더보드 CSV 다운로드", leaderboard.to_csv(index=False).encode("utf-8"),
        file_name=f"prediction_leaderboard_h{hz}.csv", mime="text/csv"
    )

# --------------------------- 탭 3: 모델 비교 ----------------------------------
with tab3:
    st.subheader("모델 비교 (티커 1개, 모델 2~3개)")
    tickers = load_tickers()
    label_ticker = dict(zip(tickers["display"], tickers["ticker"]))
    t = st.selectbox("티커", options=list(label_ticker.values()))
    hz = 1
    # 평가 시계열 가져오기
    df = pd.read_sql(
        text("""
            WITH filtered AS (
              SELECT date, ticker, model_name, horizon, y_pred, y_true
              FROM prediction_eval
              WHERE ticker=:t AND horizon=:hz
            )
            SELECT * FROM filtered ORDER BY date
        """), engine.connect(), params={"t": t, "hz": hz}
    )
    if df.empty:
        st.info("데이터가 없습니다.")
    else:
        models = sorted(df["model_name"].unique().tolist())
        sel = st.multiselect("모델 선택(2~3개 권장)", models, default=models[:2])
        df = df[df["model_name"].isin(sel)]
        pvt = df.pivot_table(index="date", columns="model_name", values="y_pred")
        pvt["y_true"] = df.groupby("date")["y_true"].mean()
        st.line_chart(pvt)

        st.download_button(
            "모델 비교 CSV 다운로드", df.to_csv(index=False).encode("utf-8"),
            file_name=f"model_compare_{t}.csv", mime="text/csv"
        )

# --------------------------- 탭 4: 시그널 보드 -------------------------------
with tab4:
    st.subheader("시그널 보드 (임계값 기반)")
    hz = 1
    theta = st.slider("예측 변화율 임계값 |pct|", 0.0025, 0.05, 0.01, 0.0025)
    topn  = st.number_input("Top N", 5, 100, 20, 5)
    df_sig = pd.read_sql(
        text("""
            SELECT * FROM signals_view
            WHERE horizon=:hz AND ABS(y_pred_pct_change) >= :theta
            ORDER BY ABS(y_pred_pct_change) DESC
            LIMIT :topn
        """),
        engine.connect(), params={"hz": hz, "theta": float(theta), "topn": int(topn)}
    )
    st.dataframe(df_sig, use_container_width=True)

    if not df_sig.empty:
        st.download_button(
            "시그널 CSV 다운로드", df_sig.to_csv(index=False).encode("utf-8"),
            file_name=f"signals_h{hz}_th{theta}.csv", mime="text/csv"
        )
