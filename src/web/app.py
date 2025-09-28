# app.py  — Streamlit Dashboard (predictions_clean + tickers 기반)
from __future__ import annotations
import os
from datetime import datetime, timedelta, date
from typing import List, Optional

import pandas as pd
import numpy as np
import streamlit as st
import altair as alt
from sqlalchemy import text

# 프로젝트의 DB 엔진
from src.db.conn import get_engine

st.set_page_config(page_title="KOSPI Signals", layout="wide")
st.title("📈 KOSPI Signals Dashboard")

# -----------------------------
# DB helpers
# -----------------------------
@st.cache_data(ttl=300)
def get_dates() -> pd.DataFrame:
    """predictions_clean 기준으로 가용 예측일 목록(최신순)"""
    with get_engine().connect() as conn:
        q = "SELECT DISTINCT date FROM predictions_clean ORDER BY date DESC"
        return pd.read_sql(q, conn)

@st.cache_data(ttl=300)
def get_models() -> List[str]:
    """predictions_clean 기준 모델 목록(알파벳 순)"""
    with get_engine().connect() as conn:
        q = "SELECT DISTINCT model_name FROM predictions_clean ORDER BY 1"
        return pd.read_sql(q, conn)["model_name"].tolist()

@st.cache_data(ttl=300)
def get_tickers_df() -> pd.DataFrame:
    """티커 마스터(이름 붙이기)"""
    with get_engine().connect() as conn:
        q = "SELECT ticker, name FROM tickers ORDER BY ticker"
        df = pd.read_sql(q, conn)
    return df

@st.cache_data(ttl=300)
def get_predictions(pred_date: date, models: List[str]) -> pd.DataFrame:
    """특정 일자/모델들의 예측 전체 (조인으로 이름 포함)"""
    if not models:
        return pd.DataFrame(columns=["date","ticker","model_name","y_pred","name"])
    with get_engine().connect() as conn:
        q = text("""
            SELECT p.date, p.ticker, p.model_name, p.y_pred, t.name
            FROM predictions_clean p
            JOIN tickers t USING (ticker)
            WHERE p.date = :d
              AND p.model_name = ANY(:m)
        """)
        df = pd.read_sql(q, conn, params={"d": pred_date, "m": models})
    return df

@st.cache_data(ttl=300)
def get_last_close_for_date(tickers: List[str], asof: date) -> pd.DataFrame:
    """각 티커의 asof 이전 마지막 종가(신호 계산용)"""
    if not tickers:
        return pd.DataFrame(columns=["ticker","last_close"])
    with get_engine().connect() as conn:
        # 각 티커별 asof 이전 최대일자 가져와서 종가 붙이기
        q = text("""
            WITH last_d AS (
              SELECT ticker, MAX(date) AS d
              FROM prices
              WHERE date <= :asof AND ticker = ANY(:ts)
              GROUP BY ticker
            )
            SELECT p.ticker, p.close AS last_close
            FROM prices p
            JOIN last_d ld
              ON p.ticker = ld.ticker AND p.date = ld.d
        """)
        df = pd.read_sql(q, conn, params={"asof": asof, "ts": tickers})
    return df

@st.cache_data(ttl=300)
def get_price_series(ticker: str, window_days: int = 180) -> pd.DataFrame:
    """가격 시계열 (최근 window_days) + 이름"""
    since = date.today() - timedelta(days=window_days*2)  # 영업일 보정 여유
    with get_engine().connect() as conn:
        q = text("""
            SELECT s.date, s.ticker, s.open, s.high, s.low, s.close, s.volume, t.name
            FROM prices s
            JOIN tickers t USING (ticker)
            WHERE s.ticker = :t
              AND s.date >= :d
            ORDER BY s.date
        """)
        df = pd.read_sql(q, conn, params={"t": ticker, "d": since})
    return df

@st.cache_data(ttl=300)
def get_model_preds_for_ticker(ticker: str, up_to: Optional[date] = None, models: Optional[List[str]] = None) -> pd.DataFrame:
    """종목의 모델별 예측 히스토리 (필요 시 up_to 이전만)"""
    with get_engine().connect() as conn:
        if models:
            q = """
                SELECT date, ticker, model_name, y_pred
                FROM predictions_clean
                WHERE ticker = :t AND model_name = ANY(:m)
            """
            params = {"t": ticker, "m": models}
        else:
            q = """
                SELECT date, ticker, model_name, y_pred
                FROM predictions_clean
                WHERE ticker = :t
            """
            params = {"t": ticker}
        if up_to:
            q += " AND date <= :d"
            params["d"] = up_to
        q += " ORDER BY date"
        df = pd.read_sql(text(q), conn, params=params)
    return df

# -----------------------------
# Sidebar filters
# -----------------------------
dates_df = get_dates()
if dates_df.empty:
    st.warning("📭 아직 predictions_clean 데이터가 없습니다. DAG를 먼저 실행해 주세요.")
    st.stop()

models_all = get_models()
tickers_df = get_tickers_df()

latest_date = dates_df["date"].max().date() if hasattr(dates_df["date"].max(), "date") else dates_df["date"].max()
pred_date = st.sidebar.date_input("예측 기준일", value=latest_date,
                                  min_value=dates_df["date"].min(),
                                  max_value=latest_date)

default_models = [m for m in models_all if m in ("safe_ens_median", "safe_ens_mean", "safe_dl_lstm_v1")]
if not default_models:
    default_models = models_all[:2]

models_select = st.sidebar.multiselect("모델 선택", models_all, default=default_models)

# 티커 검색(이름/코드)
st.sidebar.markdown("---")
q = st.sidebar.text_input("티커/이름 검색", placeholder="예: 삼성전자 또는 005930")
if q:
    mask = tickers_df["ticker"].str.contains(q, case=False) | tickers_df["name"].str.contains(q, case=False, na=False)
    search_candidates = tickers_df[mask].copy()
else:
    search_candidates = tickers_df.copy()

sel_ticker = st.sidebar.selectbox(
    "상세 보기 종목",
    ["(선택 안 함)"] + [f"{r['ticker']} · {r['name']}" for _, r in search_candidates.iterrows()]
)

topk = st.sidebar.number_input("Top/Bottom K", min_value=5, max_value=50, value=20, step=5)

# -----------------------------
# Overview: Top/Bottom signals
# -----------------------------
st.subheader("🔎 Signals Overview")

preds = get_predictions(pred_date, models_select)
if preds.empty:
    st.info("선택한 날짜/모델 조합에 예측 데이터가 없습니다.")
else:
    # 신호 계산: (예측 / 직전 종가) - 1
    last_close = get_last_close_for_date(preds["ticker"].unique().tolist(), pred_date)
    preds = preds.merge(last_close, on="ticker", how="left")
    preds["signal"] = (preds["y_pred"] / preds["last_close"] - 1.0).replace([np.inf, -np.inf], np.nan)
    preds = preds.dropna(subset=["signal"])

    # 모델별 탭
    tabs = st.tabs([f"📌 {m}" for m in models_select])
    for tab, m in zip(tabs, models_select):
        with tab:
            dfm = preds.loc[preds["model_name"] == m].copy()
            if dfm.empty:
                st.warning(f"{m}: 데이터 없음")
                continue

            # 상위/하위
            top_df = dfm.sort_values("signal", ascending=False).head(topk)
            bottom_df = dfm.sort_values("signal", ascending=True).head(topk)

            c1, c2 = st.columns(2)
            with c1:
                st.markdown(f"**상위 {topk} (상승 신호)** — {m}")
                st.dataframe(
                    top_df[["ticker","name","y_pred","last_close","signal"]]
                    .assign(signal_pct=lambda x: (x["signal"]*100).round(2))
                    .rename(columns={"y_pred": "pred", "last_close": "last"}),
                    use_container_width=True, hide_index=True
                )
            with c2:
                st.markdown(f"**하위 {topk} (하락 신호)** — {m}")
                st.dataframe(
                    bottom_df[["ticker","name","y_pred","last_close","signal"]]
                    .assign(signal_pct=lambda x: (x["signal"]*100).round(2))
                    .rename(columns={"y_pred": "pred", "last_close": "last"}),
                    use_container_width=True, hide_index=True
                )

# -----------------------------
# Ticker Explorer (차트)
# -----------------------------
st.markdown("---")
st.subheader("📊 Ticker Explorer")

if sel_ticker != "(선택 안 함)":
    sel_code = sel_ticker.split("·")[0].strip()
    px = get_price_series(sel_code, window_days=220)
    if px.empty:
        st.warning("가격 데이터가 없습니다.")
    else:
        name = px["name"].iloc[0] if "name" in px.columns and pd.notna(px["name"].iloc[0]) else sel_code
        st.markdown(f"**{sel_code} · {name}**")
        # 가격 라인
        base = alt.Chart(px.assign(date=pd.to_datetime(px["date"]))).mark_line().encode(
            x=alt.X("date:T", title="Date"),
            y=alt.Y("close:Q", title="Close"),
            tooltip=["date:T","close:Q","volume:Q"]
        ).properties(height=320)

        st.altair_chart(base.interactive(), use_container_width=True)

        # 예측 오버레이(선택 모델만 / pred_date 이전 히스토리 포함)
        mp = get_model_preds_for_ticker(sel_code, up_to=pred_date, models=models_select)
        if not mp.empty:
            mp = mp.assign(date=pd.to_datetime(mp["date"]))
            pred_lines = alt.Chart(mp).mark_line(point=True).encode(
                x="date:T",
                y="y_pred:Q",
                color=alt.Color("model_name:N", title="model"),
                tooltip=["date:T","model_name:N","y_pred:Q"]
            ).properties(height=320)
            st.altair_chart((base + pred_lines).interactive(), use_container_width=True)
        else:
            st.info("선택 모델의 예측 히스토리가 없습니다.")

# -----------------------------
# Coverage (모델·티커 교차표)
# -----------------------------
st.markdown("---")
st.subheader("🧩 Coverage")

if not preds.empty:
    cov = (preds.groupby(["model_name"])["ticker"]
           .nunique()
           .reset_index(name="num_tickers")
           .sort_values("num_tickers", ascending=False))
    st.dataframe(cov, use_container_width=True, hide_index=True)
else:
    st.caption("예측 데이터가 있을 때 커버리지를 보여줍니다.")

# -----------------------------
# Download today's signals
# -----------------------------
st.markdown("---")
st.subheader("⬇️ Export")

if not preds.empty:
    exp = preds.sort_values(["model_name","signal"], ascending=[True, False]).copy()
    exp["signal_pct"] = (exp["signal"]*100).round(3)
    exp = exp[["date","ticker","name","model_name","last_close","y_pred","signal_pct"]]
    st.download_button(
        label=f"Download signals ({pred_date})",
        data=exp.to_csv(index=False).encode("utf-8-sig"),
        file_name=f"signals_{pred_date}.csv",
        mime="text/csv"
    )
else:
    st.caption("다운로드할 신호가 없습니다.")
