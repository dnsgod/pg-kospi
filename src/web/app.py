# src/web/app.py
from __future__ import annotations

import numpy as np
import pandas as pd
import altair as alt
import streamlit as st
from datetime import date, timedelta
from sqlalchemy import text

from src.db.conn import get_engine  # DB_* 환경변수 사용

pd.options.mode.copy_on_write = True
alt.data_transformers.disable_max_rows()
st.set_page_config(page_title="KOSPI Daily Signals Dashboard", layout="wide")

# ----------------------------- 스타일 ---------------------------------
st.markdown(
    """
<style>
/* 다크테마 가독성 */
:root, .block-container, .stMarkdown, .stMarkdown p, .stMarkdown span {
  color: #e5e7eb !important;
}
[data-testid="stMetricValue"], [data-testid="stMetricLabel"] {
  color: #e5e7eb !important;
}
section[data-testid="stSidebar"] * { color: #e5e7eb !important; }
</style>
""",
    unsafe_allow_html=True,
)

# =========================== 데이터 로더 ===============================

def load_ticker_name_map() -> dict[str, str]:
    """tickers(ticker,name)에서 맵을 만든다. 없으면 prices/predictions에서 코드만."""
    eng = get_engine()
    try:
        with eng.connect() as c:
            # tickers 테이블이 있으면 우선 사용
            try:
                df = pd.read_sql(text("SELECT ticker, name FROM tickers ORDER BY name"), c)
                if not df.empty:
                    return dict(zip(df["ticker"], df["name"]))
            except Exception:
                pass

            # fallback: predictions 또는 prices의 코드만으로 리스트
            for t in ("predictions_clean", "predictions", "prices"):
                try:
                    df = pd.read_sql(text(f"SELECT DISTINCT ticker FROM {t} ORDER BY 1"), c)
                    if not df.empty:
                        return {x: x for x in df["ticker"].astype(str)}
                except Exception:
                    continue
    except Exception as e:
        print(f"[WARN] ticker map load failed: {e}")
    return {}

def fetch_model_catalog(horizon: int, include_dl: bool, include_ml: bool) -> list[str]:
    """모델 리스트를 predictions_clean -> predictions 순서로 조회."""
    eng = get_engine()
    df = pd.DataFrame()
    with eng.connect() as c:
        for table in ("predictions_clean", "predictions"):
            try:
                df = pd.read_sql(
                    text(f"""
                        SELECT DISTINCT model_name
                        FROM {table}
                        WHERE horizon = :h
                    """),
                    c,
                    params={"h": horizon},
                )
                if not df.empty:
                    break
            except Exception:
                continue

    if df.empty:
        return []

    m = df["model_name"].astype(str)

    def is_dl(x: str) -> bool:
        return x.startswith("safe_dl_") or x.startswith("dl_")

    def is_ml(x: str) -> bool:
        return (x.startswith("safe_") and not is_dl(x)) or \
               any(x.startswith(p) for p in ("ma_", "ses_", "ens_"))

    mask = pd.Series(True, index=m.index)
    if not include_dl:
        mask &= ~m.map(is_dl)
    if not include_ml:
        mask &= ~m.map(is_ml)

    return m[mask].sort_values().tolist()

def fetch_data(
    ticker: str,
    horizon: int,
    since: date | None,
    until: date | None,
    models: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """선택된 티커의 가격과 예측을 기간/모델 필터와 함께 반환."""
    eng = get_engine()
    with eng.connect() as c:
        price_df = pd.read_sql(
            text("""
                SELECT date, open, high, low, close, volume
                FROM prices
                WHERE ticker = :t
                ORDER BY date
            """),
            c,
            params={"t": ticker},
        )

        pred_df = pd.DataFrame(columns=["date", "ticker", "model_name", "horizon", "y_pred"])
        for table in ("predictions_clean", "predictions"):
            try:
                sql = f"""
                    SELECT date, ticker, model_name, horizon, y_pred
                    FROM {table}
                    WHERE ticker = :t AND horizon = :h
                """
                params = {"t": ticker, "h": horizon}
                if models:
                    # psycopg2의 list -> ARRAY 바인딩 사용
                    sql += " AND model_name = ANY(:models)"
                    params["models"] = models
                pred_df = pd.read_sql(text(sql), c, params=params)
                break
            except Exception:
                continue

    if not price_df.empty:
        price_df["date"] = pd.to_datetime(price_df["date"])
    if not pred_df.empty:
        pred_df["date"] = pd.to_datetime(pred_df["date"])

    # 기간 필터
    if since:
        price_df = price_df[price_df["date"] >= pd.Timestamp(since)]
        pred_df = pred_df[pred_df["date"] >= pd.Timestamp(since)]
    if until:
        price_df = price_df[price_df["date"] <= pd.Timestamp(until)]
        pred_df = pred_df[pred_df["date"] <= pd.Timestamp(until)]

    return price_df.reset_index(drop=True), pred_df.reset_index(drop=True)

# ============================ 차트 빌더 =================================

def build_chart(
    price_df: pd.DataFrame,
    pred_df: pd.DataFrame,
    model_order: list[str],
    lock_axes: bool = True,
):
    """가격과 예측을 2축으로 겹쳐 그림. lock_axes=True면 축/줌 고정."""
    if price_df.empty:
        return alt.Chart(pd.DataFrame({"msg": ["데이터 없음"]})).mark_text(
            align="center", baseline="middle", fontSize=16
        ).encode(text="msg:N")

    # 고정 도메인 계산 (여유 5%)
    x_min = pd.to_datetime(price_df["date"].min())
    x_max = pd.to_datetime(price_df["date"].max())
    y_p_min = float(price_df["close"].min())
    y_p_max = float(price_df["close"].max())
    pad_p = max(1.0, (y_p_max - y_p_min) * 0.05)
    y_price_dom = [y_p_min - pad_p, y_p_max + pad_p]

    y_pred_dom = None
    if not pred_df.empty:
        y_h_min = float(pred_df["y_pred"].min())
        y_h_max = float(pred_df["y_pred"].max())
        pad_h = max(1.0, (y_h_max - y_h_min) * 0.05)
        y_pred_dom = [y_h_min - pad_h, y_h_max + pad_h]

    # 가격 라인
    price_line = (
        alt.Chart(price_df)
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X(
                "date:T",
                title="Date",
                scale=alt.Scale(domain=[x_min, x_max]) if lock_axes else alt.Undefined,
            ),
            y=alt.Y(
                "close:Q",
                title="Price",
                scale=alt.Scale(domain=y_price_dom) if lock_axes else alt.Undefined,
            ),
            tooltip=[alt.Tooltip("date:T"), alt.Tooltip("close:Q", format=",.0f")],
            color=alt.value("#8ab4f8"),
        )
    )

    if pred_df.empty:
        layers = price_line
    else:
        pred_line = (
            alt.Chart(pred_df)
            .mark_line(strokeDash=[4, 3])
            .encode(
                x=alt.X(
                    "date:T",
                    scale=alt.Scale(domain=[x_min, x_max]) if lock_axes else alt.Undefined,
                ),
                y=alt.Y(
                    "y_pred:Q",
                    title="Prediction",
                    scale=alt.Scale(domain=y_pred_dom) if (lock_axes and y_pred_dom) else alt.Undefined,
                ),
                color=alt.Color("model_name:N", title="Model", sort=model_order),
                tooltip=[
                    alt.Tooltip("date:T"),
                    alt.Tooltip("model_name:N"),
                    alt.Tooltip("y_pred:Q", format=",.0f"),
                ],
            )
        )
        layers = alt.layer(price_line, pred_line).resolve_scale(y="independent")

    base = layers.properties(height=560)  # 더 크게
    chart = base if lock_axes else base.interactive(bind_x=True)

    return (
        chart
        .configure_axis(labelColor="#e5e7eb", titleColor="#e5e7eb")
        .configure_legend(labelColor="#e5e7eb", titleColor="#e5e7eb")
    )

# ============================ UI 사이드바 =================================

name_map = load_ticker_name_map()

with st.sidebar:
    st.header("Controls")

    horizon = st.selectbox("Horizon", [1, 5], index=0)

    include_ml = st.checkbox("ML 포함 (safe_, ens_, ma_, ses_)", value=True)
    include_dl = st.checkbox("DL 포함 (safe_dl_* 등)", value=True)

    catalog = fetch_model_catalog(horizon, include_dl, include_ml)
    default_models = [m for m in ["safe_ens_mean", "safe_ens_median", "safe_ma_w20", "safe_dl_lstm_v1"] if m in catalog]
    selected_models = st.multiselect("Models (다중 선택)", options=catalog, default=default_models)

    today = date.today()
    since = st.date_input("기간 시작", today - timedelta(days=180))
    until = st.date_input("기간 종료", today)

    # 회사명으로 보이는 드롭다운 (내부 값은 티커)
    if not name_map:
        st.warning("티커 목록을 불러오지 못했습니다. 먼저 refresh_tickers를 실행해 주세요.")
        ticker = st.text_input("티커 코드 직접 입력", "005930")
    else:
        codes = list(name_map.keys())
        default_idx = 0
        if "ticker" in st.session_state and st.session_state["ticker"] in name_map:
            default_idx = codes.index(st.session_state["ticker"])
        ticker = st.selectbox(
            "티커 선택",
            options=codes,
            index=default_idx,
            format_func=lambda t: f"{name_map.get(t, t)} · {t}",
            placeholder="회사명으로 검색…",
        )
        st.session_state["ticker"] = ticker

# ============================== 본문 ====================================

sel_name = name_map.get(ticker, ticker)
st.title("KOSPI Daily Signals Dashboard")
st.caption("수집된 가격 데이터와 다양한 모델 예측을 한 화면에서 확인하세요. 사이드바에서 모델/기간/티커를 조정할 수 있습니다.")

price_df, pred_df = fetch_data(ticker, horizon, since, until, selected_models)

# 상단 메트릭
latest_close = float(price_df["close"].iloc[-1]) if not price_df.empty else np.nan
last_pred = float(pred_df.groupby("date")["y_pred"].mean().iloc[-1]) if not pred_df.empty else np.nan

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("최근 종가", f"{latest_close:,.0f}" if np.isfinite(latest_close) else "-")
with c2:
    st.metric("최근 예측(평균)", f"{last_pred:,.0f}" if np.isfinite(last_pred) else "-")
with c3:
    diff = last_pred - latest_close if np.isfinite(last_pred) and np.isfinite(latest_close) else None
    st.metric("예측-실제", f"{diff:,.0f}" if diff is not None else "-")
with c4:
    st.metric("선택", f"{sel_name} · H{horizon}")

# 선택 모델 요약
st.subheader("선택한 모델")
st.write(", ".join(selected_models) if selected_models else "(선택 없음)")

# 차트
chart = build_chart(price_df, pred_df, model_order=selected_models, lock_axes=True)
st.altair_chart(chart, use_container_width=True)

# 예측 테이블 + CSV
st.subheader("예측 테이블")
if pred_df.empty:
    st.info("선택한 조건에 해당하는 예측 데이터가 없습니다.")
else:
    view = pred_df.sort_values(["date", "model_name"]).reset_index(drop=True)
    st.dataframe(
        view[["date", "ticker", "model_name", "horizon", "y_pred"]],
        use_container_width=True,
        hide_index=True,
        height=420,
    )
    st.download_button(
        "CSV 다운로드",
        data=view.to_csv(index=False).encode("utf-8"),
        file_name=f"predictions_{ticker}_H{horizon}.csv",
        mime="text/csv",
    )
