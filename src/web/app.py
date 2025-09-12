# src/web/app.py

# 0) 기본 import ---------------------------------------------------------------
import pandas as pd
import streamlit as st
from sqlalchemy import text
from src.db.conn import get_engine
from src.db.watchlist import add_watchlist, list_watchlist, list_watchlist_df, remove_watchlist
import plotly.graph_objects as go
from datetime import datetime
from zoneinfo import ZoneInfo


# 1) 페이지 설정 ---------------------------------------------------------------
st.set_page_config(page_title="KOSPI100 주가 예측 데모", layout="wide")
st.title("📈 KOSPI100 주가 예측 데모 (PostgreSQL)")
if "watchlist_v" not in st.session_state:
    st.session_state["watchlist_v"] = 0

# 2) DB 엔진 준비 --------------------------------------------------------------
engine = get_engine()

# 3) 유틸/데이터 로더 ----------------------------------------------------------
@st.cache_data(ttl=300)
def load_tickers():
    """prices 테이블에서 전체 티커 목록 조회 (5분 캐시)."""
    with engine.connect() as conn:
        df = pd.read_sql("SELECT DISTINCT ticker, name FROM prices ORDER BY ticker", conn)
    df["display"] = df.apply(
        lambda r: f"{r['name']} ({r['ticker']})" if pd.notna(r['name']) and str(r['name']).strip() else r['ticker'],
        axis=1
    )
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

@st.cache_data(ttl=60)
def load_watchlist_table(version: int) -> pd.DataFrame:
    """관심 종목 목록을 캐시로 읽는다.
    - version: st.session_state["watchlist_v"]가 바뀌면 캐시가 새로고침됨"""
    return list_watchlist_df()

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
    """prediction_metrics 뷰에서 티커별/호라이즌별 모델 성능 로드."""
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

@st.cache_data(ttl=900)
def load_price_with_ma(ticker: str, start_date: str | None = None) -> pd.DataFrame:
    """
    DB에서 해당 티커의 Close + MA5/MA20을 윈도우로 계산해서 로드.
    """
    with engine.connect() as conn:
        sql = text(f"""
            WITH base AS (
                SELECT date, ticker, close
                FROM prices
                WHERE ticker = :t { "AND date >= :sd" if start_date else "" }
                ORDER BY date
            ),
            ma AS (
                SELECT
                    date, ticker, close,
                    AVG(close) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)  AS ma5,
                    AVG(close) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20
                FROM base
            )
            SELECT * FROM ma ORDER BY date
        """)
        params = {"t": ticker}
        if start_date:
            params["sd"] = start_date
        df = pd.read_sql(sql, conn, params=params)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df

@st.cache_data(ttl=900)
def load_signals_ma(ticker: str, start_date: str | None = None) -> pd.DataFrame:
    """
    MA 골든/데드크로스 전용 뷰에서 신호 로드.
    - 주의: prediction 기반 signals_view와 충돌 방지 위해 'signals_ma_view' 사용
    - 필요 컬럼: date, ticker, close, ma5, ma20, signal_type('BUY'|'SELL'), reason
    """
    with engine.connect() as conn:
        sql = text(f"""
            SELECT date, ticker, close, ma5, ma20, signal_type, reason
            FROM signals_ma_view
            WHERE ticker = :t { "AND date >= :sd" if start_date else "" }
            ORDER BY date
        """)
        params = {"t": ticker}
        if start_date:
            params["sd"] = start_date
        df = pd.read_sql(sql, conn, params=params)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df

# === [ADD] 250912 ==============================================
@st.cache_data(ttl=120)
def load_signal_pred_report(d: str, hz: int, theta: float, topn: int, wl_only: bool) -> pd.DataFrame:
    """
    예측 기반 시그널 리포트 (signals_view)
    - d: 'YYYY-MM-DD' (하루치)
    - wl_only: True면 watchlist에 있는 티커만
    """
    sql = text("""
        WITH wl AS (SELECT ticker FROM watchlist)
        SELECT date, ticker, model_name, horizon, y_pred, y_true,
               y_pred_pct_change, y_pred_abs_change
        FROM signals_view
        WHERE date = :d
          AND horizon = :hz
          AND ABS(y_pred_pct_change) >= :theta
          AND (:wl_only = FALSE OR ticker IN (SELECT ticker FROM wl))
        ORDER BY ABS(y_pred_pct_change) DESC
        LIMIT :topn
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={
            "d": d, "hz": int(hz), "theta": float(theta),
            "topn": int(topn), "wl_only": bool(wl_only)
        })

@st.cache_data(ttl=120)
def load_signal_ma_report(d: str, topn: int, wl_only: bool) -> pd.DataFrame:
    """
    MA 기반 시그널 리포트 (signals_ma_view)
    - d: 'YYYY-MM-DD' (하루치)
    - wl_only: True면 watchlist에 있는 티커만
    """
    sql = text("""
        WITH wl AS (SELECT ticker FROM watchlist)
        SELECT date, ticker, close, ma5, ma20, signal_type, reason
        FROM signals_ma_view
        WHERE date = :d
          AND (:wl_only = FALSE OR ticker IN (SELECT ticker FROM wl))
        ORDER BY date DESC, ticker
        LIMIT :topn
    """)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params={
            "d": d, "topn": int(topn), "wl_only": bool(wl_only)
        })
# === [END ADD] 250912 ================================================================

# 4) 탭 구성 -------------------------------------------------------------------
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📈 티커별 성능", "🏆 모델 리더보드", "🔬 모델 비교", "🚨 시그널 보드", "⭐ 관심 종목", "🧾 시그널 리포트"
])
show_recent = st.toggle("최근 1개월 데이터만 보기", value=True)

# ----------------------------- 탭 1: 티커별 성능 ------------------------------
with tab1:
    tickers = load_tickers()
    label_ticker = dict(zip(tickers["display"], tickers["ticker"]))
    label = st.selectbox("종목 선택", options=list(label_ticker.keys()))
    t = label_ticker[label]
    st.subheader(f"선택된 종목: {label}")

    df_price = load_prices(t)
    if show_recent:
        df_price = df_price.tail(22)
    st.subheader("실제 종가")
    st.line_chart(df_price.set_index("date")[["close"]])

    pred = load_predictions(t)
    if pred.empty:
        st.info("예측 데이터가 없습니다. Day2/Day3 파이프라인을 먼저 실행하세요.")
        st.stop()

    seq = make_target_date_index(df_price)
    pred = pred.merge(seq, on="date", how="left").dropna(subset=["target_date"])
    pred["target_date"] = pd.to_datetime(pred["target_date"])

    all_models = sorted(pred["model_name"].unique().tolist())
    default_models = [m for m in all_models if m.startswith("safe_ens_")] or all_models
    sel_models = st.multiselect("표시할 모델 선택", all_models, default=default_models)

    pvt = (
        pred[pred["model_name"].isin(sel_models)]
        .pivot_table(index="target_date", columns="model_name", values="y_pred", aggfunc="last")
        .sort_index()
    )
    viz = df_price.set_index("date")[["close"]].join(pvt, how="outer")

    st.subheader("실제 vs 예측 (D+1, 거래일 기준)")
    st.line_chart(viz)

    metrics = load_metrics_by_ticker(t)
    st.subheader("티커별 지표 (MAE / ACC, 최근 250일 보정 포함)")
    st.dataframe(metrics, use_container_width=True)

    c1, c2 = st.columns(2)
    with c1:
        metrics_out = metrics.copy()
        metrics_out.insert(0, "company", label)
        st.download_button(
            "CSV 다운로드: 티커별 지표",
            metrics_out.to_csv(index=False).encode("utf-8"),
            file_name=f"metrics_{t}.csv", mime="text/csv"
        )
    with c2:
        out = viz.reset_index().rename(columns={"index": "date"})
        out.insert(0, "company", label)
        st.download_button(
            "CSV 다운로드: 실제+예측 시계열",
            out.to_csv(index=False).encode("utf-8"),
            file_name=f"series_{t}.csv", mime="text/csv"
        )
    
    st.markdown("---")
    st.subheader("시그널 오버레이 (MA 골든/데드크로스)")

    # 최근 1개월 토글과 연동해 시작일 결정
    start_for_signal = None
    if show_recent and not df_price.empty:
        start_for_signal = df_price["date"].min().strftime("%Y-%m-%d")
    else:
        # 전체 보기일 때는 선택적 시작일 지정
        start_picker = st.date_input("시작일(옵션)", value=None, format="YYYY-MM-DD", key="sig_start_picker")
        start_for_signal = str(start_picker) if start_picker else None

    # DB에서 MA 포함 가격/신호 로드
    price_ma = load_price_with_ma(t, start_for_signal)
    sig_ma   = load_signals_ma(t, start_for_signal)

    if price_ma.empty:
        st.info("가격 데이터가 없습니다.")
    else:
        fig = go.Figure()
        # 종가 + MA
        fig.add_trace(go.Scatter(x=price_ma["date"], y=price_ma["close"], mode="lines", name="Close"))
        if "ma5" in price_ma.columns:
            fig.add_trace(go.Scatter(x=price_ma["date"], y=price_ma["ma5"], mode="lines", name="MA5"))
        if "ma20" in price_ma.columns:
            fig.add_trace(go.Scatter(x=price_ma["date"], y=price_ma["ma20"], mode="lines", name="MA20"))

        # BUY/SELL 마커
        if not sig_ma.empty:
            buys  = sig_ma[sig_ma["signal_type"] == "BUY"]
            sells = sig_ma[sig_ma["signal_type"] == "SELL"]
            if not buys.empty:
                fig.add_trace(go.Scatter(
                    x=buys["date"], y=buys["close"], mode="markers",
                    name="BUY (GC)", marker_symbol="triangle-up", marker_size=12
                ))
            if not sells.empty:
                fig.add_trace(go.Scatter(
                    x=sells["date"], y=sells["close"], mode="markers",
                    name="SELL (DC)", marker_symbol="triangle-down", marker_size=12
                ))
        fig.update_layout(legend=dict(orientation="h"))
        st.plotly_chart(fig, use_container_width=True)

# --------------------------- 탭 2: 모델 리더보드 ------------------------------
with tab2:
    st.subheader("모델별 리더보드 (전체/최근250)")
    hz = st.selectbox("호라이즌", [1], index=0)
    leaderboard = load_leaderboard(hz)

    st.dataframe(leaderboard, use_container_width=True)

    chart_df = leaderboard.set_index("model_name")[["mae_250d"]].rename(columns={"mae_250d":"MAE(250d)"})
    if chart_df["MAE(250d)"].isna().all():
        chart_df = leaderboard.set_index("model_name")[["mae_all"]].rename(columns={"mae_all":"MAE(all)"})
    st.markdown("**MAE 기준 막대그래프**")
    st.bar_chart(chart_df)

    st.download_button(
        "CSV 다운로드: 리더보드",
        leaderboard.to_csv(index=False).encode("utf-8"),
        file_name=f"prediction_leaderboard_h{hz}.csv", mime="text/csv"
    )

# --------------------------- 탭 3: 모델 비교 ----------------------------------
with tab3:
    st.subheader("모델 비교 (티커 1개, 모델 2~3개)")
    tickers = load_tickers()
    label_ticker = dict(zip(tickers["display"], tickers["ticker"]))
    t = st.selectbox("티커", options=list(label_ticker.values()))
    hz = 1
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
            "CSV 다운로드: 모델 비교",
            df.to_csv(index=False).encode("utf-8"),
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
            "CSV 다운로드: 시그널",
            df_sig.to_csv(index=False).encode("utf-8"),
            file_name=f"signals_h{hz}_th{theta}.csv", mime="text/csv"
        )

# --------------------------- 탭 5: 관심 종목 --------------------------------
with tab5:
    st.subheader("내 관심 종목 관리")

    # 5-1) 티커 선택 → 추가
    tickers_df = load_tickers()
    label_ticker = dict(zip(tickers_df["display"], tickers_df["ticker"]))
    add_col1, add_col2 = st.columns([3, 1], vertical_alignment="bottom")

    with add_col1:
        add_label = st.selectbox("종목 선택 후 [추가]를 눌러 관심 종목에 등록하세요", options=list(label_ticker.keys()), key="wl_add_select")
        add_ticker = label_ticker[add_label]

    with add_col2:
        if st.button("➕ 추가", use_container_width=True):
            ok, msg = add_watchlist(add_ticker, validate=True)
            st.toast(msg)
            if ok:
                st.session_state["watchlist_v"] += 1  # 캐시 무효화 트리거

    # 5-2) 현재 관심 종목 표
    st.markdown("---")
    st.subheader("관심 종목 목록")
    wl_df = load_watchlist_table(st.session_state["watchlist_v"])
    if wl_df.empty:
        st.info("아직 관심 종목이 없습니다. 위에서 종목을 추가해 보세요.")
    else:
        # 행별 삭제 버튼
        # (표 보여주기 + 삭제 UI는 분리: 사용자에게 표로 전체를 보여주고, 아래에서 선택 삭제)
        st.dataframe(wl_df, use_container_width=True)

        # 삭제 UI
        del_col1, del_col2 = st.columns([3, 1], vertical_alignment="bottom")
        with del_col1:
            # 현재 watchlist 중에서 지울 티커 선택
            wl_list = list_watchlist()
            del_choice = st.selectbox("삭제할 종목을 선택하세요", options=wl_list, key="wl_del_select")
        with del_col2:
            if st.button("🗑️ 삭제", use_container_width=True):
                n = remove_watchlist(del_choice)
                if n > 0:
                    st.toast(f"삭제되었습니다: {del_choice}")
                    st.session_state["watchlist_v"] += 1
                else:
                    st.toast("삭제할 항목이 없습니다.")

    # 5-3) 관심 종목 기반 요약 보기 (차트/시그널)
    st.markdown("---")
    st.subheader("관심 종목 빠른 보기 (차트 & 시그널)")

    if wl_df.empty:
        st.caption("관심 종목을 추가하면 빠른 보기를 사용할 수 있습니다.")
        st.stop()

    # 멀티 선택 (기본: 전부 선택)
    wl_list_all = wl_df["ticker"].tolist()
    sel = st.multiselect("볼 종목(최대 3개 권장)", wl_list_all, default=wl_list_all[:3], key="wl_view_sel")

    # (A) 종가 차트: 여러 종목을 wide 형태로 합쳐 한 번에 보기
    if sel:
        st.markdown("**종가 추이(최근 1개월)**")
        price_wide = None
        for t in sel:
            dfp = load_prices(t).tail(22).rename(columns={"close": t}).set_index("date")
            price_wide = dfp if price_wide is None else price_wide.join(dfp, how="outer")
        st.line_chart(price_wide)

    # (B) 시그널 요약: 각 종목 상위 시그널 몇 개 모아서 표로
    st.markdown("**시그널 요약**")
    c1, c2, c3 = st.columns(3)
    with c1:
        hz = st.selectbox("호라이즌", [1], index=0, key="wl_sig_hz")
    with c2:
        theta = st.slider("예측 변화율 임계값 |pct|", 0.0025, 0.05, 0.01, 0.0025, key="wl_sig_th")
    with c3:
        topn = st.number_input("각 종목 Top N", 1, 50, 5, 1, key="wl_sig_topn")

    # 종목별로 signals_view에서 가져와 concat (간단·안전한 방식)
    sig_list = []
    if sel:
        with engine.connect() as conn:
            for t in sel:
                q = text("""
                    SELECT *
                    FROM signals_view
                    WHERE ticker = :t
                      AND horizon = :hz
                      AND ABS(y_pred_pct_change) >= :theta
                    ORDER BY ABS(y_pred_pct_change) DESC
                    LIMIT :topn
                """)
                sdf = pd.read_sql(q, conn, params={"t": t, "hz": int(hz), "theta": float(theta), "topn": int(topn)})
                sig_list.append(sdf.assign(_ticker=t))
    sig_df = pd.concat(sig_list, ignore_index=True) if sig_list else pd.DataFrame()

    if sig_df.empty:
        st.info("조건을 만족하는 시그널이 없습니다. 임계값(|pct|)을 낮춰보세요.")
    else:
        # 보기 좋게 컬럼 정리
        view_cols = ["ticker", "date", "model_name", "horizon", "y_pred", "y_true", "y_pred_pct_change", "y_pred_abs_change"]
        view_cols = [c for c in view_cols if c in sig_df.columns]
        st.dataframe(sig_df[view_cols], use_container_width=True)

        st.download_button(
            "CSV 다운로드: 관심 종목 시그널",
            sig_df.to_csv(index=False).encode("utf-8"),
            file_name="watchlist_signals.csv",
            mime="text/csv"
        )


# --------------------------- 탭 6: 시그널 리포트 -----------------------------
with tab6:
    st.subheader("🧾 시그널 요약 리포트 (하루치)")

    today_kst = datetime.now(ZoneInfo("Asia/Seoul")).date()
    col1, col2, col3, col4, col5 = st.columns([1.3, 1.1, 1.2, 1.3, 1.1])

    with col1:
        sel_date = st.date_input("날짜", value=today_kst, format="YYYY-MM-DD", key="report_date")
    with col2:
        wl_only = st.radio("범위", ["Watchlist", "전체"], horizontal=True) == "Watchlist"
    with col3:
        hz = st.selectbox("호라이즌", [1], index=0, help="현재는 D+1만 지원")
    with col4:
        theta = st.slider("|예측 변화율| 임계값", 0.0025, 0.05, 0.01, 0.0025, help="signals_view 필터")
    with col5:
        topn = st.number_input("Top N", 5, 200, 20, 5)

    query_date = sel_date.strftime("%Y-%m-%d")

    st.markdown("---")

    # (B) 예측 기반 시그널 표 ---------------------------------------------------
    st.markdown("### 🤖 예측 기반 (signals_view)")
    pred_df = load_signal_pred_report(query_date, hz=int(hz), theta=float(theta), topn=int(topn), wl_only=bool(wl_only))
    if pred_df.empty:
        st.info("선택한 날짜에 예측 기반 시그널이 없습니다.")
    else:
        # 보기 좋게 일부 컬럼 정렬/포맷
        cols_order = ["date","ticker","model_name","horizon","y_pred_pct_change","y_pred_abs_change","y_pred","y_true"]
        cols_order = [c for c in cols_order if c in pred_df.columns]
        dfv = pred_df[cols_order].copy()
        # 퍼센트 포맷(선택)
        if "y_pred_pct_change" in dfv.columns:
            try:
                dfv["y_pred_pct_change"] = (dfv["y_pred_pct_change"].astype(float) * 100).round(2).astype(str) + "%"
            except Exception:
                pass
        st.dataframe(dfv, use_container_width=True)
        st.download_button(
            "CSV 다운로드: 예측 기반 시그널",
            pred_df.to_csv(index=False).encode("utf-8"),
            file_name=f"signal_pred_{query_date}.csv", mime="text/csv"
        )

    st.markdown("---")

    # (C) MA 기반 시그널 표 -----------------------------------------------------
    st.markdown("### 📏 MA 기반 (signals_ma_view)")
    ma_df = load_signal_ma_report(query_date, topn=int(topn), wl_only=bool(wl_only))
    if ma_df.empty:
        st.info("선택한 날짜에 MA 기반 시그널이 없습니다.")
    else:
        cols_order = ["date","ticker","signal_type","reason","close","ma5","ma20"]
        cols_order = [c for c in cols_order if c in ma_df.columns]
        st.dataframe(ma_df[cols_order], use_container_width=True)
        st.download_button(
            "CSV 다운로드: MA 기반 시그널",
            ma_df.to_csv(index=False).encode("utf-8"),
            file_name=f"signal_ma_{query_date}.csv", mime="text/csv"
        )

    # (D) UX 가이드 -------------------------------------------------------------
    st.caption("※ 예측 기반은 Day2/Day3 파이프라인 실행 후 생성됩니다. 데이터가 없으면 상단 탭의 파이프라인을 먼저 실행하세요.")
