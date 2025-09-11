import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import text
from src.db.conn import get_engine

def load_signals(ticker: str, start_date: str = None) -> pd.DataFrame:
    """
    signals_view에서 특정 티커의 BUY/SELL 신호 로드.
    - 반환 컬럼: date, ticker, name, close, ma5, ma20, signal_type, reason
    """
    eng = get_engine()
    where = "ticker = :t"
    params = {"t": ticker}
    if start_date:
        where += " AND date >= :sd"
        params["sd"] = start_date

    sql = text(f"""
        SELECT date, ticker, name, close, ma5, ma20, signal_type, reason
        FROM signals_ma_view
        WHERE {where}
        ORDER BY date
    """)
    with eng.connect() as conn:
        return pd.read_sql(sql, conn, params=params)

def plot_price_with_signals(price_df: pd.DataFrame, signals_df: pd.DataFrame, title: str):
    """
    price_df: [date, close] 포함
    signals_df: load_signals() 결과
    """
    fig = go.Figure()

    # 종가 라인
    fig.add_trace(go.Scatter(
        x=price_df["date"], y=price_df["close"],
        mode="lines", name="Close"
    ))
    # MA5, MA20
    if "ma5" in price_df.columns:
        fig.add_trace(go.Scatter(x=price_df["date"], y=price_df["ma5"], mode="lines", name="MA5"))
    if "ma20" in price_df.columns:
        fig.add_trace(go.Scatter(x=price_df["date"], y=price_df["ma20"], mode="lines", name="MA20"))

    # BUY/SELL 마커
    if not signals_df.empty:
        buys  = signals_df[signals_df["signal_type"] == "BUY"]
        sells = signals_df[signals_df["signal_type"] == "SELL"]

        fig.add_trace(go.Scatter(
            x=buys["date"], y=buys["close"], mode="markers",
            name="BUY (GC)", marker_symbol="triangle-up", marker_size=12
        ))
        fig.add_trace(go.Scatter(
            x=sells["date"], y=sells["close"], mode="markers",
            name="SELL (DC)", marker_symbol="triangle-down", marker_size=12
        ))

    fig.update_layout(title=title, legend=dict(orientation="h"))
    return fig