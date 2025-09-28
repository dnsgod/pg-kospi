from __future__ import annotations
from sqlalchemy import text
from .conn import get_engine

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tickers (
  ticker TEXT PRIMARY KEY,
  name   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS prices (
  date       DATE NOT NULL,
  ticker     TEXT NOT NULL,
  open       NUMERIC,
  high       NUMERIC,
  low        NUMERIC,
  close      NUMERIC,
  adj_close  NUMERIC,
  volume     BIGINT,
  change     NUMERIC,
  PRIMARY KEY (date, ticker),
  FOREIGN KEY (ticker) REFERENCES tickers(ticker)
);

CREATE TABLE IF NOT EXISTS predictions (
  date       DATE NOT NULL,
  ticker     TEXT NOT NULL,
  model_name TEXT NOT NULL,
  horizon    INT  NOT NULL,
  y_pred     NUMERIC NOT NULL,
  PRIMARY KEY (date, ticker, model_name, horizon),
  FOREIGN KEY (ticker) REFERENCES tickers(ticker)
);

CREATE TABLE IF NOT EXISTS evals (
  date       DATE NOT NULL,  -- asof (예측 기준일)
  ticker     TEXT NOT NULL,
  model_name TEXT NOT NULL,
  horizon    INT  NOT NULL,
  mae        NUMERIC,
  mape       NUMERIC,
  rmse       NUMERIC,
  PRIMARY KEY (date, ticker, model_name, horizon),
  FOREIGN KEY (ticker) REFERENCES tickers(ticker)
);

-- 뷰: 안전 모델만 + horizon=1
CREATE OR REPLACE VIEW predictions_clean AS
SELECT * FROM predictions
WHERE model_name LIKE 'safe_%' AND horizon = 1;

CREATE OR REPLACE VIEW model_catalog AS
SELECT DISTINCT model_name FROM predictions_clean ORDER BY 1;
"""

def ensure_schema():
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text(SCHEMA_SQL))

def upsert_tickers(rows: list[dict]):
    if not rows: return 0
    sql = """
    INSERT INTO tickers (ticker, name)
    VALUES (:ticker, :name)
    ON CONFLICT (ticker) DO UPDATE SET name = EXCLUDED.name;
    """
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text(sql), rows)
    return len(rows)

def upsert_prices(rows: list[dict]):
    if not rows: return 0
    sql = """
    INSERT INTO prices (date,ticker,open,high,low,close,adj_close,volume,change)
    VALUES (:date,:ticker,:open,:high,:low,:close,:adj_close,:volume,:change)
    ON CONFLICT (date,ticker) DO UPDATE SET
        open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
        close=EXCLUDED.close, adj_close=EXCLUDED.adj_close,
        volume=EXCLUDED.volume, change=EXCLUDED.change;
    """
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text(sql), rows)
    return len(rows)

def upsert_predictions(rows: list[dict]):
    if not rows: return 0
    sql = """
    INSERT INTO predictions (date,ticker,model_name,horizon,y_pred)
    VALUES (:date,:ticker,:model_name,:horizon,:y_pred)
    ON CONFLICT (date,ticker,model_name,horizon)
    DO UPDATE SET y_pred = EXCLUDED.y_pred;
    """
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text(sql), rows)
    return len(rows)

def upsert_evals(rows: list[dict]):
    if not rows: return 0
    sql = """
    INSERT INTO evals (date,ticker,model_name,horizon,mae,mape,rmse)
    VALUES (:date,:ticker,:model_name,:horizon,:mae,:mape,:rmse)
    ON CONFLICT (date,ticker,model_name,horizon)
    DO UPDATE SET mae=EXCLUDED.mae,mape=EXCLUDED.mape,rmse=EXCLUDED.rmse;
    """
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text(sql), rows)
    return len(rows)
