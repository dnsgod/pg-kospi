-- =====================================================================
-- KOSPI100 Demo DB Schema (Unified, 2025-09-10)
--  - predictions PK 정렬: (date, ticker, model_name, horizon) ← 코드와 일치
--  - prediction_eval 방향정확도 컬럼: dir_correct BOOLEAN 로 표준화
--  - signals_view: predictions↔prediction_eval 조인 방식으로 단순/명료화
--  - watchlist 테이블(데모용 단일 사용자) 포함
-- =====================================================================

-- -----------------------------
-- 0) Extensions (optional)
-- -----------------------------
-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- -----------------------------
-- 1) Core Tables
-- -----------------------------
CREATE TABLE IF NOT EXISTS prices (
    date        date    NOT NULL,
    ticker      text    NOT NULL,
    name        text,
    open        numeric,
    high        numeric,
    low         numeric,
    close       numeric,
    adj_close   numeric,
    volume      bigint,
    change      numeric,
    PRIMARY KEY (ticker, date)
);

CREATE TABLE IF NOT EXISTS predictions (
    date        date    NOT NULL,
    ticker      text    NOT NULL,
    model_name  text    NOT NULL,
    horizon     int     NOT NULL,
    y_pred      numeric NOT NULL,
    PRIMARY KEY (date, ticker, model_name, horizon)  -- ← PK 순서 통일
);

CREATE TABLE IF NOT EXISTS prediction_eval (
    date        date    NOT NULL,   -- as-of (prediction date)
    ticker      text    NOT NULL,
    model_name  text    NOT NULL,
    horizon     int     NOT NULL,
    y_pred      numeric NOT NULL,
    y_true      numeric NOT NULL,   -- next trading day's close
    abs_err     numeric NOT NULL,
    dir_correct boolean NOT NULL,   -- ← 표준화
    PRIMARY KEY (date, ticker, model_name, horizon)
);

-- 데모 버전: 단일 사용자 가정
CREATE TABLE IF NOT EXISTS watchlist (
    ticker     varchar(20) PRIMARY KEY,
    note       text,
    created_at timestamp DEFAULT now()
);

-- -----------------------------
-- 2) Helpful Indexes
-- -----------------------------
CREATE INDEX IF NOT EXISTS idx_prices_date                   ON prices(date);
CREATE INDEX IF NOT EXISTS idx_prices_ticker                 ON prices(ticker);
CREATE INDEX IF NOT EXISTS idx_predictions_ticker_date       ON predictions(ticker, date);
CREATE INDEX IF NOT EXISTS idx_prediction_eval_ticker_date   ON prediction_eval(ticker, date);
CREATE INDEX IF NOT EXISTS ix_eval_ticker_model_horizon_date
    ON prediction_eval(ticker, model_name, horizon, date);

-- (과거 오타 인덱스 제거 - 안전한 no-op)
DROP INDEX IF EXISTS ix_eval_ticke_model_horizon_date;

-- -----------------------------
-- 3) Small Utility Views
-- -----------------------------
CREATE OR REPLACE VIEW latest_prices AS
SELECT DISTINCT ON (ticker) ticker, date, close, volume
FROM prices
ORDER BY ticker, date DESC;

-- 최근 250 거래일(date) 세트 (티커별)
CREATE OR REPLACE VIEW last250_dates AS
WITH distinct_days AS (
  SELECT DISTINCT ticker, date
  FROM prediction_eval
),
ranked AS (
  SELECT
    ticker,
    date,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
  FROM distinct_days
)
SELECT ticker, date
FROM ranked
WHERE rn <= 250;

-- -----------------------------
-- 4) App-Facing Views
-- -----------------------------
-- A) 티커×모델×호라이즌 성능 집계
DROP VIEW IF EXISTS prediction_metrics CASCADE;
CREATE VIEW prediction_metrics AS
SELECT
  e.ticker,
  e.model_name,
  e.horizon,
  AVG(e.abs_err)::float AS mae_all,
  AVG(CASE WHEN e.dir_correct THEN 1 ELSE 0 END)::float AS acc_all,
  AVG(CASE WHEN l.date IS NOT NULL THEN e.abs_err END)::float AS mae_250d,
  AVG(
    CASE WHEN l.date IS NOT NULL
         THEN CASE WHEN e.dir_correct THEN 1 ELSE 0 END
    END
  )::float AS acc_250d
FROM prediction_eval e
LEFT JOIN last250_dates l
  ON l.ticker = e.ticker AND l.date = e.date
GROUP BY e.ticker, e.model_name, e.horizon;

-- B) 모델 전반 성능 요약(모델×호라이즌)
DROP VIEW IF EXISTS prediction_leaderboard CASCADE;
CREATE VIEW prediction_leaderboard AS
WITH base AS (
  SELECT
    e.model_name,
    e.horizon,
    e.abs_err,
    e.dir_correct,
    (CASE WHEN l.date IS NOT NULL THEN 1 ELSE 0 END) AS in_250
  FROM prediction_eval e
  LEFT JOIN last250_dates l
    ON l.ticker = e.ticker AND l.date = e.date
)
SELECT
  model_name,
  horizon,
  AVG(abs_err)::float AS mae_all,
  AVG(CASE WHEN dir_correct THEN 1 ELSE 0 END)::float AS acc_all,
  AVG(CASE WHEN in_250 = 1 THEN abs_err END)::float AS mae_250d,
  AVG(
    CASE WHEN in_250 = 1
         THEN CASE WHEN dir_correct THEN 1 ELSE 0 END
    END
  )::float AS acc_250d
FROM base
GROUP BY model_name, horizon;

-- C) 시그널 뷰: 예측 vs y_true 변화율/절대변화
DROP VIEW IF EXISTS signals_view CASCADE;
CREATE VIEW signals_view AS
SELECT
  p.ticker,
  p.date,
  p.model_name,
  p.horizon,
  p.y_pred,
  e.y_true,
  CASE WHEN e.y_true = 0 THEN NULL
       ELSE (p.y_pred - e.y_true) / e.y_true
  END AS y_pred_pct_change,
  (p.y_pred - e.y_true) AS y_pred_abs_change
FROM predictions p
JOIN prediction_eval e
  ON e.ticker = p.ticker
 AND e.date   = p.date
 AND e.model_name = p.model_name
 AND e.horizon    = p.horizon
WHERE p.model_name LIKE 'safe_%';

-- =====================================================================
-- END
-- =====================================================================
