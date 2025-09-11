
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
CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON prices(ticker, date);
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

-- C) 시그널 뷰
DROP VIEW IF EXISTS signals_view CASCADE;
CREATE OR REPLACE VIEW signals_view AS
WITH base AS (
  -- as-of 기준 예측과 실제가 있는 테이블 활용
  SELECT
    date,
    ticker,
    model_name,
    horizon,
    y_pred,
    y_true
  FROM prediction_eval
),
lagged AS (
  SELECT
    *,
    LAG(y_pred) OVER (
      PARTITION BY ticker, model_name, horizon
      ORDER BY date
    ) AS prev_pred
  FROM base
)
SELECT
  date,
  ticker,
  model_name,
  horizon,
  y_pred,
  y_true,
  (y_pred - prev_pred)                               AS y_pred_abs_change,
  CASE
    WHEN prev_pred = 0 THEN NULL
    ELSE (y_pred - prev_pred) / NULLIF(prev_pred, 0)
  END                                                AS y_pred_pct_change
FROM lagged
WHERE prev_pred IS NOT NULL
ORDER BY ABS((y_pred - prev_pred) / NULLIF(prev_pred, 0)) DESC, date DESC;

CREATE OR REPLACE VIEW signals_ma_view AS
WITH base AS (
  SELECT date, ticker, close FROM prices
),
ma AS (
  SELECT
    date, ticker, close,
    AVG(close) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)  AS ma5,
    AVG(close) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20
  FROM base
),
lagged AS (
  SELECT *, LAG(ma5) OVER (PARTITION BY ticker ORDER BY date) AS prev_ma5,
           LAG(ma20) OVER (PARTITION BY ticker ORDER BY date) AS prev_ma20
  FROM ma
)
SELECT
  date, ticker, close, ma5, ma20,
  CASE WHEN ma5>ma20 AND prev_ma5<=prev_ma20 THEN 'BUY'
       WHEN ma5<ma20 AND prev_ma5>=prev_ma20 THEN 'SELL'
       ELSE NULL END AS signal_type,
  CASE WHEN ma5>ma20 AND prev_ma5<=prev_ma20 THEN 'ma_golden_cross'
       WHEN ma5<ma20 AND prev_ma5>=prev_ma20 THEN 'ma_dead_cross'
       ELSE NULL END AS reason
FROM lagged
WHERE (ma5>ma20 AND prev_ma5<=prev_ma20)
   OR (ma5<ma20 AND prev_ma5>=prev_ma20)
ORDER BY ticker, date;

-- =====================================================================
-- END
-- =====================================================================
