-- =====================================================================
-- KOSPI100 Demo DB Schema (Consolidated)
-- Tables, Indexes, and Views used by the Streamlit app & pipelines
-- Date: 2025-09-09
-- Notes:
--  - Uses dir_corr (BOOLEAN) in prediction_eval for directional accuracy
--  - Provides reusable views: last250_dates, prediction_metrics,
--    prediction_leaderboard, signals_view
-- =====================================================================

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
    PRIMARY KEY (date, model_name, horizon, ticker)
);

CREATE TABLE IF NOT EXISTS prediction_eval (
    date        date    NOT NULL,   -- as-of (prediction date)
    ticker      text    NOT NULL,
    model_name  text    NOT NULL,
    horizon     int     NOT NULL,
    y_pred      numeric NOT NULL,
    y_true      numeric NOT NULL,   -- next trading day's close
    abs_err     numeric NOT NULL,
    dir_corr    boolean NOT NULL,   -- directional accuracy
    PRIMARY KEY (date, ticker, model_name, horizon)
);

-- -----------------------------
-- 2) Helpful Indexes
-- -----------------------------
CREATE INDEX IF NOT EXISTS idx_prices_date      ON prices(date);
CREATE INDEX IF NOT EXISTS idx_prices_ticker    ON prices(ticker);
CREATE INDEX IF NOT EXISTS idx_predictions_ticker_date ON predictions(ticker, date);
CREATE INDEX IF NOT EXISTS idx_prediction_eval_ticker_date ON prediction_eval(ticker, date);
CREATE INDEX IF NOT EXISTS ix_eval_ticker_model_horizon_date
    ON prediction_eval(ticker, model_name, horizon, date);

-- Clean up any legacy/typo objects (safe no-ops if absent)
DROP INDEX IF EXISTS ix_eval_ticke_model_horizon_date;  -- typo legacy

-- -----------------------------
-- 3) Small Utility View(s)
-- -----------------------------
CREATE OR REPLACE VIEW latest_prices AS
SELECT DISTINCT ON (ticker) ticker, date, close, volume
FROM prices
ORDER BY ticker, date DESC;

-- 티커별 최근 250 거래일(date) 세트
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
-- 4) App-Facing Views (Reusable data contracts)
-- -----------------------------
-- A) 티커×모델×호라이즌 성능: 전체/최근250 MAE & 방향정확도
CREATE OR REPLACE VIEW prediction_metrics AS
SELECT
  e.ticker,
  e.model_name,
  e.horizon,
  AVG(e.abs_err)::float AS mae_all,
  AVG(CASE WHEN e.dir_corr THEN 1 ELSE 0 END)::float AS acc_all,
  AVG(CASE WHEN l.date IS NOT NULL THEN e.abs_err END)::float AS mae_250d,
  AVG(CASE WHEN l.date IS NOT NULL THEN CASE WHEN e.dir_corr THEN 1 ELSE 0 END END)::float AS acc_250d
FROM prediction_eval e
LEFT JOIN last250_dates l
  ON l.ticker = e.ticker AND l.date = e.date
GROUP BY e.ticker, e.model_name, e.horizon;

-- B) 모델 전반 성능 요약(모델×호라이즌): 전체/최근250
CREATE OR REPLACE VIEW prediction_leaderboard AS
WITH base AS (
  SELECT
    e.model_name,
    e.horizon,
    e.abs_err,
    e.dir_corr,
    (CASE WHEN l.date IS NOT NULL THEN 1 ELSE 0 END) AS in_250
  FROM prediction_eval e
  LEFT JOIN last250_dates l
    ON l.ticker = e.ticker AND l.date = e.date
)
SELECT
  model_name,
  horizon,
  AVG(abs_err)::float AS mae_all,
  AVG(CASE WHEN dir_corr THEN 1 ELSE 0 END)::float AS acc_all,
  AVG(CASE WHEN in_250 = 1 THEN abs_err END)::float AS mae_250d,
  AVG(CASE WHEN in_250 = 1 THEN CASE WHEN dir_corr THEN 1 ELSE 0 END END)::float AS acc_250d
FROM base
GROUP BY model_name, horizon;

-- C) 시그널 뷰: 전일 종가 대비 예측 변화율(앱에서 임계값 θ로 필터링)
CREATE OR REPLACE VIEW signals_view AS
SELECT
  p.ticker,
  p.date,
  p.model_name,
  p.horizon,
  p.y_pred,
  e.y_true,              -- (참고) 다음날 종가
  -- 전일(예측 기준일) 종가: prediction_eval이 이미 prices와 매칭한 close_asof를 보유
  -- 만약 별도 컬럼으로 저장하지 않았다면, 필요 시 prices를 조인해도 됨
  -- 여기서는 prediction_eval의 y_true와 y_pred로 변화율을 산출하는 방식 사용
  NULL::numeric AS close_asof,  -- 유지보수 편의용 placeholder (필요시 가격 조인으로 대체)
  NULLIF(NULLIF(0,0),0) AS dummy_null, -- no-op (placeholder to remind maintainers)
  CASE
    WHEN (SELECT pe2.y_true FROM prediction_eval pe2
          WHERE pe2.date = p.date AND pe2.ticker = p.ticker
            AND pe2.model_name = p.model_name AND pe2.horizon = p.horizon) IS NULL
    THEN NULL
    ELSE (
      p.y_pred - (
        SELECT pe3.y_true FROM prediction_eval pe3
        WHERE pe3.date = p.date AND pe3.ticker = p.ticker
          AND pe3.model_name = p.model_name AND pe3.horizon = p.horizon
      )
    )
  END AS y_pred_abs_change,
  CASE
    WHEN (
      SELECT pe4.y_true FROM prediction_eval pe4
      WHERE pe4.date = p.date AND pe4.ticker = p.ticker
        AND pe4.model_name = p.model_name AND pe4.horizon = p.horizon
    ) IS NULL
    OR (
      SELECT pe5.y_true FROM prediction_eval pe5
      WHERE pe5.date = p.date AND pe5.ticker = p.ticker
        AND pe5.model_name = p.model_name AND pe5.horizon = p.horizon
    ) = 0
    THEN NULL
    ELSE (
      p.y_pred - (
        SELECT pe6.y_true FROM prediction_eval pe6
        WHERE pe6.date = p.date AND pe6.ticker = p.ticker
          AND pe6.model_name = p.model_name AND pe6.horizon = p.horizon
      )
    ) / (
      SELECT pe7.y_true FROM prediction_eval pe7
      WHERE pe7.date = p.date AND pe7.ticker = p.ticker
        AND pe7.model_name = p.model_name AND pe7.horizon = p.horizon
    )
  END AS y_pred_pct_change
FROM predictions p
WHERE p.model_name LIKE 'safe_%';

-- 참고: 위 signals_view는 간단한 self-subquery 버전입니다.
-- 대체로 앱에서 임계값 필터링:
--   SELECT * FROM signals_view
--   WHERE ABS(y_pred_pct_change) >= :theta
--   ORDER BY ABS(y_pred_pct_change) DESC
--   LIMIT :top_n;

-- =====================================================================
-- END OF SCHEMA
-- =====================================================================
