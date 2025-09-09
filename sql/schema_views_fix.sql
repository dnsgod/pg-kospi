-- ==== FIX VIEWS to use dir_correct instead of dir_corr =================

-- 1) 최근 250일 날짜 세트 (재생성해도 안전)
CREATE OR REPLACE VIEW public.last250_dates AS
WITH distinct_days AS (
  SELECT DISTINCT ticker, date
  FROM public.prediction_eval
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

-- 2) prediction_metrics : horizon 포함 + dir_correct 사용
DROP VIEW IF EXISTS public.prediction_metrics CASCADE;
CREATE OR REPLACE VIEW public.prediction_metrics AS
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
FROM public.prediction_eval e
LEFT JOIN public.last250_dates l
  ON l.ticker = e.ticker AND l.date = e.date
GROUP BY e.ticker, e.model_name, e.horizon;

-- 3) prediction_leaderboard : dir_correct 사용
DROP VIEW IF EXISTS public.prediction_leaderboard CASCADE;
CREATE OR REPLACE VIEW public.prediction_leaderboard AS
WITH base AS (
  SELECT
    e.model_name,
    e.horizon,
    e.abs_err,
    e.dir_correct,
    (CASE WHEN l.date IS NOT NULL THEN 1 ELSE 0 END) AS in_250
  FROM public.prediction_eval e
  LEFT JOIN public.last250_dates l
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

-- 4) signals_view : e(alias) 조인 명시 + y_true 기준 간단 변화율
DROP VIEW IF EXISTS public.signals_view CASCADE;
CREATE OR REPLACE VIEW public.signals_view AS
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
FROM public.predictions p
JOIN public.prediction_eval e
  ON e.ticker = p.ticker
 AND e.date   = p.date
 AND e.model_name = p.model_name
 AND e.horizon    = p.horizon
WHERE p.model_name LIKE 'safe_%';

-- ==== END FIX ==========================================================
