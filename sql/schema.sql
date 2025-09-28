
-- 1) 티커 마스터
CREATE TABLE IF NOT EXISTS tickers (
  ticker     varchar(6) PRIMARY KEY,
  name       text NOT NULL,
  market     text,
  sector     text,
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- 2) 가격
CREATE TABLE IF NOT EXISTS prices (
  date       date        NOT NULL,
  ticker     varchar(6)  NOT NULL,
  open       numeric,
  high       numeric,
  low        numeric,
  close      numeric,
  adj_close  numeric,
  volume     bigint,
  change     numeric,
  PRIMARY KEY (date, ticker),
  FOREIGN KEY (ticker) REFERENCES tickers(ticker)
);

-- 3) 예측
CREATE TABLE IF NOT EXISTS predictions (
  date       date        NOT NULL,
  ticker     varchar(6)  NOT NULL,
  model_name text        NOT NULL,
  horizon    int         NOT NULL DEFAULT 1,
  y_pred     numeric     NOT NULL,
  PRIMARY KEY (date, ticker, model_name, horizon),
  FOREIGN KEY (ticker) REFERENCES tickers(ticker)
);

-- 4) 평가
CREATE TABLE IF NOT EXISTS evals (
  model_name text        NOT NULL,
  asof_date  date        NOT NULL,
  metric     text        NOT NULL,
  value      numeric     NOT NULL,
  PRIMARY KEY (model_name, asof_date, metric)
);

-- 5) 조회용 뷰(스트림릿/리포트)
CREATE OR REPLACE VIEW predictions_clean AS
SELECT *
FROM predictions
WHERE model_name LIKE 'safe_%' AND horizon = 1;

CREATE OR REPLACE VIEW model_catalog AS
SELECT DISTINCT model_name
FROM predictions_clean
ORDER BY 1;

