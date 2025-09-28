# airflow/dags/airflow_etl.py
from __future__ import annotations
import os, sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.timezone import make_aware
from zoneinfo import ZoneInfo

DAG_ID = "airflow_etl_daily"
KST = ZoneInfo("Asia/Seoul")
PROJECT_DIR = os.getenv("PROJECT_DIR", "/opt/project")
if PROJECT_DIR and PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

PY_CMD = "python"

default_args = {
    "owner": "ds",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=2),
}

common_env = {
    "TZ": os.getenv("TZ", "Asia/Seoul"),
    "DB_HOST": os.getenv("DB_HOST"),
    "DB_PORT": os.getenv("DB_PORT"),
    "DB_NAME": os.getenv("DB_NAME"),
    "DB_USER": os.getenv("DB_USER"),
    "DB_PASS": os.getenv("DB_PASS"),
}

with DAG(
    dag_id=DAG_ID,
    description="KOSPI200 ETL: refresh -> incremental -> predict -> eval -> report",
    default_args=default_args,
    start_date=make_aware(datetime(2025, 9, 1), timezone=KST),
    schedule_interval="0 6 * * 1-5",  # 평일 06:00 (KST)
    catchup=False,
    max_active_runs=1,
    tags=["portfolio", "etl", "daily"],
) as dag:

    refresh_tickers = BashOperator(
        task_id="refresh_tickers",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"export PYTHONPATH={PROJECT_DIR} && "
            f"{PY_CMD} -m src.ingest.refresh_tickers"
        ),
        env=common_env,
        execution_timeout=timedelta(minutes=30),
    )

    incremental_prices = BashOperator(
        task_id="incremental_prices",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"export PYTHONPATH={PROJECT_DIR} && "
            f"{PY_CMD} -m src.ingest.incremental_prices"
        ),
        env=common_env,
        execution_timeout=timedelta(hours=1),
    )

    predict_daily = BashOperator(
        task_id="predict_daily",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"export PYTHONPATH={PROJECT_DIR} && "
            f"{PY_CMD} -m src.pipeline.predict_daily"
        ),
        env=common_env,
        execution_timeout=timedelta(hours=2),
    )

    eval_daily = BashOperator(
        task_id="eval_daily",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"export PYTHONPATH={PROJECT_DIR} && "
            f"{PY_CMD} -m src.pipeline.ensemble_and_eval"
        ),
        env=common_env,
        execution_timeout=timedelta(hours=2),
    )

    report_daily = BashOperator(
        task_id="report_daily",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"export PYTHONPATH={PROJECT_DIR} && "
            f"""{PY_CMD} -c "from src.pipeline.signals_report_daily import run; run()" """
        ),
        env=common_env,
        execution_timeout=timedelta(minutes=30),
    )

    refresh_tickers >> incremental_prices >> predict_daily >> eval_daily >> report_daily
