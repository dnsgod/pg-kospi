from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "ds",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="airlofw_etl_daily",   # <- 네가 쓰는 DAG ID
    default_args=DEFAULT_ARGS,
    schedule="0 6 * * 1-5",
    start_date=datetime(2025, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["portfolio","daily"],
) as dag:

    # 공통 환경 변수 (주가 DB + PYTHONPATH)
    common_env = {
        "DB_HOST": os.getenv("DB_HOST"),
        "DB_PORT": os.getenv("DB_PORT"),
        "DB_NAME": os.getenv("DB_NAME"),
        "DB_USER": os.getenv("DB_USER"),
        "DB_PASS": os.getenv("DB_PASS"),
        "PYTHONPATH": "/opt/project",  # ← src를 찾게 해줌
        # 선택: 경고 소음 줄이기
        # "PYTHONWARNINGS": "ignore::SyntaxWarning",
    }

    ingest = BashOperator(
        task_id="ingest_daily",
        bash_command="cd /opt/project && python -m src.pipeline.ingest_daily",  # ← 작업 디렉토리 보장
        env=common_env,
    )

    predict = BashOperator(
        task_id="predict_daily",
        bash_command="cd /opt/project && python -m src.pipeline.predict_daily",
        env=common_env,
    )

    evaluate = BashOperator(
        task_id="eval_daily",
        bash_command="cd /opt/project && python -m src.pipeline.eval_daily",
        env=common_env,
    )

    ingest >> predict >> evaluate
