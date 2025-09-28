import os
from sqlalchemy import create_engine

def get_engine():
    user = os.getenv("DB_USER", "kospi")
    pwd  = os.getenv("DB_PASS", "kospi")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "stocks")
    url  = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}"
    # 프리핑으로 끊어진 커넥션 자동 복구
    return create_engine(url, pool_pre_ping=True, future=True)
