from typing import List,Tuple
from sqlalchemy import text
from src.db.conn import get_engine
import pandas as pd

def _ticker_exists(ticker: str) -> bool:
    """prices 테이블에 티커 존재 여부 확인."""
    eng = get_engine()
    sql = text("SELECT 1 FROM prices WHERE ticker=:t LIMIT 1")
    with eng.connect() as conn:
        row = conn.execute(sql, {"t": ticker.strip()}).fetchone()
    return row is not None

def add_watchlist(ticker: str, validate: bool = True) -> Tuple[bool, str]:
    """관심종목 추가.
    - validate=true: prices에 티커 존재 여부 확인
    - 반환: (성공여부, 메시지)
    """
    t = ticker.strip()
    if not t:
        return False, "빈 티커는 추가할 수 없습니다."
    
    if validate and not _ticker_exists(t):
        return False, f"알 수 없는 티커: {t}"
    
    eng = get_engine()
    sql = text("""
               INSERT INTO watchlist(ticker)
               VALUES (:t)
               ON CONFLICT (ticker) DO NOTHING""")
    with eng.begin() as conn:
        res = conn.execute(sql, {"t": t})
        if res.rowcount == 0:
            return False, f"이미 관심종목에 있는 티커: {t}"
    return True, f"관심종목에 추가됨: {t}"

def list_watchlist() -> List[str]:
    """저장된 관심 종목 티커 최신순 리스트 반환."""
    eng = get_engine()
    sql = text("SELECT ticker FROM watchlist ORDER BY created_at DESC")
    with eng.connect() as conn:
        rows = conn.execute(sql).fetchall()
    return [r[0] for r in rows]

def list_watchlist_df() -> pd.DataFrame:
    """관심종목 이름, 생성일 DataFrame 반환.
    - prices 최신 name 하나 조인 (없는 경우 NULL)"""
    eng = get_engine()
    sql = text("""
        WITH latest_name AS (
            SELECT DISTINCT ON (ticker) ticker, name
            FROM prices
            ORDER BY ticker, date DESC)
               
        SELECT w.ticker, ln.name, w.created_at
        FROM watchlist w
        LEFT JOIN latest_name ln USING (ticker)
        ORDER BY w.created_at DESC""")
    with eng.connect() as conn:
        df = pd.read_sql(sql, conn)
    return df

def remove_watchlist(ticker: str) -> int:
    """관심종목에서 티커 제거.
    - 반환: 삭제된 행 수 (0 또는 1)"""
    t = ticker.strip()
    if not t:
        return 0
    eng = get_engine()
    sql = text("DELETE FROM watchlist WHERE ticker=:t")
    with eng.begin() as conn:
        res = conn.execute(sql, {"t": t})
        return res.rowcount