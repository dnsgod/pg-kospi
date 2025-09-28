# -*- coding: utf-8 -*-
"""
KOSPI100 우주 수집 (티커+이름)
- 1순위: pykrx 지수 1028 구성종목에서 티커 얻기
- 이름 매핑: FDR KRX 상장목록으로 일괄 매핑 (빠르고 안정적)
- 폴백: FDR KRX 상장목록 상위 100개
"""
from __future__ import annotations
from datetime import date, timedelta
from typing import List, Tuple
import pandas as pd

def get_kospi100(today: date | None = None) -> pd.DataFrame:
    tickers: List[str] = []
    d = today or date.today()

    # 1) pykrx에서 티커
    try:
        from pykrx import stock
        for i in range(5):  # 직전 영업일까지 백오프
            ds = (d - timedelta(days=i)).strftime("%Y%m%d")
            try:
                tickers = stock.get_index_portfolio_deposit_file("1028", ds) or []
                if tickers:
                    break
            except Exception:
                pass
    except Exception:
        pass

    # 2) 이름 매핑 (FDR KRX)
    try:
        import FinanceDataReader as fdr
        krx = fdr.StockListing("KRX")[["Code", "Name", "Market"]].rename(
            columns={"Code": "ticker", "Name": "name", "Market": "market"}
        )
        krx["ticker"] = krx["ticker"].astype(str).str.zfill(6)

        if not tickers:
            # 폴백: 전체 KRX 중 숫자 6자리만, 상위 100개 사용
            base = krx[krx["ticker"].str.fullmatch(r"\d{6}")].copy()
            return base.head(100).reset_index(drop=True)

        df = pd.DataFrame({"ticker": tickers})
        out = df.merge(krx, on="ticker", how="left")
        # 이름 없으면 임시로 티커 사용
        out["name"] = out["name"].fillna(out["ticker"])
        out["market"] = out["market"].fillna("KRX")
        return out.reset_index(drop=True)

    except Exception:
        # FDR도 실패하면 최소한 티커만이라도 반환
        return pd.DataFrame({"ticker": tickers, "name": tickers, "market": "KRX"})

if __name__ == "__main__":
    print(get_kospi100().head())
