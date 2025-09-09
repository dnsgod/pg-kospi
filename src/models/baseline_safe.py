import pandas as pd
from statsmodels.tsa.holtwinters import SimpleExpSmoothing

# ========= 유틸: 다음 거래일 인덱스 매핑 =========
def next_trading_index_map(dates: pd.Series) -> dict[int, int]:
    """
    정렬된 날짜 시리즈를 받아서,
    각 i에 대해 다음 거래일의 인덱스 i+1을 매핑한 dict를 만든다.
    (마지막 인덱스는 다음 날이 없으니 제외)
    """
    idx_map = {}
    for i in range(len(dates) - 1):  # 마지막은 다음날 없음
        idx_map[i] = i + 1
    return idx_map

# ========= 이동평균(누수 방지) =========
def ma_next_day_series(close: pd.Series, window: int = 5) -> pd.DataFrame:
    """
    close: 날짜 오름차순 Series (index는 0..n-1 또는 datetime 둘 다 OK)
    반환: DataFrame(columns=["asof_idx","target_idx","y_pred"])
      - asof_idx: 예측 생성 기준이 된 마지막 관측치의 인덱스(i)
      - target_idx: 예측 대상(다음 거래일)의 인덱스(i+1)
      - y_pred: 예측값
    """
    s = pd.Series(close).astype(float).reset_index(drop=True)  # 0..n-1 인덱스로 강제
    idx_map = next_trading_index_map(s.index)
    rows = []
    for i, j in idx_map.items():
        # i 시점까지의 과거만 사용 (s[:i+1])
        hist = s.iloc[:i+1]
        if len(hist) < window:
            continue  # 히스토리 부족
        yhat = hist.tail(window).mean()  # 미래 미포함
        rows.append({"asof_idx": i, "target_idx": j, "y_pred": float(yhat)})
    return pd.DataFrame(rows)

# ========= SES(누수 방지) =========
def ses_next_day_series(close: pd.Series, alpha: float = 0.4) -> pd.DataFrame:
    """
    Simple Exponential Smoothing을 롤링 방식으로 적용해
    각 시점 i에서 i+1을 예측한다. 미래 데이터 절대 미포함.
    """
    s = pd.Series(close).astype(float).reset_index(drop=True)
    idx_map = next_trading_index_map(s.index)
    rows = []
    # 최소 충분 길이(=5) 전까진 예측 생략
    for i, j in idx_map.items():
        hist = s.iloc[:i+1]                 # i까지의 데이터만
        if len(hist) < 5:
            continue
        # optimized=False + smoothing_level 고정 → 빠르고 안정
        fit = SimpleExpSmoothing(hist).fit(smoothing_level=alpha, optimized=False)
        yhat = float(fit.forecast(1).iloc[0])  # i+1 예측
        rows.append({"asof_idx": i, "target_idx": j, "y_pred": yhat})
    return pd.DataFrame(rows)
