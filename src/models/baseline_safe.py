from __future__ import annotations
import numpy as np
import pandas as pd

def ma_next_day_series(y: pd.Series, window: int) -> pd.DataFrame:
    y = pd.Series(y).astype(float)
    ma = y.rolling(window).mean()
    # asof_idx: 예측 기준 인덱스(그 날의 종가로 다음날 예측)
    asof_idx = np.arange(len(y) - 1)  # 마지막 날은 다음날 없음
    y_pred = ma.iloc[asof_idx]
    out = pd.DataFrame({"asof_idx": asof_idx, "y_pred": y_pred.values})
    return out.dropna().reset_index(drop=True)

def ses_next_day_series(y: pd.Series, alpha: float) -> pd.DataFrame:
    y = pd.Series(y).astype(float)
    if y.empty: return pd.DataFrame(columns=["asof_idx","y_pred"])
    s = [y.iloc[0]]
    for i in range(1, len(y)):
        s.append(alpha * y.iloc[i-1] + (1-alpha) * s[-1])
    asof_idx = np.arange(len(y) - 1)
    y_pred = pd.Series(s).iloc[asof_idx]
    out = pd.DataFrame({"asof_idx": asof_idx, "y_pred": y_pred.values})
    return out.dropna().reset_index(drop=True)
