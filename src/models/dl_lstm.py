# src/models/dl_lstm.py
from __future__ import annotations
import os
import numpy as np
import pandas as pd

# TF 로깅 억제 (선택)
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "2")

try:
    import tensorflow as tf
    from tensorflow.keras import layers, callbacks, models
except Exception as e:  # 텐서플로 미설치/미로딩 시
    class DLNotAvailable(RuntimeError): ...
    raise DLNotAvailable(f"TensorFlow not available: {e}")

from sklearn.preprocessing import MinMaxScaler
from typing import Tuple

# 재현성(완벽하진 않지만 기본 고정)
_SEED = 42
np.random.seed(_SEED)
tf.random.set_seed(_SEED)


class DLNotAvailable(RuntimeError):
    """predict_daily에서 import 하는 예외 클래스(호환용)."""
    pass


def _make_supervised(y: np.ndarray, window: int) -> Tuple[np.ndarray, np.ndarray]:
    """
    1차원 시계열 y(길이 N) -> (X, y_next)
    X: (N-window, window, 1), y_next: (N-window,)
    """
    X, Y = [], []
    for i in range(window, len(y)):
        X.append(y[i - window:i])
        Y.append(y[i])
    X = np.asarray(X, dtype=np.float32)
    Y = np.asarray(Y, dtype=np.float32)
    # LSTM 입력 차원 (batch, time, features)
    return X.reshape((-1, window, 1)), Y


def _build_model(window: int) -> tf.keras.Model:
    """
    간단하고 빠른 LSTM 회귀 모델.
    """
    inp = layers.Input(shape=(window, 1))
    x = layers.LSTM(32, return_sequences=False)(inp)
    x = layers.Dense(16, activation="relu")(x)
    out = layers.Dense(1)(x)
    model = models.Model(inputs=inp, outputs=out)
    model.compile(optimizer="adam", loss="mae", metrics=["mae"])
    return model


def predict_next_day_close(
    close_series: np.ndarray | pd.Series,
    window: int = 20,
    epochs: int = 12,
    batch_size: int = 32,
    patience: int = 3,
) -> float:
    """
    종가 시계열로 LSTM을 학습한 뒤, 마지막 구간을 사용해 '다음날 종가' 1-step 예측을 반환.

    Parameters
    ----------
    close_series : array-like
        종가 시계열(최근이 마지막). 길이는 최소 window+100 정도 권장.
    window : int
        LSTM 입력 시퀀스 길이.
    epochs, batch_size, patience : 학습 하이퍼파라미터.

    Returns
    -------
    float : 예측 종가(원 스케일)
    """
    # 입력 정리
    y = np.asarray(close_series, dtype=np.float32)
    y = y[~np.isnan(y)]
    if len(y) < window + 5:
        raise ValueError(f"Not enough history for DL. len={len(y)}, window={window}")

    # 스케일링(0~1)
    scaler = MinMaxScaler()
    y_scaled = scaler.fit_transform(y.reshape(-1, 1)).flatten()

    # 지도학습 데이터
    X, Y = _make_supervised(y_scaled, window)
    # 매우 긴 시계열이면 최근 2~3천 포인트로 제한해서 속도 유지(선택)
    MAX_SAMPLES = 3000
    if len(X) > MAX_SAMPLES:
        X = X[-MAX_SAMPLES:]
        Y = Y[-MAX_SAMPLES:]

    # 학습/검증 분리(마지막 10% 검증)
    n = len(X)
    split = max(int(n * 0.9), 1)
    X_tr, Y_tr = X[:split], Y[:split]
    X_va, Y_va = X[split:], Y[split:] if split < n else (X[:1], Y[:1])  # 최소 보장

    model = _build_model(window)
    es = callbacks.EarlyStopping(monitor="val_mae", patience=patience, restore_best_weights=True, verbose=0)

    model.fit(
        X_tr, Y_tr,
        validation_data=(X_va, Y_va),
        epochs=epochs,
        batch_size=batch_size,
        callbacks=[es],
        verbose=0,
    )

    # 다음날 입력 윈도우 = 최근 window 길이
    last_win = y_scaled[-window:].reshape(1, window, 1)
    yhat_scaled = float(model.predict(last_win, verbose=0).squeeze())
    # 역스케일
    yhat = scaler.inverse_transform([[yhat_scaled]])[0, 0]
    return float(yhat)
