"""Regression metrics: R2, MAPE (%), RMSE (RUB), MAE (RUB)."""

from __future__ import annotations

import numpy as np


def regression_metrics(actual, predicted) -> dict:
    """MAPE skips rows whose actual is not positive and reports how many were skipped."""
    y = np.asarray(actual, dtype=float)
    p = np.asarray(predicted, dtype=float)
    if y.shape != p.shape or y.ndim != 1:
        raise ValueError(f"actual {y.shape} and predicted {p.shape} must be 1-D and equal length")
    if not len(y):
        raise ValueError("no rows to evaluate")
    if not (np.isfinite(y).all() and np.isfinite(p).all()):
        raise ValueError("actual and predicted must be finite")
    error = p - y
    ss_res = float(np.sum(error ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    positive = y > 0
    mape = float(np.mean(np.abs(error[positive]) / y[positive]) * 100) if positive.any() else None
    return {
        "n": int(len(y)),
        "r2": 1 - ss_res / ss_tot if ss_tot > 0 else None,
        "mape_pct": mape,
        "rmse_rub": float(np.sqrt(ss_res / len(y))),
        "mae_rub": float(np.mean(np.abs(error))),
        "mape_rows": int(positive.sum()),
        "mape_skipped_non_positive": int((~positive).sum()),
        "bias_rub": float(np.mean(error)),
        "median_ape_pct": float(np.median(np.abs(error[positive]) / y[positive]) * 100) if positive.any() else None,
    }
