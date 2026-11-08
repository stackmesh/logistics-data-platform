"""Training configuration and artifact locations."""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path

from ml.features.spec import DEFAULT_SPLIT, SplitWindows

MODEL_NAME = "catboost_regressor"
ARTIFACT_ROOT = Path(__file__).resolve().parents[1] / "artifacts"


def artifact_root() -> Path:
    configured = os.environ.get("ML_ARTIFACT_DIR", "").strip()
    return Path(configured) if configured else ARTIFACT_ROOT


@dataclass(frozen=True)
class TrainingConfig:
    model_name: str = MODEL_NAME
    random_seed: int = 42
    loss_function: str = "RMSE"
    eval_metric: str = "RMSE"
    iterations: int = 3000
    learning_rate: float = 0.05
    depth: int = 6
    l2_leaf_reg: float = 3.0
    early_stopping_rounds: int = 200
    validation_days: int = 14
    thread_count: int = 4
    verbose: int = 100

    def validation_start(self, split: SplitWindows = DEFAULT_SPLIT) -> date:
        """Chronological hold-out: the last validation_days of the train window."""
        start = split.test_start - timedelta(days=self.validation_days)
        if start <= split.train_start:
            raise ValueError("validation window leaves no training rows")
        return start

    def catboost_params(self, cat_features: list[str], iterations: int | None = None) -> dict:
        return {
            "loss_function": self.loss_function,
            "eval_metric": self.eval_metric,
            "iterations": iterations or self.iterations,
            "learning_rate": self.learning_rate,
            "depth": self.depth,
            "l2_leaf_reg": self.l2_leaf_reg,
            "random_seed": self.random_seed,
            "thread_count": self.thread_count,
            "verbose": self.verbose,
            "cat_features": cat_features,
            # Rows arrive in loading_date order; CatBoost must not permute them for ordered statistics.
            "has_time": True,
            "allow_writing_files": False,
        }

    def to_dict(self) -> dict:
        return asdict(self)
