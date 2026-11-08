"""Model artifacts under ml/artifacts/<model_version>/ and the latest-model pointer."""

from __future__ import annotations

import hashlib
import json
import pickle
from pathlib import Path

from catboost import CatBoostRegressor

from ml.features.spec import FEATURE_COLUMNS
from ml.features.transform import FeaturePipeline

LATEST = "latest.json"


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def pin_metadata(model: CatBoostRegressor, model_version: str) -> None:
    """CatBoost writes a random guid and the wall-clock finish time into every model file."""
    metadata = model.get_metadata()
    metadata["model_guid"] = model_version
    if "train_finish_time" in metadata:
        del metadata["train_finish_time"]


def save_artifacts(result, root: Path) -> dict:
    from ml.model.train import model_card

    directory = root / result.model_version
    directory.mkdir(parents=True, exist_ok=True)
    model_path = directory / "model.cbm"
    pin_metadata(result.model, result.model_version)
    result.model.save_model(str(model_path), format="cbm")
    card = model_card(result, sha256_file(model_path))
    (directory / "model_card.json").write_text(json.dumps(card, ensure_ascii=False, indent=2, default=str) + "\n")
    with (directory / "baselines.pkl").open("wb") as handle:
        pickle.dump({name: entry["model"] for name, entry in result.baselines.items()}, handle)
    return card


def publish_latest(root: Path, card: dict) -> None:
    """Point the scorer at this version. Call only after the registry row is committed."""
    (root / LATEST).write_text(json.dumps(
        {"model_name": card["model_name"], "model_version": card["model_version"]}, indent=2) + "\n")


def load_model(root: Path, model_version: str | None = None) -> tuple[CatBoostRegressor, dict, FeaturePipeline]:
    if model_version is None:
        model_version = json.loads((root / LATEST).read_text())["model_version"]
    directory = root / model_version
    card = json.loads((directory / "model_card.json").read_text())
    model_path = directory / card["model_file"]
    if sha256_file(model_path) != card["model_sha256"]:
        raise ValueError(f"{model_path} does not match the sha256 in its model card")
    if tuple(card["feature_columns"]) != FEATURE_COLUMNS:
        raise ValueError("model was trained on a different feature list than this code produces")
    model = CatBoostRegressor()
    model.load_model(str(model_path), format="cbm")
    if list(model.feature_names_) != card["feature_columns"]:
        raise ValueError("model file feature order differs from its model card")
    return model, card, FeaturePipeline.from_dict(card["categorical_encoding"])
