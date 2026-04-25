"""Dataset loaders for CSV, JSON Lines, and Parquet."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pandas as pd

from .config import DatasetConfig


def load_dataset(path: str | Path, dataset: DatasetConfig) -> pd.DataFrame:
    """Load a dataset described by `dataset` from `path` into a DataFrame."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")

    fmt = dataset.format

    if fmt == "csv":
        opts = _csv_kwargs(dataset.csv_options)
        return pd.read_csv(path, **opts)

    if fmt == "jsonl":
        # `lines=True` is the JSONL flag in pandas
        opts = dict(dataset.jsonl_options or {})
        opts.setdefault("lines", True)
        return pd.read_json(path, **opts)

    if fmt == "parquet":
        try:
            import pyarrow  # noqa: F401
        except ImportError as e:
            raise RuntimeError(
                "Parquet support requires `pyarrow`. Install with: pip install data-quality-checker[parquet]"
            ) from e
        return pd.read_parquet(path, **(dataset.parquet_options or {}))

    raise ValueError(f"Unsupported dataset format: {fmt!r}")


def _csv_kwargs(opts: Dict[str, Any]) -> Dict[str, Any]:
    """Translate friendly YAML keys to pandas.read_csv kwargs."""
    out = dict(opts)
    # Friendly aliases
    if "delimiter" in out and "sep" not in out:
        out["sep"] = out.pop("delimiter")
    return out
