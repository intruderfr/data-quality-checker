"""Config loader and schema for dqc."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


VALID_SEVERITIES = {"error", "warn", "info"}
VALID_FORMATS = {"csv", "jsonl", "parquet"}
VALID_CHECK_TYPES = {
    "row_count",
    "null_rate",
    "unique",
    "distinct_count",
    "enum",
    "regex",
    "range",
    "datetime",
    "dtype",
    "query",
}


@dataclass
class DatasetConfig:
    format: str = "csv"
    csv_options: Dict[str, Any] = field(default_factory=dict)
    jsonl_options: Dict[str, Any] = field(default_factory=dict)
    parquet_options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SchemaConfig:
    required: List[str] = field(default_factory=list)
    forbidden: List[str] = field(default_factory=list)
    dtypes: Dict[str, str] = field(default_factory=dict)


@dataclass
class CheckConfig:
    name: str
    type: str
    severity: str = "error"
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Config:
    dataset: DatasetConfig
    schema: SchemaConfig
    checks: List[CheckConfig]


def load_config(path: str | Path) -> Config:
    """Load and validate a YAML config file."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    if not isinstance(raw, dict):
        raise ValueError("Config root must be a mapping.")

    return _parse_config(raw)


def _parse_config(raw: Dict[str, Any]) -> Config:
    dataset_raw = raw.get("dataset", {}) or {}
    if not isinstance(dataset_raw, dict):
        raise ValueError("`dataset` must be a mapping.")

    fmt = dataset_raw.get("format", "csv")
    if fmt not in VALID_FORMATS:
        raise ValueError(f"Invalid dataset.format: {fmt!r}. Expected one of {sorted(VALID_FORMATS)}.")

    dataset = DatasetConfig(
        format=fmt,
        csv_options=dataset_raw.get("csv_options", {}) or {},
        jsonl_options=dataset_raw.get("jsonl_options", {}) or {},
        parquet_options=dataset_raw.get("parquet_options", {}) or {},
    )

    schema_raw = raw.get("schema", {}) or {}
    if not isinstance(schema_raw, dict):
        raise ValueError("`schema` must be a mapping.")

    schema = SchemaConfig(
        required=list(schema_raw.get("required", []) or []),
        forbidden=list(schema_raw.get("forbidden", []) or []),
        dtypes=dict(schema_raw.get("dtypes", {}) or {}),
    )

    checks_raw = raw.get("checks", []) or []
    if not isinstance(checks_raw, list):
        raise ValueError("`checks` must be a list.")

    checks: List[CheckConfig] = []
    seen_names: set[str] = set()
    for i, check in enumerate(checks_raw):
        if not isinstance(check, dict):
            raise ValueError(f"Check #{i} must be a mapping.")
        if "type" not in check:
            raise ValueError(f"Check #{i} is missing required field `type`.")

        ctype = check["type"]
        if ctype not in VALID_CHECK_TYPES:
            raise ValueError(
                f"Check #{i} has unknown type {ctype!r}. Expected one of {sorted(VALID_CHECK_TYPES)}."
            )

        name = check.get("name") or f"{ctype}_{i+1}"
        if name in seen_names:
            raise ValueError(f"Duplicate check name: {name!r}")
        seen_names.add(name)

        severity = check.get("severity", "error")
        if severity not in VALID_SEVERITIES:
            raise ValueError(
                f"Check {name!r}: invalid severity {severity!r}. "
                f"Expected one of {sorted(VALID_SEVERITIES)}."
            )

        params = {k: v for k, v in check.items() if k not in ("name", "type", "severity")}
        checks.append(CheckConfig(name=name, type=ctype, severity=severity, params=params))

    return Config(dataset=dataset, schema=schema, checks=checks)
