"""Built-in quality checks. Each check returns a CheckResult.

A CheckResult has:
  name      — user-facing label
  type      — check type
  status    — "OK" | "WARN" | "FAIL"
  severity  — "error" | "warn" | "info" (from config; only used to map FAIL/WARN)
  message   — short human-readable summary
  details   — structured detail dict (sample rows, percentages, etc.)
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Tuple

import pandas as pd

from .config import CheckConfig, Config, SchemaConfig


@dataclass
class CheckResult:
    name: str
    type: str
    status: str  # "OK" | "WARN" | "FAIL"
    severity: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def run_checks(df: pd.DataFrame, config: Config) -> List[CheckResult]:
    """Run schema validation + all configured checks against a DataFrame."""
    results: List[CheckResult] = []
    results.extend(_run_schema_checks(df, config.schema))
    for check in config.checks:
        try:
            results.append(_dispatch_check(df, check))
        except Exception as exc:  # pragma: no cover - defensive
            results.append(
                CheckResult(
                    name=check.name,
                    type=check.type,
                    status="FAIL",
                    severity=check.severity,
                    message=f"check raised {type(exc).__name__}: {exc}",
                    details={"error": str(exc)},
                )
            )
    return results


def _dispatch_check(df: pd.DataFrame, check: CheckConfig) -> CheckResult:
    handler = _HANDLERS.get(check.type)
    if handler is None:
        raise ValueError(f"No handler registered for check type {check.type!r}")
    return handler(df, check)


# ---------------------------------------------------------------------------
# Schema checks
# ---------------------------------------------------------------------------

def _run_schema_checks(df: pd.DataFrame, schema: SchemaConfig) -> List[CheckResult]:
    out: List[CheckResult] = []
    if not (schema.required or schema.forbidden or schema.dtypes):
        return out

    cols = set(df.columns)

    if schema.required:
        missing = [c for c in schema.required if c not in cols]
        if missing:
            out.append(
                CheckResult(
                    name="schema.required",
                    type="schema",
                    status="FAIL",
                    severity="error",
                    message=f"missing required columns: {missing}",
                    details={"missing": missing},
                )
            )
        else:
            out.append(
                CheckResult(
                    name="schema.required",
                    type="schema",
                    status="OK",
                    severity="error",
                    message=f"all {len(schema.required)} required columns present",
                    details={"required": list(schema.required)},
                )
            )

    if schema.forbidden:
        present = [c for c in schema.forbidden if c in cols]
        if present:
            out.append(
                CheckResult(
                    name="schema.forbidden",
                    type="schema",
                    status="FAIL",
                    severity="error",
                    message=f"forbidden columns present: {present}",
                    details={"present": present},
                )
            )
        else:
            out.append(
                CheckResult(
                    name="schema.forbidden",
                    type="schema",
                    status="OK",
                    severity="error",
                    message=f"no forbidden columns present",
                    details={"forbidden": list(schema.forbidden)},
                )
            )

    if schema.dtypes:
        mismatches: Dict[str, Dict[str, str]] = {}
        for col, expected in schema.dtypes.items():
            if col not in cols:
                continue
            actual = _normalize_dtype(df[col])
            if not _dtype_matches(actual, expected):
                mismatches[col] = {"expected": expected, "actual": actual}
        if mismatches:
            out.append(
                CheckResult(
                    name="schema.dtypes",
                    type="schema",
                    status="FAIL",
                    severity="error",
                    message=f"{len(mismatches)} column(s) have unexpected dtype",
                    details={"mismatches": mismatches},
                )
            )
        else:
            out.append(
                CheckResult(
                    name="schema.dtypes",
                    type="schema",
                    status="OK",
                    severity="error",
                    message=f"all {len(schema.dtypes)} dtype declarations match",
                    details={"checked": list(schema.dtypes)},
                )
            )

    return out


def _normalize_dtype(series: pd.Series) -> str:
    """Map pandas dtype to a friendly bucket name."""
    if pd.api.types.is_bool_dtype(series):
        return "bool"
    if pd.api.types.is_integer_dtype(series):
        return "int"
    if pd.api.types.is_float_dtype(series):
        return "float"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    if pd.api.types.is_string_dtype(series):
        return "string"
    return str(series.dtype)


def _dtype_matches(actual: str, expected: str) -> bool:
    expected = expected.lower()
    if expected in ("str", "string", "text", "object"):
        return actual in ("string", "object")
    if expected in ("int", "integer", "int64", "int32"):
        return actual == "int"
    if expected in ("float", "double", "number", "numeric"):
        return actual in ("int", "float")  # int is acceptable as numeric
    if expected in ("bool", "boolean"):
        return actual == "bool"
    if expected in ("datetime", "date", "timestamp"):
        return actual == "datetime"
    return actual == expected


# ---------------------------------------------------------------------------
# Individual check handlers
# ---------------------------------------------------------------------------

def _check_row_count(df: pd.DataFrame, check: CheckConfig) -> CheckResult:
    n = len(df)
    lo = check.params.get("min")
    hi = check.params.get("max")
    fails: List[str] = []
    if lo is not None and n < lo:
        fails.append(f"row count {n} < min {lo}")
    if hi is not None and n > hi:
        fails.append(f"row count {n} > max {hi}")
    if fails:
        return CheckResult(
            name=check.name, type=check.type, status=_status_from_severity(check.severity),
            severity=check.severity,
            message="; ".join(fails),
            details={"rows": n, "min": lo, "max": hi},
        )
    return CheckResult(
        name=check.name, type=check.type, status="OK", severity=check.severity,
        message=f"{n} rows within bounds",
        details={"rows": n, "min": lo, "max": hi},
    )


def _check_null_rate(df: pd.DataFrame, check: CheckConfig) -> CheckResult:
    column = _required_param(check, "column")
    max_pct = float(check.params.get("max_pct", 0.0))
    if column not in df.columns:
        return _missing_column(check, column)

    total = len(df)
    nulls = int(df[column].isna().sum())
    pct = (nulls / total * 100.0) if total else 0.0

    if pct > max_pct:
        return CheckResult(
            name=check.name, type=check.type, status=_status_from_severity(check.severity),
            severity=check.severity,
            message=f"{pct:.2f}% null > {max_pct:.2f}% allowed ({nulls}/{total})",
            details={"column": column, "nulls": nulls, "total": total, "pct": round(pct, 4), "max_pct": max_pct},
        )
    return CheckResult(
        name=check.name, type=check.type, status="OK", severity=check.severity,
        message=f"{pct:.2f}% null <= {max_pct:.2f}%",
        details={"column": column, "nulls": nulls, "total": total, "pct": round(pct, 4), "max_pct": max_pct},
    )


def _check_unique(df: pd.DataFrame, check: CheckConfig) -> CheckResult:
    column = _required_param(check, "column")
    if column not in df.columns:
        return _missing_column(check, column)

    series = df[column]
    distinct = int(series.nunique(dropna=False))
    total = len(series)
    dup_count = int(total - distinct)

    if dup_count > 0:
        # find a few sample duplicate values
        dup_values = (
            series[series.duplicated(keep=False)]
            .dropna()
            .astype(str)
            .head(5)
            .tolist()
        )
        return CheckResult(
            name=check.name, type=check.type, status=_status_from_severity(check.severity),
            severity=check.severity,
            message=f"{dup_count} duplicate value(s) in {column!r}",
            details={"column": column, "distinct": distinct, "total": total, "sample_duplicates": dup_values},
        )
    return CheckResult(
        name=check.name, type=check.type, status="OK", severity=check.severity,
        message=f"{distinct} distinct / {total} rows — all unique",
        details={"column": column, "distinct": distinct, "total": total},
    )


def _check_distinct_count(df: pd.DataFrame, check: CheckConfig) -> CheckResult:
    column = _required_param(check, "column")
    if column not in df.columns:
        return _missing_column(check, column)

    distinct = int(df[column].nunique(dropna=False))
    lo = check.params.get("min")
    hi = check.params.get("max")
    fails: List[str] = []
    if lo is not None and distinct < lo:
        fails.append(f"distinct {distinct} < min {lo}")
    if hi is not None and distinct > hi:
        fails.append(f"distinct {distinct} > max {hi}")

    if fails:
        return CheckResult(
            name=check.name, type=check.type, status=_status_from_severity(check.severity),
            severity=check.severity,
            message="; ".join(fails),
            details={"column": column, "distinct": distinct, "min": lo, "max": hi},
        )
    return CheckResult(
        name=check.name, type=check.type, status="OK", severity=check.severity,
        message=f"{distinct} distinct values within bounds",
        details={"column": column, "distinct": distinct, "min": lo, "max": hi},
    )


def _check_enum(df: pd.DataFrame, check: CheckConfig) -> CheckResult:
    column = _required_param(check, "column")
    allowed = check.params.get("allowed")
    if allowed is None or not isinstance(allowed, list):
        raise ValueError(f"Check {check.name!r}: `allowed` (list) is required for enum.")
    if column not in df.columns:
        return _missing_column(check, column)

    allowed_set = set(allowed)
    series = df[column].dropna()
    bad_mask = ~series.isin(allowed_set)
    bad_count = int(bad_mask.sum())

    if bad_count > 0:
        bad_values = sorted(series[bad_mask].astype(str).unique().tolist())[:10]
        return CheckResult(
            name=check.name, type=check.type, status=_status_from_severity(check.severity),
            severity=check.severity,
            message=f"{bad_count} row(s) with disallowed values",
            details={"column": column, "violations": bad_count, "sample_bad_values": bad_values, "allowed": list(allowed)},
        )
    return CheckResult(
        name=check.name, type=check.type, status="OK", severity=check.severity,
        message=f"all values within allowed set ({len(allowed_set)} options)",
        details={"column": column, "allowed": list(allowed)},
    )


def _check_regex(df: pd.DataFrame, check: CheckConfig) -> CheckResult:
    column = _required_param(check, "column")
    pattern = _required_param(check, "pattern")
    if column not in df.columns:
        return _missing_column(check, column)

    try:
        compiled = re.compile(pattern)
    except re.error as e:
        raise ValueError(f"Check {check.name!r}: invalid regex {pattern!r}: {e}") from e

    series = df[column].dropna().astype(str)
    bad_mask = ~series.map(lambda s: bool(compiled.fullmatch(s)))
    bad_count = int(bad_mask.sum())

    if bad_count > 0:
        sample_rows = series[bad_mask].head(5).index.tolist()
        return CheckResult(
            name=check.name, type=check.type, status=_status_from_severity(check.severity),
            severity=check.severity,
            message=f"{bad_count} row(s) did not match pattern",
            details={"column": column, "pattern": pattern, "violations": bad_count, "sample_rows": sample_rows},
        )
    return CheckResult(
        name=check.name, type=check.type, status="OK", severity=check.severity,
        message=f"all {len(series)} non-null values matched",
        details={"column": column, "pattern": pattern},
    )


def _check_range(df: pd.DataFrame, check: CheckConfig) -> CheckResult:
    column = _required_param(check, "column")
    lo = check.params.get("min")
    hi = check.params.get("max")
    if column not in df.columns:
        return _missing_column(check, column)

    series = pd.to_numeric(df[column], errors="coerce")
    nan_mask = series.isna() & df[column].notna()
    bad_mask = pd.Series([False] * len(series), index=series.index)
    if lo is not None:
        bad_mask |= series < lo
    if hi is not None:
        bad_mask |= series > hi
    # values that failed to parse as numeric also count as range violations
    bad_mask |= nan_mask
    bad_count = int(bad_mask.sum())

    if bad_count > 0:
        sample_rows = bad_mask[bad_mask].head(5).index.tolist()
        return CheckResult(
            name=check.name, type=check.type, status=_status_from_severity(check.severity),
            severity=check.severity,
            message=f"{bad_count} row(s) outside [{lo}, {hi}]",
            details={"column": column, "min": lo, "max": hi, "violations": bad_count, "sample_rows": sample_rows},
        )
    return CheckResult(
        name=check.name, type=check.type, status="OK", severity=check.severity,
        message=f"all values within [{lo}, {hi}]",
        details={"column": column, "min": lo, "max": hi},
    )


def _check_datetime(df: pd.DataFrame, check: CheckConfig) -> CheckResult:
    column = _required_param(check, "column")
    if column not in df.columns:
        return _missing_column(check, column)

    series = df[column]
    parsed = pd.to_datetime(series, errors="coerce", utc=False)
    bad_mask = parsed.isna() & series.notna()
    bad_count = int(bad_mask.sum())

    if bad_count > 0:
        sample_rows = bad_mask[bad_mask].head(5).index.tolist()
        return CheckResult(
            name=check.name, type=check.type, status=_status_from_severity(check.severity),
            severity=check.severity,
            message=f"{bad_count} row(s) failed to parse as datetime",
            details={"column": column, "violations": bad_count, "sample_rows": sample_rows},
        )
    return CheckResult(
        name=check.name, type=check.type, status="OK", severity=check.severity,
        message=f"all non-null values parsed as datetime",
        details={"column": column},
    )


def _check_dtype(df: pd.DataFrame, check: CheckConfig) -> CheckResult:
    column = _required_param(check, "column")
    expected = _required_param(check, "expected")
    if column not in df.columns:
        return _missing_column(check, column)

    actual = _normalize_dtype(df[column])
    if not _dtype_matches(actual, expected):
        return CheckResult(
            name=check.name, type=check.type, status=_status_from_severity(check.severity),
            severity=check.severity,
            message=f"dtype mismatch: expected {expected}, got {actual}",
            details={"column": column, "expected": expected, "actual": actual},
        )
    return CheckResult(
        name=check.name, type=check.type, status="OK", severity=check.severity,
        message=f"dtype is {actual}",
        details={"column": column, "expected": expected, "actual": actual},
    )


def _check_query(df: pd.DataFrame, check: CheckConfig) -> CheckResult:
    expression = _required_param(check, "expression")
    expect = check.params.get("expect", 0)

    try:
        matched = df.query(expression, engine="python")
    except Exception as e:
        raise ValueError(f"Check {check.name!r}: query failed: {e}") from e

    n = len(matched)
    if isinstance(expect, dict):
        lo = expect.get("min")
        hi = expect.get("max")
        fails: List[str] = []
        if lo is not None and n < lo:
            fails.append(f"matched {n} < min {lo}")
        if hi is not None and n > hi:
            fails.append(f"matched {n} > max {hi}")
        if fails:
            return CheckResult(
                name=check.name, type=check.type, status=_status_from_severity(check.severity),
                severity=check.severity,
                message="; ".join(fails),
                details={"expression": expression, "matched": n, "expect": expect, "sample_rows": matched.head(5).index.tolist()},
            )
        return CheckResult(
            name=check.name, type=check.type, status="OK", severity=check.severity,
            message=f"matched {n} rows within expected bounds",
            details={"expression": expression, "matched": n, "expect": expect},
        )

    expected_n = int(expect)
    if n != expected_n:
        return CheckResult(
            name=check.name, type=check.type, status=_status_from_severity(check.severity),
            severity=check.severity,
            message=f"expected {expected_n} matching row(s), found {n}",
            details={"expression": expression, "matched": n, "expect": expected_n, "sample_rows": matched.head(5).index.tolist()},
        )
    return CheckResult(
        name=check.name, type=check.type, status="OK", severity=check.severity,
        message=f"matched {n} row(s) (expected {expected_n})",
        details={"expression": expression, "matched": n, "expect": expected_n},
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HANDLERS = {
    "row_count": _check_row_count,
    "null_rate": _check_null_rate,
    "unique": _check_unique,
    "distinct_count": _check_distinct_count,
    "enum": _check_enum,
    "regex": _check_regex,
    "range": _check_range,
    "datetime": _check_datetime,
    "dtype": _check_dtype,
    "query": _check_query,
}


def _required_param(check: CheckConfig, key: str) -> Any:
    if key not in check.params:
        raise ValueError(f"Check {check.name!r}: missing required parameter {key!r}")
    return check.params[key]


def _missing_column(check: CheckConfig, column: str) -> CheckResult:
    return CheckResult(
        name=check.name, type=check.type, status=_status_from_severity(check.severity),
        severity=check.severity,
        message=f"column {column!r} not found in dataset",
        details={"column": column, "error": "column_not_found"},
    )


def _status_from_severity(severity: str) -> str:
    if severity == "error":
        return "FAIL"
    if severity == "warn":
        return "WARN"
    return "INFO"


def summarize(results: List[CheckResult]) -> Tuple[Dict[str, int], int]:
    """Return summary counts and a process exit code (0 ok, 1 warn, 2 fail)."""
    counts = {"total": len(results), "passed": 0, "warnings": 0, "errors": 0, "info": 0}
    for r in results:
        if r.status == "OK":
            counts["passed"] += 1
        elif r.status == "WARN":
            counts["warnings"] += 1
        elif r.status == "FAIL":
            counts["errors"] += 1
        else:
            counts["info"] += 1

    if counts["errors"] > 0:
        exit_code = 2
    elif counts["warnings"] > 0:
        exit_code = 1
    else:
        exit_code = 0
    return counts, exit_code
