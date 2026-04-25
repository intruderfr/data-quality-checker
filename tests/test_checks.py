"""Unit tests for built-in checks."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path

import pandas as pd
import pytest

from dqc.checks import run_checks, summarize
from dqc.config import (
    CheckConfig,
    Config,
    DatasetConfig,
    SchemaConfig,
    load_config,
)
from dqc.loader import load_dataset
from dqc.reporter import render_html, render_json


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id":      ["a", "b", "c", "d", "e"],
            "amount":  [10, 20, 30, 40, 50],
            "status":  ["NEW", "PAID", "PAID", "REFUNDED", "PAID"],
            "email":   ["x@y.z", "x@y.z", "bad-email", "ok@ok.io", "fine@x.org"],
        }
    )


@pytest.fixture
def empty_config() -> Config:
    return Config(
        dataset=DatasetConfig(),
        schema=SchemaConfig(),
        checks=[],
    )


# ---------------------------------------------------------------------------
# row_count
# ---------------------------------------------------------------------------

def test_row_count_within_bounds(sample_df, empty_config):
    cfg = empty_config
    cfg.checks = [CheckConfig(name="rc", type="row_count", params={"min": 1, "max": 100})]
    [r] = run_checks(sample_df, cfg)
    assert r.status == "OK"
    assert r.details["rows"] == 5


def test_row_count_below_min_fails(sample_df, empty_config):
    cfg = empty_config
    cfg.checks = [CheckConfig(name="rc", type="row_count", params={"min": 10})]
    [r] = run_checks(sample_df, cfg)
    assert r.status == "FAIL"
    assert "min 10" in r.message


def test_row_count_warn_severity(sample_df, empty_config):
    cfg = empty_config
    cfg.checks = [
        CheckConfig(name="rc", type="row_count", severity="warn", params={"max": 1})
    ]
    [r] = run_checks(sample_df, cfg)
    assert r.status == "WARN"


# ---------------------------------------------------------------------------
# null_rate / unique / distinct_count
# ---------------------------------------------------------------------------

def test_null_rate_passes_when_below_threshold(empty_config):
    df = pd.DataFrame({"x": [1, None, 3, 4]})
    cfg = empty_config
    cfg.checks = [
        CheckConfig(name="nr", type="null_rate", params={"column": "x", "max_pct": 50.0})
    ]
    [r] = run_checks(df, cfg)
    assert r.status == "OK"


def test_null_rate_fails_when_above_threshold(empty_config):
    df = pd.DataFrame({"x": [None, None, 3, 4]})
    cfg = empty_config
    cfg.checks = [
        CheckConfig(name="nr", type="null_rate", params={"column": "x", "max_pct": 25.0})
    ]
    [r] = run_checks(df, cfg)
    assert r.status == "FAIL"
    assert r.details["nulls"] == 2


def test_unique_detects_duplicates(empty_config):
    df = pd.DataFrame({"id": ["a", "a", "b"]})
    cfg = empty_config
    cfg.checks = [CheckConfig(name="u", type="unique", params={"column": "id"})]
    [r] = run_checks(df, cfg)
    assert r.status == "FAIL"
    assert "a" in r.details["sample_duplicates"]


def test_unique_all_distinct(sample_df, empty_config):
    cfg = empty_config
    cfg.checks = [CheckConfig(name="u", type="unique", params={"column": "id"})]
    [r] = run_checks(sample_df, cfg)
    assert r.status == "OK"


def test_distinct_count_max(sample_df, empty_config):
    cfg = empty_config
    cfg.checks = [
        CheckConfig(name="d", type="distinct_count", params={"column": "status", "max": 2})
    ]
    [r] = run_checks(sample_df, cfg)
    assert r.status == "FAIL"
    assert r.details["distinct"] == 3


# ---------------------------------------------------------------------------
# enum / regex / range
# ---------------------------------------------------------------------------

def test_enum_passes(sample_df, empty_config):
    cfg = empty_config
    cfg.checks = [
        CheckConfig(
            name="e",
            type="enum",
            params={"column": "status", "allowed": ["NEW", "PAID", "REFUNDED"]},
        )
    ]
    [r] = run_checks(sample_df, cfg)
    assert r.status == "OK"


def test_enum_fails(sample_df, empty_config):
    cfg = empty_config
    cfg.checks = [
        CheckConfig(name="e", type="enum", params={"column": "status", "allowed": ["NEW", "PAID"]})
    ]
    [r] = run_checks(sample_df, cfg)
    assert r.status == "FAIL"
    assert "REFUNDED" in r.details["sample_bad_values"]


def test_regex_passes_and_fails(sample_df, empty_config):
    cfg = empty_config
    cfg.checks = [
        CheckConfig(
            name="re",
            type="regex",
            params={"column": "email", "pattern": r"^[^@\s]+@[^@\s]+\.[^@\s]+$"},
        )
    ]
    [r] = run_checks(sample_df, cfg)
    assert r.status == "FAIL"
    assert r.details["violations"] == 1


def test_range_passes_and_fails(sample_df, empty_config):
    cfg = empty_config
    cfg.checks = [
        CheckConfig(name="r1", type="range", params={"column": "amount", "min": 0, "max": 100}),
        CheckConfig(name="r2", type="range", params={"column": "amount", "min": 0, "max": 25}),
    ]
    [a, b] = run_checks(sample_df, cfg)
    assert a.status == "OK"
    assert b.status == "FAIL"
    assert b.details["violations"] == 3


# ---------------------------------------------------------------------------
# datetime / dtype / query
# ---------------------------------------------------------------------------

def test_datetime_check(empty_config):
    df = pd.DataFrame({"d": ["2024-01-01", "2024-02-15", "not-a-date"]})
    cfg = empty_config
    cfg.checks = [CheckConfig(name="dt", type="datetime", params={"column": "d"})]
    [r] = run_checks(df, cfg)
    assert r.status == "FAIL"
    assert r.details["violations"] == 1


def test_dtype_check(empty_config):
    df = pd.DataFrame({"x": [1, 2, 3]})
    cfg = empty_config
    cfg.checks = [
        CheckConfig(name="dt", type="dtype", params={"column": "x", "expected": "int"})
    ]
    [r] = run_checks(df, cfg)
    assert r.status == "OK"


def test_query_zero_expected(sample_df, empty_config):
    cfg = empty_config
    cfg.checks = [
        CheckConfig(
            name="q",
            type="query",
            params={"expression": "status == 'PAID' and amount > 1000", "expect": 0},
        )
    ]
    [r] = run_checks(sample_df, cfg)
    assert r.status == "OK"


def test_query_finds_violation(sample_df, empty_config):
    cfg = empty_config
    cfg.checks = [
        CheckConfig(
            name="q",
            type="query",
            params={"expression": "status == 'PAID' and amount >= 30", "expect": 0},
        )
    ]
    [r] = run_checks(sample_df, cfg)
    assert r.status == "FAIL"
    assert r.details["matched"] == 2


# ---------------------------------------------------------------------------
# Schema checks
# ---------------------------------------------------------------------------

def test_schema_required_missing(empty_config):
    df = pd.DataFrame({"a": [1]})
    cfg = empty_config
    cfg.schema = SchemaConfig(required=["a", "b", "c"])
    results = run_checks(df, cfg)
    failed = [r for r in results if r.name == "schema.required"]
    assert failed and failed[0].status == "FAIL"
    assert set(failed[0].details["missing"]) == {"b", "c"}


def test_schema_dtypes_match(empty_config):
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    cfg = empty_config
    cfg.schema = SchemaConfig(dtypes={"a": "int", "b": "string"})
    results = run_checks(df, cfg)
    matches = [r for r in results if r.name == "schema.dtypes"]
    assert matches and matches[0].status == "OK"


# ---------------------------------------------------------------------------
# End-to-end: load_config + load_dataset + reporter
# ---------------------------------------------------------------------------

def test_end_to_end_with_examples(tmp_path):
    examples = Path(__file__).resolve().parent.parent / "examples"
    cfg = load_config(examples / "orders.yaml")
    df = load_dataset(examples / "orders.csv", cfg.dataset)
    results = run_checks(df, cfg)
    counts, exit_code = summarize(results)

    assert counts["total"] >= 8
    # we deliberately ship orders.csv with quality issues so the gate fires
    assert counts["errors"] >= 1
    assert exit_code == 2

    j = render_json(results, "orders.csv", len(df), len(df.columns))
    parsed = json.loads(j)
    assert parsed["summary"]["exit_code"] == 2

    h = render_html(results, "orders.csv", len(df), len(df.columns))
    assert "<table>" in h
    assert "FAIL" in h


def test_load_config_rejects_unknown_check_type(tmp_path):
    cfg_path = tmp_path / "bad.yaml"
    cfg_path.write_text(textwrap.dedent("""
        dataset: {format: csv}
        schema: {}
        checks:
          - {name: bad, type: not_a_real_type}
    """).strip())
    with pytest.raises(ValueError, match="unknown type"):
        load_config(cfg_path)


def test_summarize_exit_codes():
    from dqc.checks import CheckResult

    assert summarize([CheckResult("a", "x", "OK", "error", "ok")])[1] == 0
    assert summarize([CheckResult("a", "x", "WARN", "warn", "warn")])[1] == 1
    assert summarize([CheckResult("a", "x", "FAIL", "error", "fail")])[1] == 2
