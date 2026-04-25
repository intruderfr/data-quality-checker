# data-quality-checker (`dqc`)

A small, fast, dependency-light **data quality checker** for tabular files. Define rules in YAML, point it at a CSV/JSON-Lines/Parquet file, get a pass/fail report (console, JSON, or HTML).

Built for data engineers, analysts, and platform teams who want a deterministic gate to slot into pipelines, pre-commit hooks, or CI/CD without standing up a Great Expectations server.

## Features

- **Built-in checks**: row count, null rate, uniqueness, distinct-value cap, allowed values (enum), regex match, numeric range, datetime parseability, type assertion, custom `pandas.query` predicates.
- **Schema validation**: required columns, forbidden columns, expected dtypes.
- **Three input formats**: CSV, JSON Lines (`.jsonl` / `.ndjson`), Parquet.
- **Three output formats**: rich console summary, machine-readable JSON, standalone HTML report.
- **Exit codes** suitable for CI: `0` all green, `1` warnings only, `2` any failure.
- **Severity levels** per check (`error` / `warn` / `info`) so you can stage rollouts.
- **Zero hidden state**: no DB, no daemon, no profile uploads. One file in, one report out.

## Install

```bash
pip install -e .
# or after publishing
pip install data-quality-checker
```

Python 3.9+. Hard deps: `pandas`, `pyyaml`, `rich`. Optional: `pyarrow` (Parquet), `jinja2` (HTML report — falls back to a built-in template if absent).

## Quick start

```bash
dqc run --config examples/orders.yaml --data examples/orders.csv
```

Sample console output:

```
data-quality-checker — examples/orders.csv (1,000 rows, 7 cols)

  Schema           OK
  Row count        OK    (1000 in [100, 100000])
  null:order_id    OK    (0.00% <= 0.00%)
  unique:order_id  OK    (1000 distinct / 1000 rows)
  range:amount     FAIL  3 rows outside [0, 10000]  -> rows 117, 402, 889
  regex:email      WARN  12 rows did not match ^[^@]+@[^@]+\.[^@]+$
  enum:status      OK    {NEW, PAID, REFUNDED}

Result: 1 error, 1 warning. Exit 2.
```

## Config format

```yaml
# examples/orders.yaml
dataset:
  format: csv          # csv | jsonl | parquet
  csv_options:
    delimiter: ","
    encoding: utf-8

schema:
  required: [order_id, customer_id, amount, status, email, created_at]
  forbidden: [internal_notes]
  dtypes:
    order_id: string
    amount: float
    created_at: datetime

checks:
  - name: row count sanity
    type: row_count
    min: 100
    max: 100000
    severity: error

  - name: null:order_id
    type: null_rate
    column: order_id
    max_pct: 0.0
    severity: error

  - name: unique:order_id
    type: unique
    column: order_id
    severity: error

  - name: range:amount
    type: range
    column: amount
    min: 0
    max: 10000
    severity: error

  - name: regex:email
    type: regex
    column: email
    pattern: '^[^@\s]+@[^@\s]+\.[^@\s]+$'
    severity: warn

  - name: enum:status
    type: enum
    column: status
    allowed: [NEW, PAID, REFUNDED, CANCELLED]
    severity: error

  - name: distinct cap on country
    type: distinct_count
    column: country
    max: 250
    severity: warn

  - name: created_at parses as date
    type: datetime
    column: created_at
    severity: error

  - name: refunds are negative
    type: query
    expression: "status == 'REFUNDED' and amount > 0"
    expect: 0          # expected row count (0 means no rows should match)
    severity: error
```

## Output formats

```bash
dqc run --config orders.yaml --data orders.csv --format console   # default
dqc run --config orders.yaml --data orders.csv --format json --out report.json
dqc run --config orders.yaml --data orders.csv --format html --out report.html
```

The JSON shape is stable and intended for downstream tools:

```json
{
  "summary": {"total": 8, "passed": 6, "warnings": 1, "errors": 1, "exit_code": 2},
  "dataset": {"path": "orders.csv", "rows": 1000, "columns": 7},
  "results": [
    {"name": "range:amount", "type": "range", "status": "FAIL",
     "severity": "error", "details": {"violations": 3, "sample_rows": [117, 402, 889]}}
  ]
}
```

## CI usage

```yaml
# .github/workflows/dq.yml
- name: Data quality gate
  run: |
    pip install data-quality-checker
    dqc run --config dq/rules.yaml --data data/orders.csv --format json --out dq.json
```

Exit code `2` fails the job on any error-severity check; `1` is warnings only (treat as advisory).

## Why not Great Expectations / Soda / Pandera?

Those are excellent at the heavy end. `dqc` is for the small end — a single binary on the path, one config file, one CSV, one report. Use it as the cheap pre-flight before you reach for the bigger tools.

## License

MIT — see `LICENSE`.

## Author

**Aslam Ahamed** — Head of IT @ Prestige One Developments, Dubai.
LinkedIn: <https://www.linkedin.com/in/aslam-ahamed/>
