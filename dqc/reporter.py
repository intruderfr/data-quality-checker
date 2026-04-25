"""Reporters: console (rich), JSON, and HTML."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from rich.console import Console
from rich.table import Table
from rich.text import Text

from .checks import CheckResult, summarize


# ---------------------------------------------------------------------------
# Console
# ---------------------------------------------------------------------------

def render_console(
    results: List[CheckResult],
    dataset_path: str,
    rows: int,
    cols: int,
    console: Console | None = None,
) -> int:
    """Render to the terminal. Returns the process exit code."""
    console = console or Console()
    counts, exit_code = summarize(results)

    header = f"data-quality-checker — {dataset_path} ({rows:,} rows, {cols} cols)"
    console.print(Text(header, style="bold"))
    console.print()

    table = Table(show_header=True, header_style="bold", expand=False)
    table.add_column("Check", overflow="fold", min_width=24)
    table.add_column("Status", justify="center", min_width=6)
    table.add_column("Severity", justify="center", min_width=8)
    table.add_column("Message", overflow="fold")

    for r in results:
        table.add_row(
            r.name,
            _styled_status(r.status),
            r.severity,
            r.message,
        )
    console.print(table)
    console.print()

    style = "red bold" if exit_code == 2 else ("yellow bold" if exit_code == 1 else "green bold")
    console.print(
        Text(
            f"Result: {counts['errors']} error(s), {counts['warnings']} warning(s), "
            f"{counts['passed']} passed. Exit {exit_code}.",
            style=style,
        )
    )
    return exit_code


def _styled_status(status: str) -> Text:
    if status == "OK":
        return Text("OK", style="green bold")
    if status == "WARN":
        return Text("WARN", style="yellow bold")
    if status == "FAIL":
        return Text("FAIL", style="red bold")
    return Text(status, style="cyan")


# ---------------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------------

def render_json(
    results: List[CheckResult],
    dataset_path: str,
    rows: int,
    cols: int,
) -> str:
    counts, exit_code = summarize(results)
    payload: Dict[str, Any] = {
        "summary": {**counts, "exit_code": exit_code},
        "dataset": {"path": dataset_path, "rows": rows, "columns": cols},
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "results": [_jsonable(r.to_dict()) for r in results],
    }
    return json.dumps(payload, indent=2, default=str)


def _jsonable(value: Any) -> Any:
    """Convert numpy scalars and other non-JSON values to plain Python."""
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if hasattr(value, "item") and callable(value.item):
        try:
            return value.item()
        except Exception:
            return str(value)
    return value


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------

_HTML_TEMPLATE = """<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"/>
<title>dqc report — {dataset}</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
          margin: 2rem; color: #1f2328; background: #f6f8fa; }}
  h1 {{ margin-bottom: 0.25rem; }}
  .meta {{ color: #57606a; font-size: 0.95rem; margin-bottom: 1.25rem; }}
  .summary {{ display: flex; gap: 1rem; margin: 1rem 0 2rem 0; flex-wrap: wrap; }}
  .card {{ padding: 0.75rem 1rem; background: #fff; border: 1px solid #d0d7de;
           border-radius: 6px; min-width: 110px; }}
  .card .label {{ font-size: 0.8rem; color: #57606a; text-transform: uppercase; }}
  .card .value {{ font-size: 1.6rem; font-weight: 600; }}
  table {{ border-collapse: collapse; background: #fff; width: 100%;
           border: 1px solid #d0d7de; border-radius: 6px; overflow: hidden; }}
  th, td {{ padding: 0.55rem 0.85rem; text-align: left; vertical-align: top;
            border-bottom: 1px solid #eaeef2; font-size: 0.92rem; }}
  th {{ background: #f6f8fa; font-weight: 600; }}
  .status {{ display: inline-block; padding: 0.1rem 0.5rem; border-radius: 999px;
             font-size: 0.78rem; font-weight: 600; text-transform: uppercase; }}
  .status.OK   {{ background: #dafbe1; color: #1a7f37; }}
  .status.WARN {{ background: #fff8c5; color: #9a6700; }}
  .status.FAIL {{ background: #ffebe9; color: #cf222e; }}
  .status.INFO {{ background: #ddf4ff; color: #0969da; }}
  details summary {{ cursor: pointer; color: #0969da; }}
  pre {{ background: #f6f8fa; padding: 0.5rem; border-radius: 4px; font-size: 0.85rem;
         overflow: auto; }}
  footer {{ margin-top: 2rem; color: #57606a; font-size: 0.8rem; }}
</style>
</head><body>
<h1>dqc report</h1>
<div class="meta">{dataset} — {rows:,} rows, {cols} columns — generated {generated_at}</div>
<div class="summary">
  <div class="card"><div class="label">Total</div><div class="value">{total}</div></div>
  <div class="card"><div class="label">Passed</div><div class="value" style="color:#1a7f37">{passed}</div></div>
  <div class="card"><div class="label">Warnings</div><div class="value" style="color:#9a6700">{warnings}</div></div>
  <div class="card"><div class="label">Errors</div><div class="value" style="color:#cf222e">{errors}</div></div>
  <div class="card"><div class="label">Exit code</div><div class="value">{exit_code}</div></div>
</div>
<table>
  <thead><tr><th>Check</th><th>Status</th><th>Severity</th><th>Message</th><th>Details</th></tr></thead>
  <tbody>
{rows_html}
  </tbody>
</table>
<footer>
  Generated by <a href="https://github.com/intruderfr/data-quality-checker">data-quality-checker</a>.
</footer>
</body></html>
"""


def render_html(
    results: List[CheckResult],
    dataset_path: str,
    rows: int,
    cols: int,
) -> str:
    counts, exit_code = summarize(results)
    rows_html = "\n".join(_html_row(r) for r in results)
    return _HTML_TEMPLATE.format(
        dataset=_escape(dataset_path),
        rows=rows,
        cols=cols,
        generated_at=datetime.utcnow().isoformat(timespec="seconds") + "Z",
        total=counts["total"],
        passed=counts["passed"],
        warnings=counts["warnings"],
        errors=counts["errors"],
        exit_code=exit_code,
        rows_html=rows_html,
    )


def _html_row(r: CheckResult) -> str:
    details_block = ""
    if r.details:
        details_block = (
            "<details><summary>view</summary><pre>"
            + _escape(json.dumps(_jsonable(r.details), indent=2, default=str))
            + "</pre></details>"
        )
    return (
        "    <tr>"
        f"<td><code>{_escape(r.name)}</code><br><small>{_escape(r.type)}</small></td>"
        f"<td><span class=\"status {r.status}\">{r.status}</span></td>"
        f"<td>{_escape(r.severity)}</td>"
        f"<td>{_escape(r.message)}</td>"
        f"<td>{details_block}</td>"
        "</tr>"
    )


def _escape(text: Any) -> str:
    s = str(text)
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def write_report(
    fmt: str,
    results: List[CheckResult],
    dataset_path: str,
    rows: int,
    cols: int,
    out_path: str | Path | None = None,
    console: Console | None = None,
) -> int:
    """Write a report in the requested format. Returns the exit code."""
    fmt = fmt.lower()
    counts, exit_code = summarize(results)

    if fmt == "console":
        return render_console(results, dataset_path, rows, cols, console=console)

    if fmt == "json":
        text = render_json(results, dataset_path, rows, cols)
        _emit(text, out_path)
        return exit_code

    if fmt == "html":
        text = render_html(results, dataset_path, rows, cols)
        _emit(text, out_path)
        return exit_code

    raise ValueError(f"Unknown report format: {fmt!r}")


def _emit(text: str, out_path: str | Path | None) -> None:
    if out_path:
        Path(out_path).write_text(text, encoding="utf-8")
    else:
        print(text)
