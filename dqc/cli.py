"""dqc command-line entry point."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Optional

from . import __version__
from .checks import run_checks
from .config import load_config
from .loader import load_dataset
from .reporter import write_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dqc",
        description="Configurable data quality checker for tabular files.",
    )
    parser.add_argument("--version", action="version", version=f"dqc {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Run quality checks against a dataset.")
    run.add_argument("--config", "-c", required=True, help="Path to YAML config file.")
    run.add_argument("--data",   "-d", required=True, help="Path to data file (CSV/JSONL/Parquet).")
    run.add_argument("--format", "-f", default="console",
                     choices=["console", "json", "html"], help="Report format.")
    run.add_argument("--out",    "-o", default=None,
                     help="Output file (json/html only). Prints to stdout if omitted.")
    run.add_argument("--quiet",  "-q", action="store_true",
                     help="Suppress console summary when used with --format json/html.")

    sub.add_parser("list-checks", help="List built-in check types.")

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "list-checks":
        return _cmd_list_checks()
    if args.command == "run":
        return _cmd_run(args)

    parser.print_help()
    return 64


def _cmd_run(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    df = load_dataset(args.data, config.dataset)
    results = run_checks(df, config)

    rows = len(df)
    cols = len(df.columns)

    exit_code = write_report(
        fmt=args.format,
        results=results,
        dataset_path=str(args.data),
        rows=rows,
        cols=cols,
        out_path=args.out,
    )

    if not args.quiet and args.format in ("json", "html") and args.out:
        # Always print a one-line summary to stderr so CI logs aren't silent.
        from .checks import summarize
        counts, _ = summarize(results)
        print(
            f"dqc: wrote {args.format} report to {args.out} — "
            f"{counts['errors']} error(s), {counts['warnings']} warning(s), "
            f"{counts['passed']} passed (exit {exit_code})",
            file=sys.stderr,
        )
    return exit_code


def _cmd_list_checks() -> int:
    from .config import VALID_CHECK_TYPES
    print("Built-in check types:")
    for name in sorted(VALID_CHECK_TYPES):
        print(f"  - {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
