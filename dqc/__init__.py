"""data-quality-checker: configurable rule-based data quality checks for tabular files."""

from .config import load_config, Config
from .checks import run_checks, CheckResult
from .loader import load_dataset

__version__ = "0.1.0"
__all__ = ["load_config", "Config", "run_checks", "CheckResult", "load_dataset", "__version__"]
