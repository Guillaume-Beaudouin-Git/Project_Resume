"""
YAML configuration loader.

Usage
-----
>>> cfg = load_config("configs/strategy.yaml")
>>> cfg["rebalance"]["frequency"]
'weekly'
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_config(path: str | Path) -> dict[str, Any]:
    """Load a YAML config file and return it as a nested dict.

    Parameters
    ----------
    path:
        Absolute or relative path to the YAML file.

    Returns
    -------
    dict
        Parsed configuration dictionary.

    Raises
    ------
    FileNotFoundError
        If the config file does not exist.
    yaml.YAMLError
        If the file cannot be parsed.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path.resolve()}")
    with path.open("r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    if cfg is None:
        return {}
    return cfg  # type: ignore[return-value]


def merge_configs(*configs: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge multiple config dicts (later dicts override earlier).

    Parameters
    ----------
    *configs:
        Config dicts in order of increasing priority.

    Returns
    -------
    dict
        Merged configuration.
    """
    result: dict[str, Any] = {}
    for cfg in configs:
        _deep_merge(result, cfg)
    return result


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> None:
    """In-place deep merge of ``override`` into ``base``."""
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
