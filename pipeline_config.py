"""Centralized toggle configuration loader for the Pipeline Anvisa project."""
from __future__ import annotations

import json
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parent
CONFIG_PATH = PROJECT_ROOT / "pipeline_config.json"

_DEFAULT_CONFIG: Dict[str, Any] = {
    "pipeline": {
        "debug_mode": False,
        "cleanup_processed": False,
    },
    "etapa14": {
        "usar_gemini_api": False,
    },
    "anvisa": {
        "usar_mes_anterior": False,
    },
}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in override.items():
        if key not in base:
            base[key] = value
            continue

        base_value = base[key]
        if isinstance(base_value, dict) and isinstance(value, dict):
            base[key] = _deep_merge(base_value, value)
        else:
            base[key] = value
    return base


@lru_cache(maxsize=1)
def load_pipeline_config() -> Dict[str, Any]:
    """Load toggle configuration, falling back to defaults on error."""
    config = deepcopy(_DEFAULT_CONFIG)

    if not CONFIG_PATH.exists():
        return config

    try:
        with CONFIG_PATH.open("r", encoding="utf-8") as fp:
            user_config = json.load(fp)
        if isinstance(user_config, dict):
            _deep_merge(config, user_config)
    except json.JSONDecodeError as exc:
        print(f"[AVISO] pipeline_config.json inválido ({exc}); usando defaults.")

    return config


def get_toggle(*keys: str, default: Any = None) -> Any:
    """Retrieve a toggle value by navigating the config dictionary."""
    config = load_pipeline_config()
    current: Any = config
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current
