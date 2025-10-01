"""Configuration loading utilities for the podcast theme analyzer."""

from __future__ import annotations

from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

import json
import os

try:  # pragma: no cover - dependency availability varies in tests
    import yaml  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback when PyYAML missing
    yaml = None  # type: ignore


DEFAULTS_PATH = Path("config/defaults.yaml")
USER_CONFIG_PATH = Path("config.yaml")
ENV_PREFIX = "PODCAST_THEME_ANALYZER"


def load_config(
    defaults_path: Path = DEFAULTS_PATH,
    user_config_path: Path = USER_CONFIG_PATH,
    *,
    env: Mapping[str, str] | None = None,
    env_prefix: str = ENV_PREFIX,
) -> Mapping[str, Any]:
    """Load configuration merging YAML defaults, optional user config and env vars.

    Parameters
    ----------
    defaults_path:
        Path to the base configuration YAML file.
    user_config_path:
        Path to an optional user configuration YAML file.
    env:
        Mapping providing environment variables (defaults to ``os.environ``).
    env_prefix:
        Prefix used to select environment variables for overrides. Nested keys
        are delimited with double underscores (``__``).

    Returns
    -------
    Mapping[str, Any]
        An immutable mapping representing the merged configuration.
    """

    if env is None:
        env = os.environ

    config: dict[str, Any] = {}

    if defaults_path.exists():
        config = _deep_merge(config, _load_yaml(defaults_path))

    if user_config_path.exists():
        config = _deep_merge(config, _load_yaml(user_config_path))

    env_overrides = _env_to_overrides(env, env_prefix)
    if env_overrides:
        config = _deep_merge(config, env_overrides)

    return _deep_freeze(config)


def _load_yaml(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    data = _parse_yaml(text)

    if not isinstance(data, dict):
        raise ValueError(f"Expected mapping at root of {path}, got {type(data)!r}")

    return data


def _deep_merge(base: dict[str, Any], overrides: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in overrides.items():
        if (
            key in result
            and isinstance(result[key], dict)
            and isinstance(value, Mapping)
        ):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _env_to_overrides(env: Mapping[str, str], prefix: str) -> dict[str, Any]:
    if not prefix:
        relevant_items = env.items()
    else:
        prefix_with_sep = f"{prefix}__"
        relevant_items = (
            (key[len(prefix_with_sep) :], value)
            for key, value in env.items()
            if key.startswith(prefix_with_sep)
        )

    overrides: dict[str, Any] = {}

    for compound_key, raw_value in relevant_items:
        if not compound_key:
            continue

        keys = [segment.lower() for segment in compound_key.split("__") if segment]
        if not keys:
            continue

        cursor = overrides
        for part in keys[:-1]:
            cursor = cursor.setdefault(part, {})  # type: ignore[assignment]

        cursor[keys[-1]] = _parse_yaml_value(raw_value)

    return overrides


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({k: _deep_freeze(v) for k, v in value.items()})
    if isinstance(value, list):
        return tuple(_deep_freeze(item) for item in value)
    return value


def _parse_yaml(text: str) -> Any:
    if yaml is not None:
        return yaml.safe_load(text) or {}

    text = text.strip()
    if not text:
        return {}
    return json.loads(text)


def _parse_yaml_value(text: str) -> Any:
    if yaml is not None:
        return yaml.safe_load(text)

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text

