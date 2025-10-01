from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

import pytest

from common.config import ENV_PREFIX, load_config


def test_environment_overrides_yaml(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(f"{ENV_PREFIX}__INGESTION__POLL_INTERVAL_SECONDS", "120")

    config = load_config()

    assert config["ingestion"]["poll_interval_seconds"] == 120


def test_config_is_immutable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    defaults = tmp_path / "defaults.yaml"
    defaults.write_text('{"runtime": {"environment": "development"}}', encoding="utf-8")

    monkeypatch.chdir(tmp_path)

    config = load_config(defaults_path=defaults, user_config_path=Path("missing"))

    with pytest.raises(TypeError):
        config["runtime"] = {"environment": "production"}

