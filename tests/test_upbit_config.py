from __future__ import annotations

from pathlib import Path

from autobot.upbit.config import load_upbit_credentials, load_upbit_settings


def test_load_upbit_credentials_autoloads_dotenv(tmp_path: Path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (tmp_path / ".env").write_text("UPBIT_ACCESS_KEY=from_dotenv\nUPBIT_SECRET_KEY=from_dotenv_secret\n", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("UPBIT_ACCESS_KEY", raising=False)
    monkeypatch.delenv("UPBIT_SECRET_KEY", raising=False)

    settings = load_upbit_settings(config_dir)
    creds = load_upbit_credentials(settings)

    assert creds is not None
    assert creds.access_key == "from_dotenv"
    assert creds.secret_key == "from_dotenv_secret"


def test_dotenv_does_not_override_existing_env(tmp_path: Path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    (tmp_path / ".env").write_text("UPBIT_ACCESS_KEY=file_key\nUPBIT_SECRET_KEY=file_secret\n", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("UPBIT_ACCESS_KEY", "env_key")
    monkeypatch.setenv("UPBIT_SECRET_KEY", "env_secret")

    settings = load_upbit_settings(config_dir)
    creds = load_upbit_credentials(settings)

    assert creds is not None
    assert creds.access_key == "env_key"
    assert creds.secret_key == "env_secret"


def test_load_upbit_settings_includes_websocket_defaults(tmp_path: Path, monkeypatch) -> None:
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(tmp_path)

    settings = load_upbit_settings(config_dir)

    assert settings.websocket.public_url == "wss://api.upbit.com/websocket/v1"
    assert settings.websocket.ratelimit.connect_rps == 5
    assert settings.websocket.ratelimit.message_rpm == 100
