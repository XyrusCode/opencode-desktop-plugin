"""Configuration resolution. Pure functions over the environment."""

from __future__ import annotations

import os

import pytest

from bridge.config import DEFAULT_AUTH_TOKEN, DEFAULT_WS_PORT, load_config


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for key in list(os.environ):
        if key.startswith("BRIDGE_"):
            monkeypatch.delenv(key, raising=False)


class TestDefaults:
    def test_matches_the_phone(self):
        config = load_config()
        assert config.auth_token == DEFAULT_AUTH_TOKEN
        assert config.ws_port == DEFAULT_WS_PORT

    def test_binds_all_interfaces(self):
        # Loopback-only would make the bridge unreachable from a real phone.
        assert load_config().ws_host == "0.0.0.0"

    def test_spawns_opencode_by_default(self):
        assert load_config().spawn_opencode is True

    def test_no_configured_url_means_spawn_one(self):
        assert load_config().opencode_url == ""
        assert load_config().resolved_opencode_url.startswith("http://")


class TestWorkdir:
    def test_defaults_to_home_not_cwd(self, tmp_path, monkeypatch):
        """The phone should see real sessions out of the box.

        opencode scopes its session list to the directory it serves, so
        defaulting to the process cwd would show nothing when the bridge is
        started from an arbitrary directory.
        """
        monkeypatch.chdir(tmp_path)
        assert load_config().resolved_workdir == os.path.expanduser("~")

    def test_env_override_narrows_to_a_project(self, tmp_path, monkeypatch):
        monkeypatch.setenv("BRIDGE_WORKDIR", str(tmp_path))
        assert load_config().resolved_workdir == str(tmp_path)


class TestOverrides:
    def test_token_and_ports_override(self, monkeypatch):
        monkeypatch.setenv("BRIDGE_AUTH_TOKEN", "hunter2")
        monkeypatch.setenv("BRIDGE_WS_PORT", "1234")
        monkeypatch.setenv("BRIDGE_OPENCODE_PORT", "5678")

        config = load_config()
        assert config.auth_token == "hunter2"
        assert config.ws_port == 1234
        assert config.resolved_opencode_url == "http://127.0.0.1:5678"

    def test_explicit_url_wins_over_host_and_port(self, monkeypatch):
        monkeypatch.setenv("BRIDGE_OPENCODE_URL", "http://example.test:9000/")
        config = load_config()
        # Trailing slash stripped so paths join cleanly.
        assert config.resolved_opencode_url == "http://example.test:9000"

    @pytest.mark.parametrize("raw,expected", [
        ("1", True), ("true", True), ("YES", True), ("on", True),
        ("0", False), ("false", False), ("", False),
    ])
    def test_spawn_flag_parsing(self, monkeypatch, raw, expected):
        monkeypatch.setenv("BRIDGE_SPAWN_OPENCODE", raw)
        assert load_config().spawn_opencode is expected

    def test_bad_port_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("BRIDGE_WS_PORT", "not-a-number")
        assert load_config().ws_port == DEFAULT_WS_PORT
