"""Dependency-free smoke tests using stub AstrBot imports and a temporary SQLite DB."""
from __future__ import annotations
import importlib.util
import sqlite3
import sys
import tempfile
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Minimal framework stubs: exercise the plugin's actual database methods without AstrBot installed.
astrbot = types.ModuleType("astrbot")
api = types.ModuleType("astrbot.api")
api.logger = types.SimpleNamespace(info=lambda *a, **k: None, warning=lambda *a, **k: None,
                                  error=lambda *a, **k: None, exception=lambda *a, **k: None)
class DummyFilter:
    @staticmethod
    def command(*args, **kwargs):
        return lambda fn: fn
api.filter = DummyFilter()
api.AstrMessageEvent = type("AstrMessageEvent", (), {})
api.MessageChain = lambda x: x
event_mod = types.ModuleType("astrbot.api.event")
event_mod.AstrMessageEvent = api.AstrMessageEvent
event_mod.MessageChain = api.MessageChain
event_mod.filter = api.filter
api.star = types.SimpleNamespace(Context=type("Context", (), {}), Star=type("Star", (), {"__init__": lambda self, ctx: None}))
components = types.ModuleType("astrbot.api.message_components")
components.Plain = lambda x: x
star_mod = types.ModuleType("astrbot.api.star")
star_mod.Context = type("Context", (), {})
star_mod.Star = type("Star", (), {"__init__": lambda self, ctx: None})
utils = types.ModuleType("astrbot.core.utils.astrbot_path")
base_dir = None
utils.get_astrbot_data_path = lambda: str(base_dir)
for name, mod in {
    "astrbot": astrbot,
    "astrbot.api": api,
    "astrbot.api.event": event_mod,
    "astrbot.api.message_components": components,
    "astrbot.api.star": star_mod,
    "astrbot.core": types.ModuleType("astrbot.core"),
    "astrbot.core.utils": types.ModuleType("astrbot.core.utils"),
    "astrbot.core.utils.astrbot_path": utils,
}.items():
    sys.modules[name] = mod

spec = importlib.util.spec_from_file_location("context_guard_tested", ROOT / "main.py")
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

with tempfile.TemporaryDirectory() as td:
    base_dir = Path(td) / "data"
    base_dir.mkdir()
    db = base_dir / "data_v4.db"
    c = sqlite3.connect(db)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("CREATE TABLE conversations (user_id TEXT, token_usage INTEGER, updated_at TEXT)")
    c.executemany("INSERT INTO conversations VALUES (?, ?, ?)", [
        ("friend-session", 850, "2020-01-01T00:00:00+00:00"),
        ("group-session", 250, "2020-01-01T00:00:00+00:00"),
        ("unrelated-session", 123, "2026-10-30T00:00:00+00:00"),
    ])
    c.commit()
    c.close()

    plugin = mod.ContextGuard(None, {
        "friend_user_id": "friend-session",
        "group_user_id": "group-session",
        "table_name": "conversations",
        "context_window_tokens": 1000,
        "threshold_percent": 80,
        "freshness_enabled": False,
    })
    snap = plugin._read_snapshot()
    assert snap["sessions"]["friend"]["token_usage"] == 850
    assert snap["sessions"]["friend"]["fresh"] is True  # disabled even for old timestamps
    assert snap["sessions"]["group"]["token_usage"] == 250

    ro = plugin._open_readonly()
    try:
        try:
            ro.execute("UPDATE conversations SET token_usage=0 WHERE user_id='friend-session'")
        except sqlite3.OperationalError:
            pass
        else:
            raise AssertionError("read-only connection unexpectedly allowed UPDATE")
    finally:
        ro.close()

    stale_plugin = mod.ContextGuard(None, {
        "friend_user_id": "friend-session",
        "group_user_id": "group-session",
        "table_name": "conversations",
        "freshness_enabled": True,
        "freshness_seconds": 600,
    })
    stale = stale_plugin._read_snapshot()
    assert stale["sessions"]["friend"]["fresh"] is False

    unconfigured = mod.ContextGuard(None, {"table_name": "conversations"})
    candidates = unconfigured._list_session_candidates()
    assert len(candidates["rows"]) == 3
    assert candidates["rows"][0]["user_id"] in {"friend-session", "group-session", "unrelated-session"}

    class FakeContext:
        def __init__(self):
            self.sent = []
        async def send_message(self, umo, chain):
            self.sent.append((umo, chain))
            return True

    import asyncio
    notify_plugin = mod.ContextGuard(None, {
        "friend_user_id": "friend-session",
        "group_user_id": "group-session",
        "table_name": "conversations",
        "context_window_tokens": 1000,
        "threshold_percent": 80,
        "reset_percent": 75,
        "freshness_enabled": False,
    })
    notify_plugin.context = FakeContext()
    notify_plugin._state["notify_friend_umo"] = "private-target"
    notify_plugin._state["notify_group_umo"] = "group-target"
    asyncio.run(notify_plugin._check_once())
    assert len(notify_plugin.context.sent) == 2  # friend threshold alerts to both configured destinations
    assert notify_plugin._state["sessions"]["friend"]["alerted"] is True
    asyncio.run(notify_plugin._check_once())
    assert len(notify_plugin.context.sent) == 2  # no repeated alert while still above threshold

print("OK: database discovery, read-only guard, optional freshness filter, session listing, threshold and duplicate suppression")
