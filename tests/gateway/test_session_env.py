import asyncio
import os

import pytest

from gateway.config import Platform
from gateway.run import GatewayRunner
from gateway.session import SessionContext, SessionSource
from gateway.session_context import (
    get_session_env,
    set_session_vars,
    clear_session_vars,
    build_session_subprocess_env,
    _VAR_MAP,
    _UNSET,
)


@pytest.fixture(autouse=True)
def _reset_contextvars():
    """Reset all session contextvars to _UNSET between tests.

    In production each asyncio.Task gets a fresh context copy where the
    defaults are _UNSET.  In tests all functions share the same thread
    context, so a clear_session_vars() from test A (which sets vars to "")
    would leak into test B.  This fixture ensures each test starts clean.
    """
    yield
    for var in _VAR_MAP.values():
        # Can't use var.reset() without a token; just set back to sentinel.
        var.set(_UNSET)


def test_set_session_env_sets_contextvars(monkeypatch):
    """_set_session_env should populate contextvars, not os.environ."""
    runner = object.__new__(GatewayRunner)
    source = SessionSource(
        platform=Platform.TELEGRAM,
        chat_id="-1001",
        chat_name="Group",
        chat_type="group",
        user_id="123456",
        user_name="alice",
        thread_id="17585",
    )
    context = SessionContext(source=source, connected_platforms=[], home_channels={})

    monkeypatch.delenv("HERMES_SESSION_PLATFORM", raising=False)
    monkeypatch.delenv("HERMES_SESSION_SOURCE", raising=False)
    monkeypatch.delenv("HERMES_SESSION_CHAT_ID", raising=False)
    monkeypatch.delenv("HERMES_SESSION_CHAT_NAME", raising=False)
    monkeypatch.delenv("HERMES_SESSION_CHAT_TYPE", raising=False)
    monkeypatch.delenv("HERMES_SESSION_USER_ID", raising=False)
    monkeypatch.delenv("HERMES_SESSION_USER_NAME", raising=False)
    monkeypatch.delenv("HERMES_SESSION_THREAD_ID", raising=False)

    tokens = runner._set_session_env(context)

    # Values should be readable via get_session_env (contextvar path)
    assert get_session_env("HERMES_SESSION_PLATFORM") == "telegram"
    assert get_session_env("HERMES_SESSION_SOURCE") == ""
    assert get_session_env("HERMES_SESSION_CHAT_ID") == "-1001"
    assert get_session_env("HERMES_SESSION_CHAT_NAME") == "Group"
    assert get_session_env("HERMES_SESSION_CHAT_TYPE") == "group"
    assert get_session_env("HERMES_SESSION_USER_ID") == "123456"
    assert get_session_env("HERMES_SESSION_USER_NAME") == "alice"
    assert get_session_env("HERMES_SESSION_THREAD_ID") == "17585"

    # os.environ should NOT be touched
    assert os.getenv("HERMES_SESSION_PLATFORM") is None
    assert os.getenv("HERMES_SESSION_SOURCE") is None
    assert os.getenv("HERMES_SESSION_CHAT_TYPE") is None
    assert os.getenv("HERMES_SESSION_THREAD_ID") is None

    # Clean up
    runner._clear_session_env(tokens)


def test_clear_session_env_restores_previous_state(monkeypatch):
    """_clear_session_env should restore contextvars to their pre-handler values."""
    runner = object.__new__(GatewayRunner)

    monkeypatch.delenv("HERMES_SESSION_PLATFORM", raising=False)
    monkeypatch.delenv("HERMES_SESSION_CHAT_ID", raising=False)
    monkeypatch.delenv("HERMES_SESSION_CHAT_NAME", raising=False)
    monkeypatch.delenv("HERMES_SESSION_CHAT_TYPE", raising=False)
    monkeypatch.delenv("HERMES_SESSION_USER_ID", raising=False)
    monkeypatch.delenv("HERMES_SESSION_USER_NAME", raising=False)
    monkeypatch.delenv("HERMES_SESSION_THREAD_ID", raising=False)

    source = SessionSource(
        platform=Platform.TELEGRAM,
        chat_id="-1001",
        chat_name="Group",
        chat_type="group",
        user_id="123456",
        user_name="alice",
        thread_id="17585",
    )
    context = SessionContext(source=source, connected_platforms=[], home_channels={})

    tokens = runner._set_session_env(context)
    assert get_session_env("HERMES_SESSION_PLATFORM") == "telegram"
    assert get_session_env("HERMES_SESSION_USER_ID") == "123456"
    assert get_session_env("HERMES_SESSION_CHAT_TYPE") == "group"

    runner._clear_session_env(tokens)

    # After clear, contextvars should return to defaults (empty)
    assert get_session_env("HERMES_SESSION_PLATFORM") == ""
    assert get_session_env("HERMES_SESSION_CHAT_ID") == ""
    assert get_session_env("HERMES_SESSION_CHAT_NAME") == ""
    assert get_session_env("HERMES_SESSION_CHAT_TYPE") == ""
    assert get_session_env("HERMES_SESSION_USER_ID") == ""
    assert get_session_env("HERMES_SESSION_USER_NAME") == ""
    assert get_session_env("HERMES_SESSION_THREAD_ID") == ""


def test_get_session_env_falls_back_to_os_environ(monkeypatch):
    """get_session_env should fall back to os.environ when contextvar is unset."""
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "discord")

    # No contextvar set — should read from os.environ
    assert get_session_env("HERMES_SESSION_PLATFORM") == "discord"

    # Now set a contextvar — should prefer it
    tokens = set_session_vars(platform="telegram")
    assert get_session_env("HERMES_SESSION_PLATFORM") == "telegram"

    # After clear — should return "" (explicitly cleared), NOT fall back
    # to os.environ.  This is the fix for #10304: stale os.environ values
    # must not leak through after a gateway session is cleaned up.
    clear_session_vars(tokens)
    assert get_session_env("HERMES_SESSION_PLATFORM") == ""


# ---------------------------------------------------------------------------
# SESSION_KEY contextvars tests
# ---------------------------------------------------------------------------


def test_session_key_falls_back_to_os_environ(monkeypatch):
    """get_session_env for SESSION_KEY should fall back to os.environ."""
    monkeypatch.setenv("HERMES_SESSION_KEY", "env-session-123")

    # No contextvar set — should read from os.environ
    assert get_session_env("HERMES_SESSION_KEY") == "env-session-123"

    # Set contextvar — should prefer it
    tokens = set_session_vars(session_key="ctx-session-456")
    assert get_session_env("HERMES_SESSION_KEY") == "ctx-session-456"

    # After clear — should return "" (explicitly cleared), not os.environ (#10304)
    clear_session_vars(tokens)
    assert get_session_env("HERMES_SESSION_KEY") == ""


def test_session_key_no_race_condition_with_contextvars(monkeypatch):
    """Prove contextvars isolates SESSION_KEY across concurrent async tasks.

    Two tasks set different session keys. With contextvars each task
    reads back its own value. With os.environ the second task would
    overwrite the first (the old bug).
    """
    monkeypatch.delenv("HERMES_SESSION_KEY", raising=False)

    results = {}

    async def handler(key: str, delay: float):
        tokens = set_session_vars(session_key=key)
        try:
            await asyncio.sleep(delay)
            read_back = get_session_env("HERMES_SESSION_KEY")
            results[key] = read_back
        finally:
            clear_session_vars(tokens)

    async def run():
        task_a = asyncio.create_task(handler("session-A", 0.15))
        await asyncio.sleep(0.05)
        task_b = asyncio.create_task(handler("session-B", 0.05))
        await asyncio.gather(task_a, task_b)

    asyncio.run(run())

    # Both tasks must read back their own session key
    assert results["session-A"] == "session-A", (
        f"Session A got '{results['session-A']}' instead of 'session-A' — race condition!"
    )
    assert results["session-B"] == "session-B", (
        f"Session B got '{results['session-B']}' instead of 'session-B' — race condition!"
    )


@pytest.mark.asyncio
async def test_run_in_executor_with_context_preserves_session_env(monkeypatch):
    """Gateway executor work should inherit session contextvars for tool routing."""
    runner = object.__new__(GatewayRunner)
    monkeypatch.delenv("HERMES_SESSION_PLATFORM", raising=False)
    monkeypatch.delenv("HERMES_SESSION_CHAT_ID", raising=False)
    monkeypatch.delenv("HERMES_SESSION_THREAD_ID", raising=False)
    monkeypatch.delenv("HERMES_SESSION_USER_ID", raising=False)

    source = SessionSource(
        platform=Platform.TELEGRAM,
        chat_id="2144471399",
        chat_type="dm",
        user_id="123456",
        user_name="alice",
        thread_id=None,
    )
    context = SessionContext(
        source=source,
        connected_platforms=[],
        home_channels={},
        session_key="agent:main:telegram:dm:2144471399",
    )

    tokens = runner._set_session_env(context)
    try:
        result = await runner._run_in_executor_with_context(
            lambda: {
                "platform": get_session_env("HERMES_SESSION_PLATFORM"),
                "chat_id": get_session_env("HERMES_SESSION_CHAT_ID"),
                "user_id": get_session_env("HERMES_SESSION_USER_ID"),
                "session_key": get_session_env("HERMES_SESSION_KEY"),
            }
        )
    finally:
        runner._clear_session_env(tokens)
        runner._shutdown_executor()

    assert result == {
        "platform": "telegram",
        "chat_id": "2144471399",
        "user_id": "123456",
        "session_key": "agent:main:telegram:dm:2144471399",
    }




def test_cron_session_contextvar_preserves_legacy_env_fallback(monkeypatch):
    """Unset cron ContextVar keeps old env-only cron callers working."""
    monkeypatch.setenv("HERMES_CRON_SESSION", "1")

    assert get_session_env("HERMES_CRON_SESSION") == "1"


def test_cron_session_explicit_blank_masks_leaked_env(monkeypatch):
    """Non-cron session bindings must override a stale process cron env flag."""
    monkeypatch.setenv("HERMES_CRON_SESSION", "1")

    tokens = set_session_vars(platform="api_server", cron_session="")
    try:
        assert get_session_env("HERMES_CRON_SESSION") == ""
    finally:
        clear_session_vars(tokens)

    assert get_session_env("HERMES_CRON_SESSION") == ""


def test_cron_session_set_clear_and_reset_tristate(monkeypatch):
    """Cron marker supports _UNSET fallback, 1 cron, and  explicit clear."""
    monkeypatch.setenv("HERMES_CRON_SESSION", "1")

    tokens = set_session_vars(cron_session="1")
    assert get_session_env("HERMES_CRON_SESSION") == "1"

    clear_session_vars(tokens)
    assert get_session_env("HERMES_CRON_SESSION") == ""


@pytest.mark.asyncio
async def test_run_in_executor_with_context_survives_default_executor_shutdown():
    """Gateway agent work should not depend on asyncio's default executor."""
    runner = object.__new__(GatewayRunner)
    loop = asyncio.get_running_loop()

    await loop.run_in_executor(None, lambda: None)
    await loop.shutdown_default_executor()

    try:
        result = await runner._run_in_executor_with_context(lambda: "ok")
    finally:
        runner._shutdown_executor()

    assert result == "ok"


# ---------------------------------------------------------------------------
# build_session_subprocess_env tests
# ---------------------------------------------------------------------------


def test_build_session_subprocess_env_no_base_env_uses_os_environ_fallback(monkeypatch):
    """With no explicit base_env, unset ContextVars fall back to os.environ."""
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "discord")
    monkeypatch.delenv("HERMES_SESSION_CHAT_ID", raising=False)

    env = build_session_subprocess_env()

    assert env["HERMES_SESSION_PLATFORM"] == "discord"
    assert "HERMES_SESSION_CHAT_ID" not in env


def test_build_session_subprocess_env_contextvar_overrides_os_environ(monkeypatch):
    """A set ContextVar always wins over os.environ, with or without base_env."""
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "discord")
    tokens = set_session_vars(platform="telegram")
    try:
        env = build_session_subprocess_env()
        assert env["HERMES_SESSION_PLATFORM"] == "telegram"

        env2 = build_session_subprocess_env({"HERMES_SESSION_PLATFORM": "slack"})
        assert env2["HERMES_SESSION_PLATFORM"] == "telegram"
    finally:
        clear_session_vars(tokens)


def test_build_session_subprocess_env_explicit_base_env_not_overridden_by_os_environ(monkeypatch):
    """Caller-supplied base_env is authoritative; unset ContextVars must not
    let a stale process-level os.environ value leak in and override it.

    Regression guard: previously, when base_env was explicitly passed and a
    ContextVar was unset, the function fell back to os.environ and clobbered
    the caller's explicit value.
    """
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "discord")

    base_env = {"HERMES_SESSION_PLATFORM": "telegram", "SOME_OTHER_VAR": "x"}
    env = build_session_subprocess_env(base_env)

    assert env["HERMES_SESSION_PLATFORM"] == "telegram"
    assert env["SOME_OTHER_VAR"] == "x"


def test_build_session_subprocess_env_explicit_base_env_preserves_unrelated_keys(monkeypatch):
    """build_session_subprocess_env must not drop unrelated base_env keys."""
    monkeypatch.delenv("HERMES_SESSION_PLATFORM", raising=False)

    base_env = {"PATH": "/usr/bin", "HOME": "/home/hermes"}
    env = build_session_subprocess_env(base_env)

    assert env["PATH"] == "/usr/bin"
    assert env["HOME"] == "/home/hermes"
    assert "HERMES_SESSION_PLATFORM" not in env


@pytest.mark.asyncio
async def test_gateway_executor_refuses_resurrection_after_shutdown():
    """A real gateway shutdown must NOT be resurrected by the recreate path.

    _shutdown_executor() means "we're stopping" — the recreate-on-shutdown
    logic exists to survive an *external* teardown of the loop default
    (test_..._survives_default_executor_shutdown), not to undo our own stop.
    """
    runner = object.__new__(GatewayRunner)

    try:
        first = await runner._run_in_executor_with_context(lambda: "first")
        assert first == "first"
        runner._shutdown_executor()

        with pytest.raises(RuntimeError, match="shutting down"):
            await runner._run_in_executor_with_context(lambda: "second")
    finally:
        runner._shutdown_executor()

