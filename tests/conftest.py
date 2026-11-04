from __future__ import annotations

import logging
import os
from typing import Any

import pytest
import pytest_asyncio

SOURCE = "!source:example.org"
DEST = "!dest:example.org"
OTHER = "!other:example.org"

USER = "m.policy.rule.user"
ROOM = "m.policy.rule.room"
SERVER = "m.policy.rule.server"

_PG_DSN = os.environ.get("TEST_POSTGRES_DSN")

_BACKENDS = ["sqlite"]
if _PG_DSN:
    _BACKENDS.append("postgres")


@pytest.fixture(params=_BACKENDS)
def db_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest_asyncio.fixture
async def database(db_backend: str, tmp_path: Any) -> Any:
    from mautrix.util.async_db import Database

    from policycopy import upgrade_table

    if db_backend == "sqlite":
        db = Database.create(f"sqlite:{tmp_path / 'test.db'}", upgrade_table=upgrade_table)
        await db.start()
        try:
            yield db
        finally:
            await db.stop()
        return

    import asyncpg

    reset = await asyncpg.connect(_PG_DSN)
    try:
        await reset.execute("DROP SCHEMA public CASCADE")
        await reset.execute("CREATE SCHEMA public")
    finally:
        await reset.close()

    db = Database.create(
        _PG_DSN,
        upgrade_table=upgrade_table,
        db_args={"min_size": 1, "max_size": 1},
    )
    await db.start()
    try:
        yield db
    finally:
        await db.stop()


class FakeClient:
    def __init__(self, sync_results: list[Any] | None = None, send_exc: Exception | None = None):
        self.writes: list[tuple[str, str, str, dict[str, Any]]] = []
        self.sync_calls: list[dict[str, Any]] = []
        self._sync_results = list(sync_results or [])
        self._send_exc = send_exc

    async def send_state_event(
        self, room_id: str, event_type: Any, content: Any, state_key: str
    ) -> None:
        if self._send_exc is not None:
            raise self._send_exc
        body = content.serialize() if hasattr(content, "serialize") else dict(content)
        self.writes.append((room_id, str(event_type), state_key, body))

    async def sync(self, since: Any = None, timeout: int = 30000, filter_id: Any = None) -> Any:
        self.sync_calls.append({"since": since, "timeout": timeout, "filter": filter_id})
        if not self._sync_results:
            raise AssertionError("unexpected /sync call")
        result = self._sync_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


async def make_plugin(
    database: Any = None,
    client: FakeClient | None = None,
    source: str = SOURCE,
    dest: str = DEST,
    started: bool = True,
) -> Any:
    from policycopy import PolicyCopy

    plugin = PolicyCopy.__new__(PolicyCopy)
    plugin.config = {"source_room": source, "destination_room": dest}
    plugin.log = logging.getLogger("policycopy.test")
    plugin.client = client or FakeClient()
    plugin.database = database
    await plugin.pre_start()
    if started:
        plugin._catchup_pending = False
    return plugin


_counter = 0


def rule(
    etype: str = USER,
    key: str = "rule1",
    content: Any = None,
    **extra: Any,
) -> dict[str, Any]:
    global _counter
    _counter += 1
    ev: dict[str, Any] = {
        "type": etype,
        "state_key": key,
        "content": {"entity": "@spam:bad.example", "recommendation": "m.ban"}
        if content is None
        else content,
        "event_id": f"$ev{_counter}",
        "sender": "@mod:example.org",
        "origin_server_ts": 1_700_000_000_000 + _counter,
    }
    ev.update(extra)
    return ev


def sync_response(
    next_batch: str = "s_next",
    state: list[dict[str, Any]] | None = None,
    timeline: list[dict[str, Any]] | None = None,
    room: str = SOURCE,
    extra_rooms: dict[str, Any] | None = None,
    is_first: bool = False,
) -> dict[str, Any]:
    join: dict[str, Any] = dict(extra_rooms or {})
    if state is not None or timeline is not None:
        join[room] = {
            "state": {"events": state or []},
            "timeline": {"events": timeline or [], "limited": False},
        }
    return {
        "next_batch": next_batch,
        "rooms": {"join": join},
        "net.maunium.mautrix": {"is_initial": False, "is_first": is_first},
    }


def sync_event(data: dict[str, Any]) -> dict[str, Any]:
    return {"data": data, "source": None}


async def stored_token(database: Any) -> tuple[str, str] | None:
    row = await database.fetchrow("SELECT source_room, next_batch FROM sync_state WHERE id = 1")
    if row is None:
        return None
    return (row["source_room"], row["next_batch"])
