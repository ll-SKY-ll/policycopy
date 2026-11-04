from __future__ import annotations

import asyncio
import json
import random
from typing import Any

import pytest
from conftest import (
    DEST,
    OTHER,
    ROOM,
    SERVER,
    SOURCE,
    USER,
    FakeClient,
    make_plugin,
    rule,
    stored_token,
    sync_event,
    sync_response,
)
from test_extract import random_sync

BAN = {"entity": "@spam:bad.example", "recommendation": "m.ban"}


async def test_no_token_means_no_sync_and_nothing_pending(database: Any) -> None:
    plugin = await make_plugin(database, started=False)
    await plugin.catch_up()
    assert plugin.client.sync_calls == []
    assert plugin.client.writes == []
    assert plugin._catchup_pending is False
    assert await stored_token(database) is None


async def test_sends_filtered_nonblocking_sync_from_stored_token(database: Any) -> None:
    client = FakeClient([sync_response("s_new")])
    plugin = await make_plugin(database, client, started=False)
    await plugin.save_token("s_old")
    await plugin.catch_up()

    [call] = client.sync_calls
    assert call["since"] == "s_old"
    assert call["timeout"] == 0
    flt = json.loads(call["filter"])
    assert flt["room"]["rooms"] == [SOURCE]
    assert sorted(flt["room"]["state"]["types"]) == sorted([USER, ROOM, SERVER])
    assert sorted(flt["room"]["timeline"]["types"]) == sorted([USER, ROOM, SERVER])
    assert flt["presence"]["types"] == []


async def test_replays_missed_rules_and_saves_new_token(database: Any) -> None:
    missed = [rule(USER, "a", BAN), rule(SERVER, "b", {"entity": "bad.example",
                                                       "recommendation": "m.ban"})]
    client = FakeClient([sync_response("s_new", timeline=missed)])
    plugin = await make_plugin(database, client, started=False)
    await plugin.save_token("s_old")
    await plugin.catch_up()

    assert client.writes == [(DEST, ev["type"], ev["state_key"], ev["content"]) for ev in missed]
    assert await stored_token(database) == (SOURCE, "s_new")
    assert plugin._catchup_pending is False


async def test_raw_events_without_room_id_are_accepted(database: Any) -> None:
    ev = rule(key="no_room_id")
    assert "room_id" not in ev
    client = FakeClient([sync_response("s_new", timeline=[ev])])
    plugin = await make_plugin(database, client, started=False)
    await plugin.save_token("s_old")
    await plugin.catch_up()
    assert len(client.writes) == 1
    assert "room_id" not in ev


async def test_newest_version_per_rule_wins(database: Any) -> None:
    old = rule(USER, "a", {"entity": "@x:y", "recommendation": "m.ban", "reason": "v1"})
    mid = rule(USER, "a", {"entity": "@x:y", "recommendation": "m.ban", "reason": "v2"})
    new = rule(USER, "a", {})
    same_key_other_type = rule(ROOM, "a", BAN)
    client = FakeClient([sync_response("s_new", state=[old], timeline=[mid, same_key_other_type,
                                                                       new])])
    plugin = await make_plugin(database, client, started=False)
    await plugin.save_token("s_old")
    await plugin.catch_up()
    assert sorted(client.writes) == sorted([
        (DEST, USER, "a", {}),
        (DEST, ROOM, "a", BAN),
    ])


async def test_malformed_event_is_skipped_rest_still_replayed(
    database: Any, caplog: pytest.LogCaptureFixture
) -> None:
    bad = rule(USER, "bad", content="not a dict")
    good = rule(USER, "good", BAN)
    client = FakeClient([sync_response("s_new", timeline=[bad, good])])
    plugin = await make_plugin(database, client, started=False)
    await plugin.save_token("s_old")
    await plugin.catch_up()
    assert client.writes == [(DEST, USER, "good", BAN)]
    assert "Could not parse missed event" in caplog.text
    assert await stored_token(database) == (SOURCE, "s_new")


async def test_other_rooms_in_response_are_ignored(database: Any) -> None:
    client = FakeClient([sync_response(
        "s_new",
        timeline=[rule(USER, "mine", BAN)],
        extra_rooms={OTHER: {"timeline": {"events": [rule(USER, "theirs", BAN)]}}},
    )])
    plugin = await make_plugin(database, client, started=False)
    await plugin.save_token("s_old")
    await plugin.catch_up()
    assert [w[2] for w in client.writes] == ["mine"]


async def test_sync_failure_keeps_token_and_marks_pending(
    database: Any, caplog: pytest.LogCaptureFixture
) -> None:
    client = FakeClient([ConnectionError("homeserver down")])
    plugin = await make_plugin(database, client, started=False)
    await plugin.save_token("s_old")
    await plugin.catch_up()
    assert client.writes == []
    assert plugin._catchup_pending is True
    assert await stored_token(database) == (SOURCE, "s_old")
    assert "Catch-up sync failed" in caplog.text


async def test_token_from_old_source_room_skips_catch_up(database: Any) -> None:
    old = await make_plugin(database, source="!old:example.org")
    await old.save_token("s_old")
    plugin = await make_plugin(database, started=False)
    await plugin.catch_up()
    assert plugin.client.sync_calls == []
    assert plugin._catchup_pending is False


# ---------------------------------------------------------------------------
# Edge case: rule lands in maubot's first sync, which maubot never dispatches.
# ---------------------------------------------------------------------------

async def test_rule_in_discarded_first_sync_is_still_mirrored(database: Any) -> None:
    client = FakeClient([sync_response("s_catchup")])
    plugin = await make_plugin(database, client, started=False)
    await plugin.save_token("s_old")
    await plugin.catch_up()
    assert client.writes == []

    late = rule(USER, "late", BAN)
    await plugin.track_token(sync_event(sync_response("s_first", timeline=[late], is_first=True)))
    assert client.writes == [(DEST, USER, "late", BAN)]


# ---------------------------------------------------------------------------
# Edge case: catch-up fails, later syncs must not overwrite the old token
# until a retry has succeeded.
# ---------------------------------------------------------------------------

async def test_pending_catch_up_blocks_token_saves(database: Any) -> None:
    client = FakeClient([ConnectionError("down")] * 4)
    plugin = await make_plugin(database, client, started=False)
    await plugin.save_token("s_old")
    await plugin.catch_up()

    for i in range(3):
        await plugin.track_token(sync_event(sync_response(f"s{i}", timeline=[rule(key=f"k{i}")])))

    assert await stored_token(database) == (SOURCE, "s_old")
    assert plugin._catchup_pending is True
    assert len(client.sync_calls) == 4
    assert all(call["since"] == "s_old" for call in client.sync_calls)


async def test_pending_catch_up_is_retried_on_next_sync(database: Any) -> None:
    missed = rule(USER, "missed", BAN)
    client = FakeClient([ConnectionError("down"), sync_response("s_retry", timeline=[missed])])
    plugin = await make_plugin(database, client, started=False)
    await plugin.save_token("s_old")
    await plugin.catch_up()
    assert plugin._catchup_pending is True

    await plugin.track_token(sync_event(sync_response("s1", timeline=[rule(key="live")])))
    assert plugin._catchup_pending is False
    assert client.writes == [(DEST, USER, "missed", BAN)]
    assert await stored_token(database) == (SOURCE, "s_retry")

    await plugin.track_token(sync_event(sync_response("s2")))
    assert await stored_token(database) == (SOURCE, "s1")


async def test_full_gap_scenario(database: Any) -> None:
    """maubot's first sync passed before the plugin loaded, catch-up then failed."""
    missed = rule(USER, "missed_while_down", BAN)
    client = FakeClient([
        ConnectionError("flapping"),
        sync_response("s_retry", timeline=[missed]),
    ])
    plugin = await make_plugin(database, client, started=False)
    await plugin.save_token("s_before_restart")

    await plugin.catch_up()
    await plugin.track_token(sync_event(sync_response("s2", timeline=[rule(key="new1")])))
    await plugin.track_token(sync_event(sync_response("s3", timeline=[rule(key="new2")])))

    assert (DEST, USER, "missed_while_down", BAN) in client.writes
    assert client.sync_calls[1]["since"] == "s_before_restart"


async def test_no_second_catch_up_while_one_is_running(database: Any) -> None:
    plugin = await make_plugin(database, started=False)
    await plugin.save_token("s_old")
    async with plugin._catchup_lock:
        for batch in ("s1", "s2"):
            await asyncio.wait_for(
                plugin.track_token(sync_event(sync_response(batch, timeline=[rule()]))),
                timeout=2,
            )
    assert plugin.client.sync_calls == []
    assert await stored_token(database) == (SOURCE, "s_old")


# ---------------------------------------------------------------------------
# Fuzz: whatever the response contains, only source-room rules get written.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("seed", range(10))
async def test_fuzz_catch_up_never_writes_foreign_events(database: Any, seed: int) -> None:
    rng = random.Random(seed)
    for i in range(20):
        data = random_sync(rng)
        client = FakeClient([data])
        plugin = await make_plugin(database, client, started=False)
        await plugin.save_token(f"s_old{i}")
        await plugin.catch_up()

        for room_id, etype, key, content in client.writes:
            assert room_id == DEST
            assert etype in (USER, ROOM, SERVER)
            assert content["origin"] == SOURCE
        assert len({(w[1], w[2]) for w in client.writes}) == len(client.writes)
        assert await stored_token(database) == (SOURCE, data["next_batch"])
