from __future__ import annotations

from typing import Any

from conftest import (
    DEST,
    OTHER,
    SOURCE,
    USER,
    make_plugin,
    rule,
    stored_token,
    sync_event,
    sync_response,
)

BAN = {"entity": "@spam:bad.example", "recommendation": "m.ban"}


def with_rules(next_batch: str) -> dict[str, Any]:
    return sync_response(next_batch, timeline=[rule(key=next_batch)])


def without_rules(next_batch: str) -> dict[str, Any]:
    return sync_response(next_batch)


async def test_current_sync_token_is_never_saved_immediately(database: Any) -> None:
    plugin = await make_plugin(database)
    await plugin.track_token(sync_event(with_rules("s1")))
    assert await stored_token(database) is None


async def test_previous_token_saved_when_it_had_rules(database: Any) -> None:
    plugin = await make_plugin(database)
    await plugin.track_token(sync_event(with_rules("s1")))
    await plugin.track_token(sync_event(without_rules("s2")))
    assert await stored_token(database) == (SOURCE, "s1")


async def test_previous_token_not_saved_without_rules(database: Any) -> None:
    plugin = await make_plugin(database)
    await plugin.track_token(sync_event(without_rules("s1")))
    await plugin.track_token(sync_event(without_rules("s2")))
    assert await stored_token(database) is None


async def test_rules_with_gap_sequence(database: Any) -> None:
    """S1 (rules) -> S2 (none) -> S3 (rules) -> S4."""
    plugin = await make_plugin(database)

    await plugin.track_token(sync_event(with_rules("s1")))
    assert await stored_token(database) is None

    await plugin.track_token(sync_event(without_rules("s2")))
    assert await stored_token(database) == (SOURCE, "s1")

    await plugin.track_token(sync_event(with_rules("s3")))
    assert await stored_token(database) == (SOURCE, "s1")

    await plugin.track_token(sync_event(without_rules("s4")))
    assert await stored_token(database) == (SOURCE, "s3")


async def test_rules_in_other_rooms_do_not_count(database: Any) -> None:
    plugin = await make_plugin(database)
    foreign = sync_response("s1", extra_rooms={OTHER: {"timeline": {"events": [rule()]}}})
    await plugin.track_token(sync_event(foreign))
    await plugin.track_token(sync_event(without_rules("s2")))
    assert await stored_token(database) is None


async def test_message_spoofing_policy_type_does_not_count(database: Any) -> None:
    plugin = await make_plugin(database)
    spoof = rule()
    del spoof["state_key"]
    await plugin.track_token(sync_event(sync_response("s1", timeline=[spoof])))
    await plugin.track_token(sync_event(without_rules("s2")))
    assert await stored_token(database) is None


async def test_first_sync_rules_are_mirrored(database: Any) -> None:
    plugin = await make_plugin(database)
    ev = rule(USER, "first", BAN)
    await plugin.track_token(sync_event(sync_response("s1", timeline=[ev], is_first=True)))
    assert plugin.client.writes == [(DEST, USER, "first", BAN)]


async def test_first_sync_token_is_never_saved(database: Any) -> None:
    plugin = await make_plugin(database)
    await plugin.track_token(sync_event(sync_response("s1", timeline=[rule()], is_first=True)))
    await plugin.track_token(sync_event(without_rules("s2")))
    await plugin.track_token(sync_event(without_rules("s3")))
    assert await stored_token(database) is None


async def test_first_sync_resets_previous_state(database: Any) -> None:
    plugin = await make_plugin(database)
    await plugin.track_token(sync_event(with_rules("s1")))
    await plugin.track_token(sync_event(sync_response("s2", is_first=True)))
    assert await stored_token(database) == (SOURCE, "s1")
    await plugin.track_token(sync_event(without_rules("s3")))
    assert await stored_token(database) == (SOURCE, "s1")


async def test_first_sync_from_other_rooms_writes_nothing(database: Any) -> None:
    plugin = await make_plugin(database)
    foreign = sync_response(
        "s1", extra_rooms={OTHER: {"timeline": {"events": [rule()]}}}, is_first=True
    )
    await plugin.track_token(sync_event(foreign))
    assert plugin.client.writes == []


async def test_live_syncs_do_not_mirror_themselves(database: Any) -> None:
    plugin = await make_plugin(database)
    await plugin.track_token(sync_event(with_rules("s1")))
    assert plugin.client.writes == []
