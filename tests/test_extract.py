from __future__ import annotations

import random
from typing import Any

import pytest
from conftest import OTHER, ROOM, SERVER, SOURCE, USER, make_plugin, rule, sync_response

POLICY = (USER, ROOM, SERVER)


async def test_reads_state_then_timeline_in_order() -> None:
    plugin = await make_plugin()
    s1, s2 = rule(key="a"), rule(ROOM, key="b")
    t1, t2 = rule(SERVER, key="c"), rule(key="a")
    data = sync_response(state=[s1, s2], timeline=[t1, t2])
    assert plugin.source_rule_events(data) == [s1, s2, t1, t2]


async def test_ignores_other_rooms() -> None:
    plugin = await make_plugin()
    mine = rule(key="mine")
    data = sync_response(
        timeline=[mine],
        extra_rooms={OTHER: {"timeline": {"events": [rule(key="theirs")]}}},
    )
    assert plugin.source_rule_events(data) == [mine]


async def test_ignores_source_room_outside_join_section() -> None:
    plugin = await make_plugin()
    room_data = {"timeline": {"events": [rule()]}, "state": {"events": [rule()]}}
    data = {"next_batch": "s", "rooms": {"leave": {SOURCE: room_data}, "invite": {SOURCE: {}}}}
    assert plugin.source_rule_events(data) == []


@pytest.mark.parametrize(
    "etype",
    [
        "m.room.member",
        "m.room.message",
        "org.matrix.mjolnir.rule.user",
        "m.policy.rule",
        "m.policy.rule.user.extra",
        "M.POLICY.RULE.USER",
    ],
)
async def test_ignores_non_policy_types(etype: str) -> None:
    plugin = await make_plugin()
    data = sync_response(timeline=[rule(etype)])
    assert plugin.source_rule_events(data) == []


async def test_ignores_policy_typed_event_without_state_key() -> None:
    plugin = await make_plugin()
    spoof = rule()
    del spoof["state_key"]
    real = rule(key="")
    data = sync_response(timeline=[spoof, real])
    assert plugin.source_rule_events(data) == [real]


@pytest.mark.parametrize(
    "data",
    [
        {},
        {"rooms": {}},
        {"rooms": {"join": {}}},
        {"rooms": {"join": {SOURCE: {}}}},
        {"rooms": {"join": {SOURCE: {"state": {}}}}},
        {"rooms": {"join": {SOURCE: {"timeline": {}}}}},
        {"rooms": {"join": {SOURCE: {"state": {"events": []}, "timeline": {"events": []}}}}},
    ],
)
async def test_missing_sections_yield_nothing(data: dict[str, Any]) -> None:
    plugin = await make_plugin()
    assert plugin.source_rule_events(data) == []


_LOOKALIKES = [
    OTHER,
    SOURCE.upper(),
    SOURCE + " ",
    " " + SOURCE,
    SOURCE.replace("!", "#"),
    SOURCE + "x",
    SOURCE[:-1],
    "!source:example.org.evil",
]

_JUNK_TYPES = [
    "m.room.member",
    "m.room.message",
    "m.reaction",
    "org.matrix.mjolnir.rule.user",
    "m.policy.rule",
    "",
    "m.policy.rule.user ",
]


def random_event(rng: random.Random, origin: str) -> dict[str, Any]:
    ev: dict[str, Any] = {
        "type": rng.choice(POLICY + tuple(_JUNK_TYPES)),
        "content": {"entity": f"@{rng.randrange(10**6)}:x", "recommendation": "m.ban",
                    "origin": origin},
        "event_id": f"${rng.getrandbits(64):x}",
        "sender": "@mod:example.org",
        "origin_server_ts": rng.randrange(10**13),
    }
    roll = rng.random()
    if roll < 0.8:
        ev["state_key"] = rng.choice(["", "k1", "k2", f"k{rng.randrange(50)}"])
    elif roll < 0.9:
        ev["state_key"] = None
    if rng.random() < 0.05:
        del ev["type"]
    return ev


def random_room(rng: random.Random, origin: str) -> dict[str, Any]:
    room: dict[str, Any] = {}
    if rng.random() < 0.85:
        room["state"] = {"events": [random_event(rng, origin) for _ in range(rng.randrange(8))]}
    if rng.random() < 0.85:
        room["timeline"] = {
            "events": [random_event(rng, origin) for _ in range(rng.randrange(12))],
            "limited": rng.random() < 0.3,
        }
    return room


def random_sync(rng: random.Random) -> dict[str, Any]:
    join: dict[str, Any] = {}
    for room_id in rng.sample(_LOOKALIKES, rng.randrange(len(_LOOKALIKES) + 1)):
        join[room_id] = random_room(rng, room_id)
    if rng.random() < 0.8:
        join[SOURCE] = random_room(rng, SOURCE)
    rooms: dict[str, Any] = {"join": join}
    if rng.random() < 0.3:
        rooms["leave"] = {SOURCE: random_room(rng, "leave:" + SOURCE)}
    if rng.random() < 0.3:
        rooms["invite"] = {OTHER: {"invite_state": {"events": []}}}
    return {"next_batch": f"s{rng.getrandbits(32)}", "rooms": rooms}


def expected_events(data: dict[str, Any]) -> list[dict[str, Any]]:
    room = data["rooms"]["join"].get(SOURCE)
    if room is None:
        return []
    events = room.get("state", {}).get("events", []) + room.get("timeline", {}).get("events", [])
    return [e for e in events if e.get("type") in POLICY and "state_key" in e]


@pytest.mark.parametrize("seed", range(20))
async def test_fuzz_only_source_room_policy_state_events(seed: int) -> None:
    plugin = await make_plugin()
    rng = random.Random(seed)
    for _ in range(100):
        data = random_sync(rng)
        got = plugin.source_rule_events(data)
        assert got == expected_events(data)
        for ev in got:
            assert ev["content"]["origin"] == SOURCE
            assert ev["type"] in POLICY
            assert "state_key" in ev
