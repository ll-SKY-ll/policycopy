from __future__ import annotations

from types import SimpleNamespace

import pytest
from conftest import DEST, OTHER, ROOM, SERVER, SOURCE, USER, FakeClient, make_plugin, rule
from mautrix.types import StateEvent


def state_event(room_id: str = SOURCE, **kwargs: object) -> StateEvent:
    return StateEvent.deserialize({**rule(**kwargs), "room_id": room_id})


@pytest.mark.parametrize("etype", [USER, ROOM, SERVER])
async def test_forwards_same_type_key_and_content(etype: str) -> None:
    plugin = await make_plugin()
    content = {"entity": "@spam:bad.example", "recommendation": "m.ban", "reason": "spam"}
    await plugin.mirror_rule(state_event(etype=etype, key="abc123", content=content))
    assert plugin.client.writes == [(DEST, etype, "abc123", content)]


async def test_forwards_retraction_with_empty_content() -> None:
    plugin = await make_plugin()
    await plugin.mirror_rule(state_event(key="abc123", content={}))
    assert plugin.client.writes == [(DEST, USER, "abc123", {})]


async def test_log_shows_entity_and_marks_retractions(caplog: pytest.LogCaptureFixture) -> None:
    plugin = await make_plugin()
    with caplog.at_level("INFO", logger="policycopy.test"):
        await plugin.mirror_rule(state_event(key="k1"))
        await plugin.mirror_rule(state_event(key="k2", content={}))
    assert "entity=@spam:bad.example" in caplog.text
    assert "entity=<empty/removed>" in caplog.text


async def test_content_is_forwarded_unchanged_including_unknown_fields() -> None:
    plugin = await make_plugin()
    content = {
        "entity": "*.bad.example",
        "recommendation": "org.example.custom",
        "reason": "ünïcödé",
        "x.custom": {"nested": [1, 2, 3]},
    }
    await plugin.mirror_rule(state_event(etype=SERVER, content=content))
    assert plugin.client.writes[0][3] == content


async def test_ignores_events_from_other_rooms() -> None:
    plugin = await make_plugin()
    await plugin.mirror_rule(state_event(room_id=OTHER))
    await plugin.mirror_rule(state_event(room_id=DEST))
    assert plugin.client.writes == []


async def test_refuses_when_source_equals_destination() -> None:
    plugin = await make_plugin(dest=SOURCE)
    await plugin.mirror_rule(state_event())
    assert plugin.client.writes == []


async def test_refuses_event_without_state_key() -> None:
    plugin = await make_plugin()
    fake = SimpleNamespace(
        type=USER, room_id=SOURCE, sender="@x:y", event_id="$e",
        content={"entity": "@a:b", "recommendation": "m.ban"},
    )
    await plugin.mirror_rule(fake)
    assert plugin.client.writes == []


async def test_accepts_empty_string_state_key() -> None:
    plugin = await make_plugin()
    await plugin.mirror_rule(state_event(key=""))
    assert plugin.client.writes == [(DEST, USER, "", rule()["content"])]


async def test_send_failure_is_logged_not_raised(caplog: pytest.LogCaptureFixture) -> None:
    plugin = await make_plugin(client=FakeClient(send_exc=RuntimeError("M_FORBIDDEN")))
    await plugin.mirror_rule(state_event(key="k"))
    assert "Failed to copy policy rule" in caplog.text


def test_handler_registration() -> None:
    from mautrix.client import InternalEventType

    from policycopy import POLICY_ROOM, POLICY_SERVER, POLICY_USER, PolicyCopy

    assert PolicyCopy.on_policy_rule.__mb_event_handler__ is True
    assert PolicyCopy.on_policy_rule.__mb_event_types__ == {POLICY_USER, POLICY_ROOM, POLICY_SERVER}
    assert PolicyCopy.track_token.__mb_event_types__ == {InternalEventType.SYNC_SUCCESSFUL}
    assert not hasattr(PolicyCopy.mirror_rule, "__mb_event_handler__")
    assert not hasattr(PolicyCopy.replay, "__mb_event_handler__")


async def test_live_handler_forwards_to_mirror_rule() -> None:
    plugin = await make_plugin()
    await plugin.on_policy_rule(state_event(key="live"))
    assert [w[2] for w in plugin.client.writes] == ["live"]
