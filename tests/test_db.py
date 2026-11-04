from __future__ import annotations

from typing import Any

import pytest
from conftest import SOURCE, make_plugin, stored_token


async def test_migration_creates_empty_table(database: Any) -> None:
    count = await database.fetchval("SELECT COUNT(*) FROM sync_state")
    assert count == 0


async def test_load_token_on_empty_table_is_none(database: Any) -> None:
    plugin = await make_plugin(database)
    assert await plugin.load_token() is None


async def test_save_then_load_round_trip(database: Any) -> None:
    plugin = await make_plugin(database)
    await plugin.save_token("s100_1")
    assert await plugin.load_token() == "s100_1"
    assert await stored_token(database) == (SOURCE, "s100_1")


async def test_second_save_overwrites_and_keeps_one_row(database: Any) -> None:
    plugin = await make_plugin(database)
    await plugin.save_token("s1")
    await plugin.save_token("s2")
    await plugin.save_token("s3")
    assert await database.fetchval("SELECT COUNT(*) FROM sync_state") == 1
    assert await plugin.load_token() == "s3"


async def test_token_for_other_source_room_is_ignored(database: Any) -> None:
    old = await make_plugin(database, source="!old:example.org")
    await old.save_token("s_old")

    new = await make_plugin(database, source=SOURCE)
    assert await new.load_token() is None


async def test_saving_under_new_source_room_replaces_old_row(database: Any) -> None:
    old = await make_plugin(database, source="!old:example.org")
    await old.save_token("s_old")
    new = await make_plugin(database, source=SOURCE)
    await new.save_token("s_new")
    assert await database.fetchval("SELECT COUNT(*) FROM sync_state") == 1
    assert await stored_token(database) == (SOURCE, "s_new")
    assert await old.load_token() is None


async def test_check_constraint_rejects_second_row(database: Any) -> None:
    with pytest.raises(Exception):  # noqa: B017
        await database.execute(
            "INSERT INTO sync_state (id, source_room, next_batch) VALUES (2, $1, $2)",
            SOURCE,
            "s_x",
        )
    assert await database.fetchval("SELECT COUNT(*) FROM sync_state") == 0
