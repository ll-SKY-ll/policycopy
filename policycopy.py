"""
PolicyCopy — a minimal maubot plugin that mirrors Matrix policy list rules
from one ban list room into another.

A Matrix "policy list" (a.k.a. ban list, in the Mjolnir/Draupnir sense) is
just a regular room whose state contains policy rule events. Each rule is a
state event of one of these types:

    m.policy.rule.user    — bans/recommends-against a specific user
    m.policy.rule.room    — bans/recommends-against a specific room
    m.policy.rule.server  — bans/recommends-against a whole homeserver

The state_key is an opaque per-rule identifier (often a hash or shortcode).
The content looks roughly like:

    {
        "entity": "@spammer:example.org",
        "recommendation": "m.ban",
        "reason": "spam"
    }

To remove a rule, the convention is to send empty content `{}` for the same
state_key. We treat that the same as any other update and forward it.

What this plugin does:
    Watch a single source room. Whenever a policy rule state event is
    observed there, send a matching state event (same type, same state_key,
    same content) into a single destination room. That's it — no filtering,
    no transformation, no shortcode rewriting.

The bot user must:
    - be joined to the source room (to receive state events)
    - be joined to the destination room with enough power level to send
      m.policy.rule.* state events
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from maubot import Plugin  # type: ignore[attr-defined]
from maubot.handlers import event
from mautrix.client import InternalEventType
from mautrix.types import JSON, EventType, FilterID, StateEvent, SyncToken
from mautrix.util.async_db import Connection, Database, UpgradeTable
from mautrix.util.config import BaseProxyConfig, ConfigUpdateHelper

# ---------------------------------------------------------------------------
# Event type registration
# ---------------------------------------------------------------------------
# mautrix-python doesn't ship m.policy.rule.* as built-in EventType constants,
# so we register them via EventType.find(). Once registered we can use these
# objects both in @event.on() decorators and when calling send_state_event().
#
# Note: this plugin only handles the stable namespace. If you also need to
# mirror legacy `org.matrix.mjolnir.rule.*` events, register them the same
# way and add them to the decorator stack on on_policy_rule().
# ---------------------------------------------------------------------------

POLICY_USER = EventType.find("m.policy.rule.user", t_class=EventType.Class.STATE)
POLICY_ROOM = EventType.find("m.policy.rule.room", t_class=EventType.Class.STATE)
POLICY_SERVER = EventType.find("m.policy.rule.server", t_class=EventType.Class.STATE)
POLICY_TYPES = {POLICY_USER.t, POLICY_ROOM.t, POLICY_SERVER.t}


upgrade_table = UpgradeTable()


@upgrade_table.register(description="Sync token for outage catch-up")  # type: ignore[call-arg,arg-type]
async def upgrade_v1(conn: Connection) -> None:
    await conn.execute(
        """CREATE TABLE sync_state (
            id          INTEGER PRIMARY KEY CHECK (id = 1),
            source_room TEXT NOT NULL,
            next_batch  TEXT NOT NULL
        )"""
    )


# ---------------------------------------------------------------------------
# Config schema
# ---------------------------------------------------------------------------
# Two room IDs. We require room IDs (!abc:server), not aliases (#name:server),
# because we never resolve aliases anywhere in the plugin.
# ---------------------------------------------------------------------------

class Config(BaseProxyConfig):
    def do_update(self, helper: ConfigUpdateHelper) -> None:
        helper.copy("source_room")
        helper.copy("destination_room")


# ---------------------------------------------------------------------------
# Plugin
# ---------------------------------------------------------------------------

class PolicyCopy(Plugin):
    config: Config
    database: Database
    _prev_token: str | None
    _prev_had_rules: bool
    _catchup_pending: bool
    _catchup_lock: asyncio.Lock

    @classmethod
    def get_config_class(cls) -> type[BaseProxyConfig]:
        return Config

    @classmethod
    def get_db_upgrade_table(cls) -> UpgradeTable:
        return upgrade_table

    async def pre_start(self) -> None:
        self._prev_token = None
        self._prev_had_rules = False
        self._catchup_pending = True
        self._catchup_lock = asyncio.Lock()

    async def start(self) -> None:
        await super().start()
        # Load config now; maubot will also reload it when edited in the UI.
        self.config.load_and_update()
        self.log.info(
            "PolicyCopy ready: %s -> %s",
            self.config["source_room"],
            self.config["destination_room"],
        )
        await self.catch_up()

    async def load_token(self) -> str | None:
        row = await self.database.fetchrow(
            "SELECT source_room, next_batch FROM sync_state WHERE id = 1"
        )
        if row is None or row["source_room"] != self.config["source_room"]:
            return None
        token: str = row["next_batch"]
        return token

    async def save_token(self, token: str) -> None:
        await self.database.execute(
            """INSERT INTO sync_state (id, source_room, next_batch) VALUES (1, $1, $2)
               ON CONFLICT (id) DO UPDATE
               SET source_room = excluded.source_room, next_batch = excluded.next_batch""",
            self.config["source_room"], token,
        )

    def source_rule_events(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        room = data.get("rooms", {}).get("join", {}).get(self.config["source_room"], {})
        raw = room.get("state", {}).get("events", []) + room.get("timeline", {}).get("events", [])
        return [ev for ev in raw if ev.get("type") in POLICY_TYPES and "state_key" in ev]

    async def replay(self, raws: list[dict[str, Any]], label: str) -> None:
        source = self.config["source_room"]
        for raw in raws:
            try:
                rule = StateEvent.deserialize(JSON({**raw, "room_id": source}))
            except Exception:
                self.log.exception("Could not parse %s event %s", label, raw.get("event_id"))
                continue
            await self.mirror_rule(rule)

    async def catch_up(self) -> None:
        async with self._catchup_lock:
            source = self.config["source_room"]
            since = await self.load_token()
            if since is None:
                self.log.info("No stored sync token for %s, skipping catch-up", source)
                self._catchup_pending = False
                return

            sync_filter = {
                "room": {
                    "rooms": [source],
                    "state": {"types": sorted(POLICY_TYPES)},
                    "timeline": {"types": sorted(POLICY_TYPES), "limit": 100},
                    "ephemeral": {"types": []},
                    "account_data": {"types": []},
                },
                "presence": {"types": []},
                "account_data": {"types": []},
            }
            try:
                data = await self.client.sync(
                    since=SyncToken(since), timeout=0, filter_id=FilterID(json.dumps(sync_filter))
                )
            except Exception:
                self.log.exception("Catch-up sync failed, keeping old token until a retry works")
                self._catchup_pending = True
                return

            latest: dict[tuple[str, str], dict[str, Any]] = {}
            for ev in self.source_rule_events(data):
                latest[(ev["type"], ev["state_key"])] = ev

            await self.replay(list(latest.values()), "missed")
            self.log.info("Catch-up done: %d missed rule(s) replayed", len(latest))
            await self.save_token(data["next_batch"])
            self._catchup_pending = False

    @event.on(InternalEventType.SYNC_SUCCESSFUL)  # type: ignore[arg-type]
    async def track_token(self, evt: dict[str, Any]) -> None:
        data = evt["data"]
        if self._catchup_pending:
            if not self._catchup_lock.locked():
                await self.catch_up()
        elif self._prev_token and self._prev_had_rules:
            await self.save_token(self._prev_token)

        if data.get("net.maunium.mautrix", {}).get("is_first"):
            await self.replay(self.source_rule_events(data), "first-sync")
            self._prev_token, self._prev_had_rules = None, False
            return
        self._prev_token = data.get("next_batch")
        self._prev_had_rules = bool(self.source_rule_events(data))

    # Stack one decorator per policy rule subtype. The handler body is the
    # same for all three, so we keep it in a single method.
    @event.on(POLICY_USER)  # type: ignore[arg-type]
    @event.on(POLICY_ROOM)  # type: ignore[arg-type]
    @event.on(POLICY_SERVER)  # type: ignore[arg-type]
    async def on_policy_rule(self, evt: StateEvent) -> None:
        await self.mirror_rule(evt)

    async def mirror_rule(self, evt: StateEvent) -> None:
        source = self.config["source_room"]
        destination = self.config["destination_room"]

        # 0. Refuse to mirror anything that isn't a real state event.
        #    Anyone with events_default power (usually 0) can send a plain
        #    *message* event whose type merely claims to be m.policy.rule.*.
        #    Such an event has no state_key at all — that's the defining
        #    difference from a state event. Today mautrix already fails to
        #    deserialize those into StateEvent (missing state_key) and never
        #    dispatches them here, but that's a library implementation detail,
        #    not a guarantee. Without this check, a change in that behavior
        #    would let an unprivileged user in the source room get their fake
        #    rule re-issued by us as genuine state in the destination room —
        #    a straight privilege escalation to "can ban things on the list".
        #
        #    NB: must be `is None`, NOT falsiness. state_key="" is a valid
        #    (and for many event types, the normal) value; only a *missing*
        #    key means "not state". Policy rules always use non-empty keys in
        #    practice, but we don't encode that assumption.
        if getattr(evt, "state_key", None) is None:
            self.log.warning(
                "Refusing to mirror non-state event %s (type=%s, sender=%s): "
                "no state_key — likely a spoofed message event",
                getattr(evt, "event_id", "<unknown>"), evt.type, evt.sender,
            )
            return

        # 1. Ignore events from rooms other than the configured source.
        #    The bot may sit in many rooms — we only mirror from one.
        if evt.room_id != source:
            return

        # 2. Sanity check: refuse to copy a room into itself. Without this,
        #    every event we send to destination would trigger another copy
        #    if source == destination, and we'd hammer the homeserver until
        #    it rate-limits us.
        if source == destination:
            self.log.warning(
                "source_room and destination_room are identical (%s); "
                "skipping to avoid a loop",
                source,
            )
            return

        # 3. Forward the event into the destination room.
        #    - Same event type, so a 'user' rule stays a 'user' rule.
        #    - Same state_key, so re-issuing the rule (an updated reason,
        #      or an empty {} to retract) lands on the same slot in dest.
        #    - Same content, byte-for-byte. We don't inject provenance
        #      metadata because doing so would break tools that hash policy
        #      contents for deduplication (e.g. Draupnir's
        #      DuplicateContentProtection).
        try:
            await self.client.send_state_event(
                room_id=destination,
                event_type=evt.type,
                content=evt.content,
                state_key=evt.state_key,
            )
        except Exception:
            # Common failure modes: bot not joined to dest, bot lacks the
            # power level required to send m.policy.rule.* state events,
            # or homeserver rate limit. Log and move on — losing one rule
            # is preferable to crashing the handler.
            self.log.exception(
                "Failed to copy policy rule (type=%s, state_key=%s) into %s",
                evt.type, evt.state_key, destination,
            )
            return

        content: Any = evt.content
        self.log.info(
            "Copied %s rule state_key=%s entity=%s -> %s",
            evt.type,
            evt.state_key,
            content.serialize().get("entity", "<empty/removed>"),
            destination,
        )