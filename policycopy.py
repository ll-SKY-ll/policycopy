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

from typing import Type

from mautrix.types import EventType, StateEvent
from mautrix.util.config import BaseProxyConfig, ConfigUpdateHelper

from maubot import Plugin
from maubot.handlers import event


# ---------------------------------------------------------------------------
# Event type registration
# ---------------------------------------------------------------------------
# mautrix-python doesn't ship m.policy.rule.* as built-in EventType constants,
# so we register them via EventType.find(). Once registered we can use these
# objects both in @event.on() decorators and when calling send_state_event().
#
# Note: this plugin only handles the stable namespace. If you also need to
# mirror legacy `org.matrix.mjolnir.rule.*` events, register them the same
# way and add them to the decorator stack on mirror_rule().
# ---------------------------------------------------------------------------

POLICY_USER = EventType.find("m.policy.rule.user", t_class=EventType.Class.STATE)
POLICY_ROOM = EventType.find("m.policy.rule.room", t_class=EventType.Class.STATE)
POLICY_SERVER = EventType.find("m.policy.rule.server", t_class=EventType.Class.STATE)


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

    @classmethod
    def get_config_class(cls) -> Type[BaseProxyConfig]:
        return Config

    async def start(self) -> None:
        await super().start()
        # Load config now; maubot will also reload it when edited in the UI.
        self.config.load_and_update()
        self.log.info(
            "PolicyCopy ready: %s -> %s",
            self.config["source_room"],
            self.config["destination_room"],
        )

    # Stack one decorator per policy rule subtype. The handler body is the
    # same for all three, so we keep it in a single method.
    @event.on(POLICY_USER)
    @event.on(POLICY_ROOM)
    @event.on(POLICY_SERVER)
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

        self.log.info(
            "Copied %s rule state_key=%s entity=%s -> %s",
            evt.type,
            evt.state_key,
            (evt.content or {}).get("entity", "<empty/removed>"),
            destination,
        )