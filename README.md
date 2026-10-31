# PolicyCopy
 
A minimal [maubot](https://github.com/maubot/maubot) plugin that mirrors Matrix policy list rules from one ban list room into another.
 
## What it does
 
Watches a single **source room** for policy rule state events and forwards each one — same type, same `state_key`, same content — into a single **destination room**. Handles all three stable policy rule subtypes:
 
- `m.policy.rule.user`
- `m.policy.rule.room`
- `m.policy.rule.server`

Retractions (empty content `{}` for an existing `state_key`) are forwarded the same way as additions.
 
## Requirements
 
- maubot instance with a Matrix bot user that is:
  - joined to the source room (to receive its state events)
  - joined to the destination room with sufficient power level to send `m.policy.rule.*` state events (typically moderator or admin)
