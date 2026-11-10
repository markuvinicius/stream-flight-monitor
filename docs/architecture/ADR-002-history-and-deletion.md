# ADR-002: Location history and asynchronous deletion

Date: 2026-11-06.
Status: 12-hour history and asynchronous removal accepted; storage details proposed.

## Context

Users browse active locations only, including snapshots from the preceding 12 hours. Deletion removes the entire history; asynchronous physical removal is accepted.

## Decision

Propose persistent Kafka Streams stores with location/time keys, bounded queries and wall-clock expiry. Retain per-aircraft regression windows only within each location. Snapshot classification is frozen at capture.

Commit a deleted lifecycle marker before acknowledging deletion and close access immediately. Remove local history in bounded batches and let retention/compaction reclaim historical physical copies. Retain a minimal ID-only terminal marker to prevent resurrection, not historical coordinates or aircraft data.

## Alternatives

An embedded SQL projection could simplify queries but needs an explicit offset/write consistency mechanism. Kafka tombstones alone do not provide immediate erasure or safe deletion-memory expiry. A topic per location creates unnecessary operational overhead.

## Consequences

No immediate physical-erasure promise is made. The proposed 48-hour healthy-operation cleanup objective requires monitoring and validation. Old history must remain inaccessible during cleanup and restoration. Permanent deletion markers need a capacity policy.

See the [detailed plan](../stream-flight-monitor-architecture-plan.md).
