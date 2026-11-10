# ADR-001: Dedicated poller and local Kafka

Date: 2026-11-06.
Status: approach accepted; detailed contracts proposed.

## Context

Thousands of locations share a public API limit of one request per second. The project prefers Java/Kafka and must run locally below 8 GB.

## Decision

Run one active Java poller, one combined Kafka Streams/web application and one persistent KRaft broker. Poll locations in equal-turn rotation with one request in flight and no catch-up bursts. Use ordered, location-keyed events for lifecycle and snapshots.

## Alternatives

A single application is smaller but couples all failures. Kafka Connect adds a runtime without removing custom scheduling work. No Kafka is lighter but does not fit the preferred ecosystem as well.

## Consequences

HTTP failures are isolated. Broker and Streams recovery need testing. Single-broker disk loss is not tolerated. More pollers cannot increase the allowed upstream rate; overlapping pollers are unsupported. Exact dependency patches and measured resource use remain implementation gates.

See the [detailed plan](../stream-flight-monitor-architecture-plan.md).
