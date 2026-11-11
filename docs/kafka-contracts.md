# Kafka contracts and local topic policy

The `contracts` Maven module contains the shared Java event records, topic names, and JSON serializer used by monitor and poller. Envelope schema version 1 carries `eventType`, `eventId`, `locationId`, `locationGeneration`, `emittedAt`, and a payload. Payload records cover registration, deletion, snapshots, and poll failures; `LocationRegistryRecord` is the compacted lifecycle projection. `LocationEventSerializer` writes ISO-8601 instants and omits null fields.

The envelope's payload is represented as `Object`; consumers must dispatch on `eventType` and decode the corresponding payload type explicitly. The serializer does not provide Kafka key serialization. Producers must set the Kafka record key to `locationId` for location events and registry records, and to the stable poller deployment ID for poller checkpoints. This preserves per-location partition ordering; do not change the partition count without a migration plan.

| Topic | Key | Partitions | Replication | Required policy |
|---|---|---:|---:|---|
| `flight.location-events.v1` | `locationId` | 3 | 1 | `cleanup.policy=delete`; `retention.ms` at least 46,800,000 (13 hours) |
| `flight.location-registry.v1` | `locationId` | 3 | 1 | `cleanup.policy=compact` |
| `flight.poller-checkpoint.v1` | poller deployment ID | 3 | 1 | `cleanup.policy=compact` |

## Provisioning and startup ordering

Only the monitor provisions topics. In the Compose deployment it runs with Spring's `local` profile; the default profile leaves bootstrap disabled, so ordinary tests and non-local starts do not require a broker. The monitor creates missing topics, then describes and validates all three topics. An existing topic with a different partition count, replication factor, cleanup policy, or too-short event retention causes monitor startup to fail with the mismatched policy in the error. Startup uses a 30-second total Admin API deadline.

The monitor's Docker health check tests the Actuator readiness group, which is not ready until startup runners (including topic provisioning) finish. Compose starts the poller only after the monitor is healthy. The poller does not create topics and therefore cannot race topic setup.

Topic retention/compaction is not an immediate deletion guarantee. Kafka may retain old segment data until retention and compaction run. Topic values can contain submitted coordinates and must not be written to logs.

## Schema and recovery rules

The `.v1` topic names and `schemaVersion=1` are contract boundaries. Preserve event IDs and location generations, and do not reuse deleted location IDs. Keep lifecycle events and snapshots keyed identically so Kafka ordering is per location. A topic partition-count change can remap keys and break that ordering; a schema or topic migration requires a separately tested bootstrap and recovery procedure. Existing policy mismatches are rejected rather than silently altered.

## Task 3 lifecycle projection

The monitor now writes the version-1 `LifecycleWire.Projection` registry wrapper: the existing
lifecycle record, active registration coordinates, a coordinate fingerprint, and bounded 24-hour
operation results committed in the same value. Deleted projections drop coordinates and
fingerprints; permanent deleted identity markers are not tombstoned. Rejected, never-admitted
registrations have a null lifecycle record and expire after 24 hours. Future poller consumers
must decode this wrapper and schedule only ACTIVE registrations.

Public topics still have three partitions. The lifecycle topology adds a named one-partition
internal repartition to enforce global admission counters without racing independent tasks.
See [lifecycle API and recovery](lifecycle-api.md) for wire details, stable internal/store names,
commit isolation, capacity limits, expiry and the required continuation boundary for Task 5.
