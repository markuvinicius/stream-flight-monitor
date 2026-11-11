# Location lifecycle (Task 3)

This is a **single-monitor, trusted local-operator** API, not an authenticated public service.
Compose publishes only `127.0.0.1:8080`. Native starts also bind loopback unless explicitly
overridden. Do not expose it through a proxy, tunnel, shared machine, or remote interface
without a separately designed authentication/deletion-authorization boundary.

## Requests

All API responses disable caching. Mutations require `X-Flight-Monitor: 1`; browser requests
must have the exact configured Origin (`http://127.0.0.1:8080` by default).
Host must match that origin's authority, including its port. Cross-origin/same-site Fetch
Metadata is denied; no CORS permission is granted. Non-browser clients may omit Origin but
still need the mutation header. These defend against CSRF and DNS rebinding, **not other
local processes**. `localhost` and `127.0.0.1` are different origins.

### Register

`POST /api/locations`, `Content-Type: application/json`, `Idempotency-Key: <client-generated key>`.
Keys contain 1–128 printable non-space ASCII characters; use a random UUID generated **before**
sending. JSON must contain exactly numeric `latitude` and `longitude`. Both must be finite,
latitude within [-90,90], longitude within [-180,180]. Request bodies are limited to 4,096
bytes, including requests without Content-Length. Duplicate JSON fields are rejected.

```json
{"latitude":0,"longitude":0}
```

201:

```json
{
  "locationId": "<opaque 64-character ID>",
  "operationId": "<opaque 64-character ID>",
  "trackingUrl": "/locations/<locationId>",
  "createdAt": "<UTC instant>",
  "state": "WAITING_FOR_FIRST_POLL"
}
```

The Location header names `/api/locations/{locationId}`. The tracking URL and current-location
resource are reserved for later query/UI tasks; they are **not implemented in Task 3**.
No upstream request is made.

The namespace and raw key determine locationId using SHA-256 with an unambiguous separator.
Only the derived ID is retained, not the raw key. Coordinate fingerprints canonicalize
binary64 values (including signed zero). The registration operation/event ID additionally
includes that fingerprint, so a conflicting attempt cannot overwrite the original result.
Same key/coordinates returns the original location, operation and creation time; conflicting
coordinates return 409, including concurrent attempts. Accepted keys expire after 24 hours;
expired keys return 409, never create a new generation. Deleted IDs return 410 and are never reused.
`sourceClientId` in the existing contract means deployment namespace here, not browser identity.
`locationKey` contains the coordinate fingerprint, not the raw Idempotency-Key.

### Delete

`DELETE /api/locations/{locationId}` with the mutation header, and no body.
202 contains `locationId`, `operationId`, `cleanupPending=true` **only after** the
read-committed projection closes the serving gate. Repeat deletion returns 202, including
after recovery. Unknown locations return 404. Subsequent location serving must return 410
for deleted locations; Task 6 must call the gate again immediately before releasing data.
History/aircraft cleanup is not implemented here because those stores do not exist yet.
The terminal projection immediately drops coordinates and fingerprints.

### Resolve an uncertain write

`GET /api/operations/{operationId}` returns the committed result (`operationId`, `eventId`,
`locationId`, `httpStatus`, `code`, `createdAt`, `expiresAt`). Results are retained for 24 hours
from their first processing, not extended by retries. Lookup of a locally pending write is
503/OUTCOME_UNCERTAIN; an absent or expired result is 404 with an explicit warning that an
unconfirmed write may still commit. After restart, absence alone cannot prove failure.

Registration and deletion wait **at most ten seconds** for the exact event result in the
committed projection, including the publication wait. Deadline expiry returns an RFC 9457-style
503 problem containing `operationId`, `code=OUTCOME_UNCERTAIN`, `outcome=UNCERTAIN`.
It does **not** mean rollback. Retry the same key and coordinates, or query the operation;
do not use a new key to compensate for a timeout.

Invalid input is a structured 400, browser/Host violations 403, admission capacity 409,
restoration/Kafka unavailability 503. There is no empty-success storage fallback.

## Bounded admission and failure behavior

- Defaults: 1,000 active locations, 10,000 retained identities/projection entries, at most
  16 retained operations per location. The last operation slot is reserved for deletion.
  Lifetime deleted markers never expire. Rejected registrations may consume a transient entry
  for 24 hours, conservatively reducing available admissions.
- Admission-budget rejections made before publication are not accepted operations and do not
  allocate an operation ID/result slot. The processor also enforces every bound; overflow
  observations do not evict already retained results.
- One different mutation at a time; concurrent same-event retries share the committed result.
  Contention returns 503 rather than building an unbounded queue. There is one producer worker
  and at most one queued command while a previous commit acknowledgement finishes.
- After a timeout, new different mutations pause until the pending committed result is observed.
  A fatal producer/reader failure makes lifecycle unavailable; restart the monitor with the
  **same** namespace/application ID/state volume after repairing Kafka. Transaction initialization
  fences the previous REST writer. Do not claim that its uncertain transaction was rolled back.
- Tomcat bounds connections (64), worker threads (32), backlog (16), headers (8 KiB), and
  connection reads. The ten-second lifecycle visibility deadline is not a guarantee of
  network delivery to a disconnected or stalled client.

## Commit and recovery boundaries

Public event/registry/checkpoint topics retain three partitions. Events are keyed by locationId.
The explicitly named `flight-monitor-lifecycle-v1-lifecycle-order-v1-repartition` internal
topic has **one partition**, enforcing global admission counters and preserving each location's
source order. There is no promised order between different locations at the public source.
All event types traverse this ordered stage; only REGISTER/DELETE mutate state in Task 3.
Task 5 must extend the same stage for snapshot/state processing, not race lifecycle processing
from an independent source branch.

The stable Streams application ID is `flight-monitor-lifecycle-v1`; persistent, changelogged
stores are `lifecycle-registry-v1`, `lifecycle-counters-v1`, `lifecycle-expiry-v1`.
Streams uses `exactly_once_v2`, one thread, RF=1 internal topics, 100 ms commits.
Operation expiry runs every second in batches of at most 128 indexed entries.

The registry value is the version-1 shared `contracts` module's `LifecycleWire.Projection` wrapper around the existing
`LocationRegistryRecord`, an active `LocationRegistration`, coordinate fingerprint, timestamps,
and bounded operation results. This freezes the previously unspecified registry payload
without changing shared contract constructors. Future poller code must decode this projection,
ignore null-registry rejected registrations, and use only ACTIVE registrations.
Operation results and lifecycle state are in **one Kafka value**, so compaction cannot lose
earlier unexpired operations and there is no cross-topic acknowledgement race.

REST publication has explicit Kafka transactions, idempotence, acks=all, and bounded timeouts.
Streams atomically commits its outputs, source offsets and recoverable state, **not HTTP responses**.
The HTTP gate never reads interactive-query stores. Its dedicated read_committed consumer
replays the registry from the beginning to startup last-stable offsets, then continues consuming.
Every fresh process reconstructs both identities and retained operation results. Readiness
requires Streams RUNNING, projection restoration, and recent successful broker contact.
Local-profile readiness includes this lifecycle indicator and gates Compose poller startup.
The default profile starts no Kafka resources and its lifecycle API returns 503.

Do not reset application ID, repartition/store names, namespace or source partition counts.
The 13-hour event log cannot rebuild registrations older than 13 hours. Recovery relies on
registry/changelogs. Migrations require a stopped-writer, deletion-aware export/bootstrap
and tested rollback. Malformed records stop processing; no skip/quarantine policy is enabled.

Deleted markers contain no coordinates/fingerprints/aircraft. Old Kafka segments, RocksDB
files and changelogs are reclaimed asynchronously; this is not immediate physical erasure.
The single broker still cannot tolerate disk loss. Full memory/cleanup qualification belongs
to later tasks and has not been measured here.

## Validation

```powershell
.\mvnw.cmd -B verify
# Opt-in real KRaft broker: never launched by ordinary Spring tests
.\mvnw.cmd -B -pl monitor -am -Pkafka-integration test
```

Focused tests cover wire dispatch/ranges, topology ordering/admission/expiry, idempotency,
deleted-ID replay, serving-gate timeouts, concurrent retries, REST errors/body limits and
browser defenses. The opt-in test exercises local-profile topic provisioning/readiness,
REST-to-Kafka lifecycle, open/aborted transaction isolation, and cold state/gate recovery.
It uses synthetic coordinates only and never accesses adsb.fi.

Executed on Java 21 / Windows, 2026-11-11:

| Command | Actual result |
|---|---|
| `mvnw.cmd -B -Pkafka-integration clean verify` | 45 tests passed, including real KRaft; no skipped tests |
| `mvnw.cmd -B verify` | 44 tests passed; the opt-in broker test was skipped |
| `git diff --check` | Passed |

Kafka logged temporary-state and Windows POSIX-permission diagnostics during tests; these
did not cause failures and do not validate Linux container permissions. Docker image builds,
Compose startup, sustained load, whole-stack memory and physical-cleanup objectives were not
validated. No public source integration was attempted.
