# Session: Flight Monitor
Skill: development
Status: phase-5-in-progress
Last saved: 2026-11-11 16:28 UTC

## Phase 1 - Feature Spec

### Goal and users
Build a local, web-based application for people who want to monitor aircraft within 25 nautical miles of a geographic location specified by latitude and longitude.

Initially display aircraft ID, flight number, current speed, last known altitude, and an altitude-trend status: FLYING, LANDING, or DEPARTURING.

### Acceptance criteria
1. A user can register a location using valid latitude and longitude.
2. Registration persists the location and returns a locationId.
3. The system fetches active aircraft near each registered location using GET https://opendata.adsb.fi/api/v3/lat/<lat>/lon/<lon>/dist/25 with the registered coordinates.
4. A user can open a URL containing a locationId and see the latest aircraft within 25 nautical miles, displaying aircraft ID, flight number, current speed, last known altitude, and status.
5. Status is inferred using regression over at least five consecutive observations of the same aircraft: decreasing altitude maps to LANDING, increasing altitude maps to DEPARTURING, and constant altitude maps to FLYING.
6. A user can delete a monitored location using its locationId.

### Constraints and existing decisions
- Greenfield project; workspace inspection found no existing files.
- Run locally; cloud deployment and paid subscriptions are out of scope.
- Web interface; registration initially uses a REST POST request, returning a location code.
- Each browser session tracks only one locationId.
- Increasing data age as registered-location count grows is accepted because of the upstream request limit. The UI must expose data freshness.
- History is location-centric, not shared across locations. Aircraft-specific regression windows remain scoped to their location.
- COLLECTING_DATA is accepted until five valid observations support classification.
- Total stack memory must remain strictly below 8 GB. This is a validation target, not a measured result.
- Users may browse only active locations. Deleting a location must remove its entire history, with asynchronous physical cleanup accepted.
- Users can browse snapshots for active locations up to 12 hours in the past.
- Immediate application-level removal followed by asynchronous physical cleanup of deleted-location history is accepted. A cleanup target and monitoring must be defined in the detailed plan; Kafka retention/compaction timing is not an immediate-erasure guarantee.
- The ingestion mechanism must support thousands of concurrently registered locations.
- Public adsb.fi requests must not exceed one request per second across ingestion, including retries.
- All registered locations must be polled at the same frequency. The upstream request budget is global across all locations; per-location limiters are insufficient.
- Use equal-turn scheduling rather than adaptive priority or browser-activity-based polling. Failed requests consume their turn and the global budget; retry behavior must not let a location monopolize other locations' turns.
- Preferred ecosystem: Java, Kafka, Kafka Streams, and Kafka Connect where appropriate.
- Keep the local development setup computationally small.
- Source documentation: https://github.com/adsbfi/opendata/blob/main/README.md
- "Passes by" means within a radius of 25 nautical miles, not necessarily directly overhead.

### Security and data terms
- No compliance requirements currently identified.
- Authentication and authorization requirements remain unspecified.
- Submitted coordinates may represent sensitive personal locations; access and deletion permissions need an explicit decision.
- The source documents personal, non-commercial use only and requires attribution with a link to adsb.fi.
- Do not send user coordinates to the upstream provider during planning or validation without a deliberate integration step.

### Architecture-relevant findings
- The source confirms the requested v3 geographic endpoint.
- The public API limit is one request per second; invalid requests also count toward limits.
- At one individual request per location, 1,000 locations require at least 1,000 seconds (16 minutes 40 seconds) per polling cycle, excluding latency and failures.
- More consumers or ingestion instances cannot increase permitted upstream throughput.
- A wider geographic query can cover up to 250 nautical miles, but using it to share coverage would change the explicit per-location dist/25 ingestion requirement and requires user approval.
- A global snapshot endpoint exists only for contributing receiver operators, refreshed twice per minute; eligibility is not assumed.
- Five samples taken across 1,000 individually polled locations span at least four cycles (about 66 minutes 40 seconds) after the first sample. This undermines timely classification for transient aircraft.
- Altitude regression indicates ascent or descent, not confirmed landing or departure intent.

### Open decisions before final architecture
- Whether exact per-location dist/25 fetching is mandatory or shared geographic coverage may be used.
- What to display after gaps or when altitude is missing or reported as ground; COLLECTING_DATA before five valid observations is approved.
- Historical snapshots are queryable for active locations over a rolling 12-hour window; storage capacity and asynchronous physical-deletion targets must be specified.
- Slope tolerance for level flight, units, observation window, and treatment of repeated or out-of-order upstream observations.
- Aircraft identity mapping (Mode-S hex versus registration) and flight-number mapping: an upstream callsign is not necessarily a commercial flight number.
- Anonymous locationId access versus authenticated registration, viewing, and deletion.
- Browser POST interaction: a small form or API client is needed; entering a URL in the address bar issues GET.
- Preferred Java/framework versions and whether the below-8-GB budget includes container-runtime/WSL overhead.

### Phase 2 - Alternatives under consideration (not yet selected)
1. Single Java application containing REST/UI, ingestion scheduling, and Kafka Streams, plus one local Kafka broker: smallest Kafka-based runtime footprint.
2. Dedicated Java poller plus a combined processing/web Java application and one Kafka broker: isolates upstream failures with modest additional local overhead.
3. Custom Kafka Connect source connector plus Kafka Streams/web application and one Kafka broker: fits Connect learning goals but adds a runtime and still requires dynamic scheduling and globally coordinated rate limiting.
4. Single Java application with a small durable database and no Kafka initially: smallest resource footprint but a weaker match for the Kafka ecosystem preference.

All alternatives must use the same equal-frequency scheduling and global request budget. No approach has been approved. Adaptive scheduling is excluded by the user's equal-frequency clarification.

### Dedicated-poller exploration
- The user is leaning toward approach 2 and requested deeper exploration, not final selection or implementation.
- Candidate runtime: one dedicated Java poller, one combined Java processing/web application, and one persistent local Kafka broker.
- Candidate durable registry: a compacted Kafka topic keyed by locationId, with acknowledged writes and local materialized views; registration visibility and poller readiness are asynchronous and must be explicit.
- Poller reconstructs active locations before polling, uses one active request scheduler, and persists a rotation checkpoint for recovery.
- A single active poller is the initial deployment constraint, not a distributed rate-limit guarantee. Multiple active pollers or automatic failover would require enforceable exclusive ownership and a coordinated request budget.
- Candidate ingestion contract: a location snapshot envelope, including request identity, timestamps, aircraft, and explicit success/empty/failure outcomes. A successful empty result must clear old membership; failure must preserve last-known data with stale/error indication.
- Candidate processing policy: bounded five-observation altitude regression per location and aircraft, using observation time, duplicate rejection, finite-state expiry, and explicit insufficient-data handling.
- Sharing aircraft observations across locations is an alternative requiring user approval because it changes the meaning of consecutive records and adds repartitioning/join complexity.
- Deletion must gate serving immediately after durable acknowledgement and eventually remove scheduling and processing state; delayed observations must not resurrect deleted locations.
- Resource consumption, Java/framework versions, freshness tolerance, status thresholds, and authentication remain undecided; no benchmarks have been executed.
- Subsequent clarification: increasing freshness delay is accepted, history is scoped per location, COLLECTING_DATA is approved, and the total stack must use less than 8 GB.
- Per-location history does not mean mixing different aircraft in one regression: maintain separate aircraft windows within each location.
- Approach 2 remains the leading candidate; request explicit approach approval before advancing to the file-level plan checkpoint.

### Relevant files
- Project root: C:\Users\NB27964\workspace\markuvinicius\stream-flight-monitor
- No existing implementation, build configuration, generated files, or tests found.

### Out of scope
- Paid subscriptions.
- Cloud environments.
- Monitoring more than one locationId in a browser session.
- Additional features beyond registration, live listing with altitude-trend status, and deletion unless separately approved.

## Phase 2 - Chosen Approach
Number: 2
Name: Dedicated Java poller plus combined Kafka Streams/web application
Rationale: Isolate globally rate-limited upstream ingestion from processing and web serving while keeping the local runtime to two JVMs and one persistent Kafka broker.

### Confirmed at approach approval
- Browse only active locations.
- Deleting a location removes its entire history, not merely its listing.
- No shared aircraft history across locations.
- Increasing polling delay with location count is accepted.
- COLLECTING_DATA is approved.
- Whole-stack memory must remain below 8 GB.

### Clarifications required for the detailed plan
- Resolved: active locations support browsing historical snapshots up to 12 hours in the past.
- Resolved: immediate logical deletion and asynchronous physical cleanup are acceptable.
- Deletion must prevent replay/restoration and late in-flight snapshots from resurrecting a removed location. A Kafka tombstone alone does not erase existing history immediately.

### Phase 3 preparation - Detailed plan outline awaiting approval
Document type: explanation and architecture reference.
Audience: project owner and implementing developers.
Goal: evaluate an implementation-ready local architecture before approving implementation tasks.
Scope: approved approach 2, REST/web interactions, globally fair ingestion, Kafka contracts, location-scoped processing, queryable 12-hour history, deletion/recovery, below-8-GB memory budget, validation, local cost and evolution.
Excluded: application code, cloud deployment, subscriptions, shared cross-location history, additional ingestion throughput beyond the source limit.

Proposed sections:
1. Requirements, assumptions, and architecture trade-offs.
2. Component boundaries and proposed technology/version selection.
3. API contracts and current/history browser flows.
4. Fair scheduler, global rate limiting, and upstream failure handling.
5. Kafka topics, keys, ordering, retention, and replay contracts.
6. Queryable 12-hour snapshot storage, pagination, expiry, and capacity.
7. Location-scoped aircraft regression and classification rules.
8. Deletion lifecycle, asynchronous cleanup, and non-resurrection guarantees.
9. Restart/recovery behavior and local security.
10. Memory/resource budgets, capacity envelope, observability, and local cost model.
11. Regression and integration validation, including request-spacing and memory thresholds.
12. File-level implementation plan, risks, decision records, and next approval checkpoint.

Planned diagram deliverables: Mermaid context, component, data-flow, deployment, constrained evolution, and resource-cost diagrams; HTML viewer; editable Draw.io context/component/deployment diagrams.
The outline and detailed plan were approved on 2026-11-06. The implementation task list awaits Checkpoint C approval.

## Phase 3 - Approved Architect Plan

Full plan: [Stream Flight Monitor architecture plan](docs/stream-flight-monitor-architecture-plan.md).
The linked document is the canonical full plan, approved on 2026-11-06 at 15:56 UTC.

### Deliverables
- Detailed requirements, proposed defaults, technology candidates and evaluation matrix.
- Ordered location lifecycle/snapshot topic, derived compacted registry, per-location history and trend stores.
- API contracts including paginated 12-hour history, operation lookup and idempotent registration.
- Fair single-owner polling and globally spaced requests; no overlapping automatic failover.
- Immediate serving gate for deletion, asynchronous physical cleanup and minimal non-coordinate deletion markers.
- Proposed 6 GiB total memory envelope, disk sizing formulas and resource validation gates.
- File-level implementation sequence, risks, recovery/migration rules and two ADRs.
- Six standalone Mermaid diagrams, embedded Markdown diagrams, HTML viewer and three-tab Draw.io export.

### Validation executed
- Six Mermaid sources match Markdown and HTML copies.
- Draw.io XML parses; three tabs, unique IDs and valid labeled connections.
- Nine local documentation links resolve.
- Resource-budget arithmetic checked only; actual memory, dependency compatibility, browser rendering and application tests have not run.

### Workflow state
- Approach 2 approved.
- Detailed plan outline approved.
- Checkpoint B: full detailed plan approved on 2026-11-06.
- Checkpoint C/task approval and implementation have not begun.
- Plan authored directly because no architect-agent delegation tool is available in this session.

## Phase 4 - Approved Task List

Specialist labels identify responsibilities. No specialist-agent delegation tool is currently available; execution must not claim delegation occurred.
All tasks read the approved architecture plan first. Paths below are proposed outputs, not existing application files.

[x] 1. Establish build and local runtime - backend/devops - build passes; broker and two application skeletons start with health checks and persistent volumes.
    Inputs: approved plan, ADR-001.
    Outputs: root/module Maven manifests, wrapper, application skeletons, Compose/container files and typed local configuration.
[x] 2. Define Kafka contracts - backend-engineer - versioned location-keyed envelopes and initialized topic policies match the approved design.
    Inputs: plan Kafka contracts, task 1 manifests.
    Outputs: contracts module event types/serialization, topic bootstrap and stable schema/store naming reference.
[x] 3. Implement location lifecycle - backend-engineer - registration persists, retries are idempotent, deletion closes access after acknowledgement and uncertain operations are explicit.
    Inputs: task 2 contracts, plan API and committed-gate rules.
    Outputs: monitor lifecycle topology, registry projection, mutation/operation APIs, admission controls and serving gate.
[ ] 4. Implement fair ingestion - backend-engineer - each stable location gets one attempt/cycle and all source request starts are at least 1,000 ms apart globally, including failures/restarts.
    Inputs: registry/contracts, source documentation, ingestion plan.
    Outputs: poller source client/normalization, rotation, lock, limiter, cooldown and checkpoint recovery.
[ ] 5. Implement snapshot processing - backend-engineer - five-sample location-scoped classification, 12-hour history and bounded expiry/deletion work without replay resurrection.
    Inputs: contracts/lifecycle, ADR-002, classification/storage rules.
    Outputs: monitor latest/history/trend stores, expiry indexes, quality handling and cleanup processors.
[ ] 6. Implement current and history queries - backend-engineer - bounded paginated current/frozen-history responses are available only for active locations with explicit readiness/failure semantics.
    Inputs: lifecycle gate, persistent stores and approved API contracts.
    Outputs: monitor read endpoints, cursor validation, timestamp/quality reporting and read-only store-query helpers.
[ ] 7. Implement browser flows - frontend-engineer - registration, current/history browsing and deletion work with one selected location per tab session.
    Inputs: API contracts/endpoints and local security requirements.
    Outputs: same-origin static UI, mutation protection, attribution and explicit waiting/stale/collecting/error states.
[ ] 8. Prepare resource and recovery validation - devops-engineer - reproducible procedures and recorded results demonstrate the approved memory, recovery and cleanup requirements; unexecuted checks remain explicit.
    Inputs: all services, Compose and resource/capacity plan.
    Outputs: metrics/resource wiring, validation workload/procedures and measured capacity report; 12-hour workload plus restoration must peak below 8,000,000,000 bytes for the whole stack.
[ ] 9. Write unit tests - qa-engineer - all new/modified logic has passing happy-path, edge, failure and negative-case tests.
    Inputs: every implementation file, approved acceptance criteria and framework versions.
    Outputs: deterministic unit, serialization and Kafka Streams topology tests, including thresholds and invalid inputs.
[ ] 10. Write end-to-end tests - qa-engineer - key user flows and at least one negative flow pass against real Kafka and a local HTTP stub.
    Inputs: all implementation files and unit tests.
    Outputs: integration/browser/API coverage for registration, current/12-hour history, deletion, global request spacing, fairness, recovery/non-resurrection and resource acceptance.

Dependencies:
- 2 requires 1; 3 requires 2.
- 4 and 5 independently require 3.
- 6 requires 5; 7 requires 6.
- 8 requires 4 and 7; 9 requires 8; 10 requires 9.

After implementation tasks: mandatory material-risk code review; security audit for location data and browser mutations; resolve findings; update changelog and offer project documentation updates. At task-list approval, no application code or tests had been created.

Task list approved on 2026-11-09. Task 1 was executed by the Data Engineer agent. Task 2 prerequisite completion began on 2026-11-11 after discovering that the committed contracts module lacked service dependencies and topic initialization. The approved target baseline is Java 21; Temurin 21.0.13+9 is available for verification. Docker CLI/Compose is unavailable, so container builds and Compose startup remain unvalidated.

## Phase 5 - Execution Log
- Task 1: complete, with full-stack startup validation pending - Added Java 21 Maven multi-module build and wrapper, monitor/poller health-enabled Spring Boot skeletons with upstream polling disabled, digest-pinned KRaft Compose runtime with persistent volumes, and startup documentation. Files: `pom.xml`, `monitor/pom.xml`, `poller/pom.xml`, `mvnw`, `mvnw.cmd`, `.mvn/wrapper/maven-wrapper.jar`, `.mvn/wrapper/maven-wrapper.properties`, `monitor/src/main/java/com/flightmonitor/monitor/MonitorApplication.java`, `monitor/src/main/java/com/flightmonitor/monitor/MonitorProperties.java`, `monitor/src/main/java/com/flightmonitor/monitor/HealthProbe.java`, `monitor/src/main/resources/application.yml`, `monitor/src/test/java/com/flightmonitor/monitor/MonitorApplicationTest.java`, `poller/src/main/java/com/flightmonitor/poller/PollerApplication.java`, `poller/src/main/java/com/flightmonitor/poller/PollerProperties.java`, `poller/src/main/java/com/flightmonitor/poller/HealthProbe.java`, `poller/src/main/resources/application.yml`, `poller/src/test/java/com/flightmonitor/poller/PollerApplicationTest.java`, `monitor/Dockerfile`, `poller/Dockerfile`, `docker-compose.yaml`, `README.md`, `.dockerignore`, `.gitignore`. Validation reported by the executing agent: `mvnw.cmd -B clean verify` passed on temporary Temurin 21.0.13+9; all 4 app smoke tests passed; Compose YAML parsed; effective dependency versions verified. Live Compose startup and container image builds were not run because Docker CLI/Compose are absent and Podman has no running engine. Source configuration and app health tests passed, but the complete stack-start acceptance condition remains environment-blocked. No Kafka contracts/topology or upstream client/scheduler was added.
- Task 2: complete - Added shared `contracts` dependencies to monitor and poller, topic definitions/validation, local monitor-owned provisioning, readiness-gated poller startup, and a concise topic/schema reference. Files: `monitor/pom.xml`, `poller/pom.xml`, `monitor/Dockerfile`, `poller/Dockerfile`, `monitor/src/main/resources/application.yml`, `monitor/src/main/resources/application-local.yml`, `monitor/src/main/java/com/flightmonitor/monitor/HealthProbe.java`, `monitor/src/main/java/com/flightmonitor/monitor/kafka/KafkaTopicPolicy.java`, `monitor/src/main/java/com/flightmonitor/monitor/kafka/KafkaTopicProvisioner.java`, `monitor/src/test/java/com/flightmonitor/monitor/MonitorApplicationTest.java`, `monitor/src/test/java/com/flightmonitor/monitor/kafka/KafkaTopicPolicyTest.java`, `docker-compose.yaml`, `docs/kafka-contracts.md`, `README.md`. Validation: `mvnw.cmd -B -pl monitor,poller -am verify` passed (13 tests total). Actual Kafka broker provisioning and Docker image/Compose startup were not tested because Docker is unavailable.
- Task 3: complete - Added additive shared lifecycle projection/operation wire contracts, persistent exactly_once_v2 lifecycle topology, a read_committed registry/operation serving gate, registration/operation/deletion REST APIs, ten-second outcome-uncertain semantics, bounded admission/expiry/concurrency, and local Host/Origin/Fetch Metadata/custom-header/body protections. Retained the existing three-partition public topics and locationId keys; explicitly introduced a one-partition internal ordered stage for authoritative global admission counters. Registry values atomically retain lifecycle and bounded 24-hour operation results; deleted markers have no coordinates or coordinate fingerprints. Added local-only lifecycle activation/readiness and a bounded retry for the real KRaft post-creation metadata visibility race. Main files: `contracts/src/main/java/com/flightmonitor/contracts/event/LifecycleWire.java`, `contracts/src/test/java/com/flightmonitor/contracts/LifecycleProjectionContractTest.java`, monitor `lifecycle/` and `api/` packages and focused tests, `monitor/src/main/java/com/flightmonitor/monitor/kafka/KafkaTopicProvisioner.java` and its test, `monitor/pom.xml`, application configuration/smoke test, `docker-compose.yaml`, `docs/lifecycle-api.md`, `docs/kafka-contracts.md`, `README.md`. Validation: Java 21 `mvnw.cmd -B -Pkafka-integration clean verify` passed all 45 tests with no failures/skips; real KRaft test covered local-profile provisioning/readiness, REST lifecycle, open/aborted transaction isolation, cold changelog/projection recovery, and processor-level rejection of deleted-ID reuse after recovery. `mvnw.cmd -B verify` passed 44 tests and intentionally skipped the one opt-in broker test; ordinary Spring tests start no Kafka resources. `git diff --check` passed and final code/diff inspected. Docker images/Compose startup, whole-stack memory, physical cleanup objectives and sustained capacity remain unvalidated; Windows temporary-state/POSIX permission diagnostics occurred in Kafka tests without test failures. Existing working-tree prerequisite changes were preserved. No upstream calls, polling, aircraft/history state, current/history queries or UI were added. Tasks 4–10 remain pending.
- Task 4: pending
- Task 5: pending
- Task 6: pending
- Task 7: pending
- Task 8: pending
- Task 9: pending
- Task 10: pending
