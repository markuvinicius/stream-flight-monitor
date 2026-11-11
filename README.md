# Stream Flight Monitor

Local implementation of the approved dedicated-poller architecture. The runtime contains a Spring Boot monitor, a separate Spring Boot poller process, and one persistent single-node Kafka broker. Location registration, idempotency/operation lookup, deletion, and a committed Kafka Streams lifecycle projection are implemented. Aircraft ingestion, aircraft/history processing, query endpoints, and UI remain later tasks; no upstream requests are made.

## Prerequisites

- JDK 21 (the Maven Wrapper downloads Maven 3.9.11 if it is not already cached).
- Docker Engine with the Docker Compose v2 plugin.
- At least 8 GB of host memory is recommended for the Docker runtime plus other applications; the planned service/container envelope is 4.5 GiB plus about 1.5 GiB for Docker/WSL runtime overhead (6 GiB total). Actual use has not yet been measured.

Java 17 is not a supported build baseline for this project. The POM compiles with `--release 21`; install/use a JDK 21 rather than lowering the target.

Pinned versions: Java 21.0.13+9 (Temurin), Maven 3.9.11, Spring Boot 3.5.16, Spring Kafka 3.3.16, and Apache Kafka clients/Streams and broker 3.9.2. The Spring dependency tree resolves the Kafka clients and Streams to the same 3.9.2 patch as the broker. These select the latest patches found for the approved Java/Spring/Kafka version lines during setup; the exact Java container tags and Kafka image are digest-pinned. No architecture version-line deviation was made. The Maven Wrapper distribution is checksum-verified.

## Build

From the repository root:

```powershell
.\mvnw.cmd clean verify
```

The reactor builds the shared `contracts` module and both services. Tests verify the application smoke paths and topic policy definitions. See [Kafka contracts and local topic policy](docs/kafka-contracts.md) for topic names, keys, retention, validation, and startup ordering.

See [lifecycle API, local browser defenses, limits and recovery](docs/lifecycle-api.md) for
Task 3 endpoints and the opt-in real-Kafka integration test (`-Pkafka-integration`).
Normal tests do not start a broker.

## Start the local runtime

```powershell
docker compose up --build
```

The monitor health endpoint is available at [http://127.0.0.1:8080/actuator/health](http://127.0.0.1:8080/actuator/health). The monitor runs in the local profile, provisions and validates the three required topics, and waits for Streams and the read-committed serving gate before readiness permits poller startup. Kafka and the poller have container health checks but no host-published ports. Both applications receive `broker:9092` over the private Compose network. Only the monitor is published, and only on the host loopback interface. Use `127.0.0.1`, not `localhost`, unless you explicitly change `MONITOR_PUBLIC_ORIGIN`.

To inspect service state and logs:

```powershell
docker compose ps
docker compose logs -f broker monitor poller
```

## Run the API requests with Bruno

The [Bruno collection](api-requests/) tests the currently implemented location lifecycle endpoints: register a location, look up an operation, and delete a location. Current-flight and snapshot/history endpoints are not implemented yet, and the collection does not call adsb.fi.

1. Start the local stack with `docker compose up --build` and wait for the monitor readiness check:

   ```powershell
   Invoke-RestMethod http://127.0.0.1:8080/actuator/health/readiness
   ```

2. In Bruno, open the `api-requests` folder as a collection and select the `local` environment. It sets the base URL to `http://127.0.0.1:8080`.
3. Run the entire collection in order. The requests use runtime variables to pass registration IDs and operation IDs between requests; the final requests delete the locations created during the run.

To run the collection with Bruno CLI on Windows, from the repository root:

```powershell
Push-Location .\api-requests
bru.cmd run --env local
Pop-Location
```

The tests use boundary coordinates, not personal locations. A successful registration temporarily stores its coordinates in local Kafka state until deletion. Keep the service on loopback; do not expose it to a shared or remote network. See the [API requests guide](api-requests/README.md) for collection details and the [lifecycle API reference](docs/lifecycle-api.md) for endpoint behavior.

Stop the services while retaining their data:

```powershell
docker compose down
```

The broker log directory, monitor state directory, and poller checkpoint/state directory are separate named volumes. `docker compose down -v` permanently deletes all three volumes and their contents; do not use it if local data must be retained.

The Compose stack targets 2 GiB for Kafka, 2 GiB for the monitor, and 512 MiB for the poller. JVM heaps are capped at 768 MiB, 768 MiB, and 192 MiB respectively, leaving room for native allocations and RocksDB. These limits are a configuration envelope, not evidence that the final history workload meets the total-memory target.

## Configuration and scope

Typed settings are defined in each module and use `application.yml` plus environment-variable overrides. Both `monitor.upstream-polling-enabled` and `poller.upstream-polling-enabled` default to `false` and remain explicitly false in Compose. Topic bootstrap is disabled by default and enabled only by the monitor's Compose `local` profile. No scheduler or upstream HTTP client is present in this scaffold.

Container image versions are exact and digest-pinned. Data persists in Docker named volumes by default. The single broker uses replication factor 1 and does not provide disk-loss tolerance.
