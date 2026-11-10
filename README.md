# Stream Flight Monitor

Local development scaffold for the approved dedicated-poller architecture. The initial runtime contains a Spring Boot monitor, a separate Spring Boot poller process, and one persistent single-node Kafka broker. This task establishes build/runtime foundations only: no API, Kafka contract or topology, aircraft processing, or upstream requests are implemented.

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

The reactor builds the `monitor` and `poller` modules. Unit-level smoke tests start both Spring applications, verify the actuator health endpoint, and assert that upstream polling remains disabled.

## Start the local runtime

```powershell
docker compose up --build
```

The monitor health endpoint is available at [http://127.0.0.1:8080/actuator/health](http://127.0.0.1:8080/actuator/health). Kafka and the poller have container health checks but no host-published ports. Both applications receive `broker:9092` over the private Compose network. Only the monitor is published, and only on the host loopback interface.

To inspect service state and logs:

```powershell
docker compose ps
docker compose logs -f broker monitor poller
```

Stop the services while retaining their data:

```powershell
docker compose down
```

The broker log directory, monitor state directory, and poller checkpoint/state directory are separate named volumes. `docker compose down -v` permanently deletes all three volumes and their contents; do not use it if local data must be retained.

The Compose stack targets 2 GiB for Kafka, 2 GiB for the monitor, and 512 MiB for the poller. JVM heaps are capped at 768 MiB, 768 MiB, and 192 MiB respectively, leaving room for native allocations and RocksDB. These limits are a configuration envelope, not evidence that the final history workload meets the total-memory target.

## Configuration and scope

Typed settings are defined in each module and use `application.yml` plus environment-variable overrides. Both `monitor.upstream-polling-enabled` and `poller.upstream-polling-enabled` default to `false` and remain explicitly false in Compose. No scheduler or upstream HTTP client is present in this scaffold.

Container image versions are exact and digest-pinned. Data persists in Docker named volumes by default. The single broker uses replication factor 1 and does not provide disk-loss tolerance.
