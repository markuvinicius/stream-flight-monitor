# Stream Flight Monitor API requests

This Bruno collection exercises the implemented location lifecycle API only. It does not call adsb.fi. Current-flight and snapshot/history routes are not implemented and are intentionally absent.

## Requirements

- Start the local Compose stack and wait for the monitor readiness health check.
- Open this folder as a collection in Bruno and select the `local` environment.
- Requests are deliberately sequenced. Run the entire collection in order; the first registration creates a fresh idempotency key and stores response IDs for later requests. The final requests delete the locations created by the run.
- Mutation requests include `X-Flight-Monitor: 1`. The local monitor must be reachable at `http://127.0.0.1:8080`.

You can run it with Bruno CLI using `bru run --env local` from this directory.

The collection uses boundary coordinates, not personal coordinates. A successful registration temporarily stores the coordinates in the local Kafka registry until the location is deleted. Do not expose the local API to a shared or remote network.

## Requests

1. Register at the upper coordinate boundary, retry with the same idempotency key, then verify conflicting coordinates are rejected.
2. Register at the opposite coordinate boundary and delete that temporary location.
3. Check missing and out-of-range values, a missing idempotency key, a missing mutation header, and cross-origin rejection.
4. Read the committed operation, then test unknown and malformed IDs.
5. Test unknown/malformed location IDs, missing mutation protection, delete the primary test location, and repeat the delete.

Collection assertions validate status and key response fields. The lifecycle API requires a healthy local Kafka broker; a missing/uncertain result is reported as a failing request rather than treated as success.
