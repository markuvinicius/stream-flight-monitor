# Session: write-api-tests
Skill: write-api-tests
Status: complete
Last saved: 2026-11-11 17:35 UTC

## Phase 1 - Configuration
Spec path: derived from implementation and docs/lifecycle-api.md (no OpenAPI/Swagger file exists)
Base URL: http://127.0.0.1:8080
Output directory: api-requests

## Phase 2 - Endpoint Scope
Total endpoints: 3
Selected endpoints: 3
Endpoints:
- POST /api/locations - Register a location with latitude/longitude and a client-generated Idempotency-Key; requires X-Flight-Monitor: 1.
- GET /api/operations/{id} - Resolve a committed lifecycle operation result.
- DELETE /api/locations/{id} - Delete a location after committed serving-gate confirmation; requires X-Flight-Monitor: 1.

The current-location and snapshot/history routes are not implemented and are excluded.
Selection: all (user-approved).

## Phase 3 - Test Cases

Endpoint: POST /api/locations
Description: Register a location with a client-generated idempotency key and local mutation protection.
Test cases:
- TC-1 Happy path and upper boundaries: latitude=90, longitude=-180; expect 201, locationId/operationId as 64-char hex, trackingUrl, createdAt, WAITING_FOR_FIRST_POLL, and Location header.
- TC-2 Idempotent retry: same key and coordinates; expect 201 and identical locationId, operationId and createdAt.
- TC-3 Idempotency conflict: same key with changed coordinates; expect 409.
- TC-4 Opposite coordinate boundaries: latitude=-90, longitude=180; expect 201; retain returned ID for cleanup by DELETE request.
- TC-5 Missing latitude; expect structured 400.
- TC-6 Latitude above 90; expect structured 400.
- TC-7 Longitude below -180; expect structured 400.
- TC-8 Missing Idempotency-Key; expect structured 400.
- TC-9 Missing X-Flight-Monitor mutation header; expect 403.
- TC-10 Cross-origin Origin; expect 403.

Endpoint: GET /api/operations/{id}
Description: Resolve a committed lifecycle operation result.
Test cases:
- TC-11 Happy path using TC-1 operationId; expect 200 and operationId, eventId, locationId, httpStatus=201, code and expiry fields.
- TC-12 Unknown well-formed 64-character ID; expect structured 404 with explicit unconfirmed-write warning.
- TC-13 Malformed operation ID; expect structured 400.

Endpoint: DELETE /api/locations/{id}
Description: Delete an active location after committed read_committed serving-gate confirmation.
Test cases:
- TC-14 Cleanup registration from TC-4; expect 202 and cleanupPending=true.
- TC-15 Unknown well-formed 64-character ID; expect structured 404.
- TC-16 Malformed location ID; expect structured 400.
- TC-17 Missing X-Flight-Monitor mutation header for TC-1 location; expect 403 and location remains available for the later delete.
- TC-18 Happy path deleting TC-1 location; expect 202 only after committed delete projection and cleanupPending=true.
- TC-19 Repeat deletion of TC-1 location; expect idempotent 202.

Collection sequencing: registration happy path initializes a fresh runtime Idempotency-Key and captures location/operation IDs; same-key retry/conflict follow. Opposite-boundary registration is deleted before other cases. Operation lookup uses the first registration result. The primary location is deleted last. API cases requiring Kafka readiness need the local Compose stack, but the collection never calls the upstream aircraft API.

## Phase 4 - Generation
Generated files:
- api-requests/bruno.json
- api-requests/environments/local.bru
- api-requests/README.md
- 19 ordered Bruno request files (POST registration validation/idempotency, GET operation resolution, DELETE lifecycle)
Status: complete (approved by user).

Validation:
- Bruno CLI 3.5.2 loaded all 19 requests and all test scripts passed against a temporary local contract stub: 19/19 requests, 19/19 tests.
- This was a collection syntax/flow check only; it did not call the Flight Monitor application. The local Compose stack was unavailable, so live API behavior remains unverified.

User reviewed and approved the generated collection.
