package com.flightmonitor.monitor.lifecycle;

import com.flightmonitor.contracts.event.LocationDelete;
import com.flightmonitor.contracts.event.LocationEventEnvelope;
import com.flightmonitor.contracts.event.LocationEventType;
import com.flightmonitor.contracts.event.LocationLifecycleState;
import com.flightmonitor.contracts.event.LocationRegistration;
import com.flightmonitor.contracts.event.LifecycleWire;
import com.flightmonitor.contracts.event.LifecycleWire.Operation;
import com.flightmonitor.contracts.event.LifecycleWire.Projection;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Single-monitor admission boundary. At most one command is outstanding; following a timeout,
 * different mutations fail closed until the exact pending event is resolved by the committed gate.
 */
public final class LifecycleService {
    private final LifecycleSettings settings;
    private final CommittedRegistry gate;
    private final LifecyclePublisher publisher;
    private final Clock clock;
    private final Duration visibilityTimeout;
    private final ReentrantLock mutation = new ReentrantLock();
    private volatile String pending;

    /** Creates the admission boundary with a visibility budget no greater than ten seconds. */
    public LifecycleService(LifecycleSettings settings, CommittedRegistry gate,
                            LifecyclePublisher publisher, Clock clock, Duration visibilityTimeout) {
        if (visibilityTimeout.isNegative() || visibilityTimeout.compareTo(Duration.ofSeconds(10)) > 0) {
            throw new IllegalArgumentException("Lifecycle visibility timeout must not exceed ten seconds");
        }
        this.settings = settings;
        this.gate = gate;
        this.publisher = publisher;
        this.clock = clock;
        this.visibilityTimeout = visibilityTimeout;
    }

    /** Public registration receipt; the tracking URL is same-origin and does not trigger polling. */
    public record Registration(String locationId, String operationId, String trackingUrl,
                               Instant createdAt, String state) { }

    /** Deletion closes serving; physical cleanup remains asynchronous. */
    public record Deletion(String locationId, String operationId, boolean cleanupPending) { }

    /** Registers or resolves a coordinate-bound retry; never acknowledges producer acceptance alone. */
    public Registration register(String key, double latitude, double longitude) {
        if (key == null || !key.matches("[\\x21-\\x7e]{1,128}")) {
            throw failure(400, "INVALID_IDEMPOTENCY_KEY", "Idempotency-Key must contain 1–128 printable ASCII characters");
        }
        try {
            LifecycleWire.coordinates(latitude, longitude);
        } catch (IllegalArgumentException e) {
            throw failure(400, "INVALID_COORDINATES", e.getMessage());
        }
        String id = LifecycleWire.locationId(settings.namespace(), key);
        String fingerprint = LifecycleWire.fingerprint(latitude, longitude);
        String operationId = LifecycleWire.operationId(id, fingerprint);
        if (!mutation.tryLock()) {
            if (operationId.equals(pending)) {
                Operation op = waitFor(operationId, System.nanoTime() + visibilityTimeout.toNanos());
                return registrationReceipt(id, op);
            }
            throw failure(503, "MUTATION_BUSY", "Another lifecycle mutation is being confirmed");
        }
        try {
            requireReady();
            Projection current = gate.location(id).orElse(null);
            if (current != null) {
                if (current.registry().state() == LocationLifecycleState.DELETED) {
                    throw failure(410, "LOCATION_DELETED", "Deleted IDs cannot be reused");
                }
                if (!clock.instant().isBefore(current.keyExpiresAt())) {
                    throw failure(409, "IDEMPOTENCY_KEY_EXPIRED", "Expired keys cannot be reused");
                }
            }
            Instant now = clock.instant();
            var registration = new LocationRegistration(id, fingerprint, latitude, longitude, now, 1, settings.namespace());
            Operation op = execute(new LocationEventEnvelope(1, LocationEventType.REGISTER,
                    operationId, id, 1, now, registration));
            return registrationReceipt(id, op);
        } finally {
            mutation.unlock();
        }
    }

    /** Returns only after a committed terminal projection, or an already-observed terminal marker. */
    public Deletion delete(String id) {
        validateId(id);
        String operationId = LifecycleWire.operationId(id, "DELETE");
        if (!mutation.tryLock()) {
            if (operationId.equals(pending)) {
                Operation op = waitFor(operationId, System.nanoTime() + visibilityTimeout.toNanos());
                checkResult(op);
                return new Deletion(id, operationId, true);
            }
            throw failure(503, "MUTATION_BUSY", "Another lifecycle mutation is being confirmed");
        }
        try {
            requireReady();
            Projection current = gate.location(id).orElseThrow(() ->
                    failure(404, "LOCATION_NOT_FOUND", "Unknown location"));
            if (current.registry().state() == LocationLifecycleState.DELETED) {
                // This terminal state itself was observed read_committed, including after recovery.
                return new Deletion(id, operationId, true);
            }
            Instant now = clock.instant();
            Operation op = execute(new LocationEventEnvelope(1, LocationEventType.DELETE, operationId,
                    id, 1, now, new LocationDelete(id, 1, now, "user-requested")));
            checkResult(op);
            if (gate.location(id).orElseThrow().registry().state() != LocationLifecycleState.DELETED) {
                throw uncertain(operationId);
            }
            return new Deletion(id, operationId, true);
        } finally {
            mutation.unlock();
        }
    }

    /** Reads an unexpired committed result; absence is not evidence that a write was rolled back. */
    public Operation operation(String id) {
        validateId(id);
        if (!gate.ready()) {
            throw failure(503, "REGISTRY_UNAVAILABLE", "Committed registry is restoring or unavailable");
        }
        return gate.operation(id).orElseThrow(() -> id.equals(pending) ? uncertain(id)
                : failure(404, "OPERATION_NOT_FOUND",
                "No committed result is retained; an unconfirmed write may still commit"));
    }

    /** Task 6 must call this gate before, and again immediately before releasing, any location data. */
    public Projection requireActive(String id) {
        requireReady();
        var projection = gate.location(id).orElseThrow(() ->
                failure(404, "LOCATION_NOT_FOUND", "Unknown location"));
        if (projection.registry().state() == LocationLifecycleState.DELETED) {
            throw failure(410, "LOCATION_DELETED", "Location has been deleted");
        }
        return projection;
    }

    private Operation execute(LocationEventEnvelope event) {
        String id = event.eventId();
        long deadline = System.nanoTime() + visibilityTimeout.toNanos();
        if (pending != null && gate.observation(pending).isPresent()) {
            pending = null;
        }
        if (pending != null && !pending.equals(id)) {
            throw new LifecycleFailure(503, "OUTCOME_UNCERTAIN",
                    "A previous write remains unconfirmed; mutations are paused, not rolled back", pending);
        }
        Operation existing = gate.operation(id).orElse(null);
        if (existing != null) {
            return existing;
        }
        if (pending == null) {
            if (!gate.hasAdmissionRoom(event.locationId(), event.eventType() == LocationEventType.DELETE)) {
                throw failure(409, "ADMISSION_CAPACITY", "Location or retained-operation capacity is exhausted");
            }
            pending = id;
            gate.waitingFor(id);
            // Publication runs on one bounded worker. It cannot extend the HTTP visibility deadline.
            publisher.publish(event);
        }
        Operation result = waitFor(id, deadline);
        pending = null;
        return result;
    }

    private Operation waitFor(String id, long deadline) {
        try {
            Operation result = gate.await(id, deadline);
            if (result == null) {
                throw uncertain(id);
            }
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw uncertain(id);
        }
    }

    private Registration registrationReceipt(String id, Operation op) {
        checkResult(op);
        if (!gate.ready()) {
            throw uncertain(op.operationId());
        }
        Projection confirmed = gate.location(id).orElseThrow(() -> uncertain(op.operationId()));
        if (confirmed.registry().state() == LocationLifecycleState.DELETED) {
            throw failure(410, "LOCATION_DELETED", "Location has been deleted");
        }
        return new Registration(id, op.operationId(), "/locations/" + id,
                confirmed.createdAt(), "WAITING_FOR_FIRST_POLL");
    }

    private void requireReady() {
        if (!publisher.ready() || !gate.ready()) {
            throw failure(503, "LIFECYCLE_UNAVAILABLE", "Kafka or committed lifecycle restoration is unavailable");
        }
    }

    private static void validateId(String id) {
        if (id == null || !id.matches("[a-f0-9]{64}")) {
            throw failure(400, "INVALID_ID", "Expected an opaque 64-character hexadecimal ID");
        }
    }

    private static void checkResult(Operation op) {
        if (op.httpStatus() >= 400) {
            throw new LifecycleFailure(op.httpStatus(), op.code(), "Lifecycle command was rejected", op.operationId());
        }
    }

    private static LifecycleFailure uncertain(String id) {
        return new LifecycleFailure(503, "OUTCOME_UNCERTAIN",
                "Outcome not yet confirmed; the write may still commit. Query the operation or retry the same request.", id);
    }

    private static LifecycleFailure failure(int status, String code, String detail) {
        return new LifecycleFailure(status, code, detail, null);
    }
}
