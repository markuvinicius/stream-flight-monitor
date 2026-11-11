package com.flightmonitor.monitor.lifecycle;

import com.flightmonitor.contracts.event.LocationLifecycleState;
import com.flightmonitor.contracts.event.LifecycleWire;
import com.flightmonitor.contracts.event.LifecycleWire.Operation;
import com.flightmonitor.contracts.event.LifecycleWire.Projection;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Bounded, read-committed serving gate. Only the dedicated registry consumer may apply records;
 * Streams interactive-query state is deliberately never read by HTTP code.
 */
public final class CommittedRegistry {
    private final LifecycleSettings settings;
    private final Clock clock;
    private final Map<String, Projection> locations = new HashMap<>();
    private final Map<String, Operation> operations = new HashMap<>();
    private boolean restored;
    private Instant lastProgress = Instant.MIN;
    private String waitingEvent;
    private Operation observedRejection;

    public CommittedRegistry(LifecycleSettings settings, Clock clock) {
        this.settings = settings;
        this.clock = clock;
    }

    public synchronized void apply(String key, String json) {
        if (key == null || !key.matches("[a-f0-9]{64}")) {
            throw new IllegalStateException("Invalid registry key");
        }
        Projection old = locations.remove(key);
        if (old != null) {
            old.operations().forEach(op -> operations.remove(op.operationId()));
        }
        if (json != null) {
            Projection next = LifecycleWire.read(json, Projection.class);
            if ((next.registry() != null && !key.equals(next.registry().locationId()))
                    || next.operations().size() > settings.maxOperationsPerLocation()
                    || next.operations().stream().anyMatch(op -> !key.equals(op.locationId()))
                    || (next.admissionRejection() != null && !key.equals(next.admissionRejection().locationId()))) {
                throw new IllegalStateException("Invalid committed projection");
            }
            if (next.admissionRejection() != null
                    && next.admissionRejection().eventId().equals(waitingEvent)) {
                observedRejection = next.admissionRejection();
            }
            // A cold replay may encounter expired rejected identities before their later
            // tombstones. Do not count those historical entries against today's live bound.
            Instant now = clock.instant();
            var liveOperations = next.operations().stream().filter(op -> now.isBefore(op.expiresAt())).toList();
            if (next.registry() != null || !liveOperations.isEmpty()) {
                var live = new Projection(1, next.registry(), next.activeRegistration(), next.createdAt(),
                        next.keyExpiresAt(), liveOperations, next.admissionRejection(), next.registrationFingerprint());
                locations.put(key, live);
                if (locations.size() > settings.maxLifetime()) {
                    throw new IllegalStateException("Registry exceeds declared lifetime capacity");
                }
                liveOperations.forEach(op -> operations.put(op.operationId(), op));
            }
        }
        notifyAll();
    }

    public synchronized void progress(boolean restored) {
        this.restored = restored;
        lastProgress = clock.instant();
        notifyAll();
    }

    public synchronized void unavailable() {
        restored = false;
        notifyAll();
    }

    public synchronized boolean ready() {
        return restored && Duration.between(lastProgress, clock.instant()).compareTo(Duration.ofSeconds(3)) < 0;
    }

    public synchronized Optional<Projection> location(String id) {
        return Optional.ofNullable(locations.get(id)).filter(projection -> projection.registry() != null);
    }

    public synchronized Optional<Operation> operation(String id) {
        Operation op = operations.get(id);
        return op == null || !clock.instant().isBefore(op.expiresAt()) ? Optional.empty() : Optional.of(op);
    }

    /** Includes the single non-admitted overflow observation for pending-request reconciliation. */
    public synchronized Optional<Operation> observation(String id) {
        var retained = operation(id);
        if (retained.isPresent()) {
            return retained;
        }
        return observedRejection != null && id.equals(observedRejection.eventId())
                && clock.instant().isBefore(observedRejection.expiresAt())
                ? Optional.of(observedRejection) : Optional.empty();
    }

    public synchronized boolean hasAdmissionRoom(String id, boolean deletion) {
        Projection projection = locations.get(id);
        if (projection != null) {
            return projection.operations().stream().filter(op -> clock.instant().isBefore(op.expiresAt())).count()
                    < settings.maxOperationsPerLocation() - (deletion ? 0 : 1);
        }
        long active = locations.values().stream().filter(p -> p.registry() != null
                && p.registry().state() == LocationLifecycleState.ACTIVE).count();
        return active < settings.maxActive() && locations.size() < settings.maxLifetime();
    }

    public synchronized void waitingFor(String eventId) {
        waitingEvent = eventId;
        observedRejection = null;
    }

    /** One request deadline covers publication and observation; timeout never implies rollback. */
    public synchronized Operation await(String eventId, long deadlineNanos) throws InterruptedException {
        while (true) {
            Operation op = observation(eventId).orElse(null);
            if (op != null) {
                return op;
            }
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0 || !ready()) {
                return null;
            }
            long millis = Math.max(1, Math.min(100, remaining / 1_000_000));
            wait(millis);
        }
    }
}
