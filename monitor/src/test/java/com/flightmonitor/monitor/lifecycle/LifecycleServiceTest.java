package com.flightmonitor.monitor.lifecycle;

import com.flightmonitor.contracts.event.*;
import com.flightmonitor.contracts.event.LifecycleWire.*;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.*;

class LifecycleServiceTest {
    private final LifecycleSettings settings = new LifecycleSettings(false, null, null, null, 1, 2, 4);
    private final Clock clock = Clock.fixed(Instant.parse("2026-11-11T12:00:00Z"), ZoneOffset.UTC);
    private final CommittedRegistry gate = new CommittedRegistry(settings, clock);
    private final AtomicInteger sends = new AtomicInteger();
    private final AtomicReference<LocationEventEnvelope> sent = new AtomicReference<>();
    private final AtomicBoolean available = new AtomicBoolean(true);
    private final LifecyclePublisher publisher = new LifecyclePublisher() {
        public boolean ready() { return available.get(); }
        public CompletableFuture<Void> publish(LocationEventEnvelope event) {
            sends.incrementAndGet();
            sent.set(event);
            return CompletableFuture.completedFuture(null);
        }
    };
    private final LifecycleService service = new LifecycleService(settings, gate, publisher, clock, Duration.ofMillis(30));

    private Projection committed(LocationEventEnvelope event, LocationLifecycleState state) {
        var op = new Operation(event.eventId(), event.eventId(), event.locationId(),
                state == LocationLifecycleState.ACTIVE ? 201 : 202, "COMPLETED", clock.instant(),
                clock.instant().plus(Duration.ofHours(24)));
        var registration = state == LocationLifecycleState.ACTIVE
                ? LifecycleWire.payload(event, LocationRegistration.class) : null;
        var projection = new Projection(1, new LocationRegistryRecord(event.locationId(), 1, state,
                event.eventId(), clock.instant(), state == LocationLifecycleState.ACTIVE ? settings.namespace() : null),
                registration, clock.instant(), clock.instant().plus(Duration.ofHours(24)), List.of(op));
        gate.apply(event.locationId(), LifecycleWire.json(projection));
        return projection;
    }

    @Test
    void acknowledgementOfProducerAloneIsNotSuccessAndSameEventCanResolveLater() {
        gate.progress(true);
        assertThatThrownBy(() -> service.register("key", 10, 20)).isInstanceOfSatisfying(LifecycleFailure.class, e -> {
            assertThat(e.status()).isEqualTo(503);
            assertThat(e.code()).isEqualTo("OUTCOME_UNCERTAIN");
            assertThat(e.operationId()).isEqualTo(sent.get().eventId());
        });
        assertThatThrownBy(() -> service.register("different", 10, 20))
                .isInstanceOfSatisfying(LifecycleFailure.class, e -> assertThat(e.code()).isEqualTo("OUTCOME_UNCERTAIN"));
        assertThatThrownBy(() -> service.operation(sent.get().eventId()))
                .isInstanceOfSatisfying(LifecycleFailure.class, e -> assertThat(e.code()).isEqualTo("OUTCOME_UNCERTAIN"));
        assertThat(sends.get()).isEqualTo(1);
        Projection projection = committed(sent.get(), LocationLifecycleState.ACTIVE);
        var receipt = service.register("key", 10, 20);
        assertThat(receipt.locationId()).isEqualTo(sent.get().locationId());
        assertThat(receipt.createdAt()).isEqualTo(projection.createdAt());
        assertThat(receipt.state()).isEqualTo("WAITING_FOR_FIRST_POLL");
        assertThat(service.operation(receipt.operationId()).httpStatus()).isEqualTo(201);
        assertThat(sends.get()).isEqualTo(1);
    }

    @Test
    void deletionCannotAcknowledgeBeforeCommittedProjectionAndClosesServingAfterIt() {
        gate.progress(true);
        assertThatThrownBy(() -> service.register("key", 10, 20)).isInstanceOf(LifecycleFailure.class);
        var registration = sent.get();
        committed(registration, LocationLifecycleState.ACTIVE);
        service.register("key", 10, 20);
        assertThatThrownBy(() -> service.delete(registration.locationId())).isInstanceOfSatisfying(
                LifecycleFailure.class, e -> assertThat(e.code()).isEqualTo("OUTCOME_UNCERTAIN"));
        assertThat(service.requireActive(registration.locationId()).registry().state()).isEqualTo(LocationLifecycleState.ACTIVE);
        committed(sent.get(), LocationLifecycleState.DELETED);
        assertThat(service.delete(registration.locationId()).cleanupPending()).isTrue();
        assertThat(service.delete(registration.locationId()).cleanupPending()).isTrue();
        assertThatThrownBy(() -> service.requireActive(registration.locationId()))
                .isInstanceOfSatisfying(LifecycleFailure.class, e -> assertThat(e.status()).isEqualTo(410));
        assertThatThrownBy(() -> service.register("key", 10, 20))
                .isInstanceOfSatisfying(LifecycleFailure.class, e -> assertThat(e.status()).isEqualTo(410));
        assertThat(sends.get()).isEqualTo(2);
    }

    @Test
    void restorationAndKafkaUnavailabilityFailWithoutPublishing() {
        assertThatThrownBy(() -> service.register("key", 0, 0)).isInstanceOfSatisfying(
                LifecycleFailure.class, e -> assertThat(e.status()).isEqualTo(503));
        gate.progress(true);
        available.set(false);
        assertThatThrownBy(() -> service.register("key", 0, 0)).isInstanceOf(LifecycleFailure.class);
        assertThat(sends.get()).isZero();
    }

    @Test
    void invalidInputAndUnknownIdentifiersAreExplicit() {
        assertThatThrownBy(() -> service.register(null, 0, 0)).isInstanceOfSatisfying(
                LifecycleFailure.class, e -> assertThat(e.status()).isEqualTo(400));
        for (double lat : new double[]{Double.NaN, Double.POSITIVE_INFINITY, -90.0001, 90.0001}) {
            assertThatThrownBy(() -> service.register("key", lat, 0)).isInstanceOf(LifecycleFailure.class);
        }
        gate.progress(true);
        assertThatThrownBy(() -> service.delete("a".repeat(64))).isInstanceOfSatisfying(
                LifecycleFailure.class, e -> assertThat(e.status()).isEqualTo(404));
        assertThatThrownBy(() -> service.operation("a".repeat(64))).isInstanceOfSatisfying(
                LifecycleFailure.class, e -> assertThat(e.status()).isEqualTo(404));
        assertThatThrownBy(() -> service.delete("invalid")).isInstanceOfSatisfying(
                LifecycleFailure.class, e -> assertThat(e.status()).isEqualTo(400));
    }

    @Test
    void replayRebuildsGateAndExpiredOperationIsNotReturned() {
        gate.progress(true);
        assertThatThrownBy(() -> service.register("key", 10, 20)).isInstanceOf(LifecycleFailure.class);
        Projection projection = committed(sent.get(), LocationLifecycleState.ACTIVE);
        CommittedRegistry replay = new CommittedRegistry(settings, clock);
        replay.apply(sent.get().locationId(), LifecycleWire.json(projection));
        assertThat(replay.ready()).isFalse();
        replay.progress(true);
        assertThat(replay.operation(sent.get().eventId())).isPresent();
        CommittedRegistry expired = new CommittedRegistry(settings,
                Clock.offset(clock, Duration.ofHours(24)));
        expired.apply(sent.get().locationId(), LifecycleWire.json(projection));
        assertThat(expired.operation(sent.get().eventId())).isEmpty();
        assertThat(expired.location(sent.get().locationId())).isPresent();
    }

    @Test
    void tenSecondsIsAHardConfigurationMaximum() {
        assertThatThrownBy(() -> new LifecycleService(settings, gate, publisher, clock, Duration.ofSeconds(11)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void coldReplayDoesNotCountExpiredRejectedIdentitiesBeforeTheirTombstones() {
        Instant past = clock.instant().minus(Duration.ofHours(25));
        for (String key : new String[]{"one", "two", "three", "four"}) {
            String id = LifecycleWire.locationId(settings.namespace(), key);
            String eventId = LifecycleWire.operationId(id, "fingerprint");
            var result = new Operation(eventId, eventId, id, 409, "LOCATION_CAPACITY",
                    past, past.plus(Duration.ofHours(24)));
            var expired = new Projection(1, null, null, null, null, List.of(result));
            gate.apply(id, LifecycleWire.json(expired));
            assertThat(gate.operation(eventId)).isEmpty();
        }
        gate.progress(true);
        assertThat(gate.hasAdmissionRoom(LifecycleWire.locationId(settings.namespace(), "new"), false)).isTrue();
    }

    @Test
    void simultaneousSameKeyRetriesShareOneEventAndOneCommittedResult() throws Exception {
        gate.progress(true);
        var waitingService = new LifecycleService(settings, gate, publisher, clock, Duration.ofSeconds(2));
        try (var executor = Executors.newFixedThreadPool(2)) {
            Future<LifecycleService.Registration> first = executor.submit(() -> waitingService.register("key", 10, 20));
            long deadline = System.nanoTime() + Duration.ofSeconds(1).toNanos();
            while (sent.get() == null && System.nanoTime() < deadline) {
                Thread.sleep(1);
            }
            assertThat(sent.get()).isNotNull();
            Future<LifecycleService.Registration> second = executor.submit(() -> waitingService.register("key", 10, 20));
            committed(sent.get(), LocationLifecycleState.ACTIVE);
            assertThat(second.get(2, TimeUnit.SECONDS)).isEqualTo(first.get(2, TimeUnit.SECONDS));
            assertThat(sends.get()).isEqualTo(1);
        }
    }
}
