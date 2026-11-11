package com.flightmonitor.contracts;

import com.flightmonitor.contracts.event.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LifecycleProjectionContractTest {
    private final Instant now = Instant.parse("2026-11-11T12:00:00Z");
    private final String id = LifecycleWire.locationId("local", "key");
    private final String fingerprint = LifecycleWire.fingerprint(10, 20);
    private final String event = LifecycleWire.operationId(id, fingerprint);

    @Test
    void projectionAndRetainedOperationRoundTripWithoutChangingLegacyRecords() {
        var registration = new LocationRegistration(id, fingerprint, 10, 20, now, 1, "local");
        var registry = new LocationRegistryRecord(id, 1, LocationLifecycleState.ACTIVE, event, now, "local");
        var operation = new LifecycleWire.Operation(event, event, id, 201, "WAITING_FOR_FIRST_POLL",
                now, now.plus(Duration.ofHours(24)));
        var projection = new LifecycleWire.Projection(1, registry, registration, now,
                operation.expiresAt(), List.of(operation));
        assertEquals(projection, LifecycleWire.read(LifecycleWire.json(projection), LifecycleWire.Projection.class));
        assertEquals(registry, LocationEventSerializer.fromJson(LocationEventSerializer.toJson(registry),
                LocationRegistryRecord.class));
    }

    @Test
    void terminalProjectionCannotContainCoordinatesOrFingerprints() {
        var registry = new LocationRegistryRecord(id, 1, LocationLifecycleState.DELETED, event, now, null);
        assertThrows(IllegalArgumentException.class, () -> new LifecycleWire.Projection(1, registry,
                new LocationRegistration(id, fingerprint, 10, 20, now, 1, "local"),
                now, now.plus(Duration.ofHours(24)), List.of()));
        assertThrows(IllegalArgumentException.class, () -> new LifecycleWire.Projection(1, registry,
                null, now, now.plus(Duration.ofHours(24)), List.of(), null, fingerprint));
    }

    @Test
    void missingOrStringCoordinatesCannotBecomePrimitiveZeroOnObjectPayloadDispatch() {
        for (var payload : List.of(Map.of("longitude", 0), Map.of("latitude", "0", "longitude", 0))) {
            var envelope = new LocationEventEnvelope(1, LocationEventType.REGISTER, event, id, 1, now, payload);
            assertThrows(IllegalArgumentException.class, () -> LifecycleWire.payload(envelope, LocationRegistration.class));
        }
    }
}
