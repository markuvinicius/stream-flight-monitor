package com.flightmonitor.contracts.event;

import java.time.Instant;
import java.util.Objects;

public record LocationEventEnvelope(
        int schemaVersion,
        LocationEventType eventType,
        String eventId,
        String locationId,
        long locationGeneration,
        Instant emittedAt,
        Object payload
) {
    public LocationEventEnvelope {
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(locationId, "locationId");
        Objects.requireNonNull(emittedAt, "emittedAt");
        if (schemaVersion < 1) {
            throw new IllegalArgumentException("schemaVersion must be >= 1");
        }
    }
}
