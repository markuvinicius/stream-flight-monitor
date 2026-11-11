package com.flightmonitor.contracts.event;

import java.time.Instant;
import java.util.Objects;

public record LocationRegistryRecord(
        String locationId,
        long locationGeneration,
        LocationLifecycleState state,
        String eventId,
        Instant updatedAt,
        String sourceClientId
) {
    public LocationRegistryRecord {
        Objects.requireNonNull(locationId, "locationId");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(updatedAt, "updatedAt");
    }
}
