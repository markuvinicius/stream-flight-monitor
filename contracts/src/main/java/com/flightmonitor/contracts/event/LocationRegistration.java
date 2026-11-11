package com.flightmonitor.contracts.event;

import java.time.Instant;
import java.util.Objects;

public record LocationRegistration(
        String locationId,
        String locationKey,
        double latitude,
        double longitude,
        Instant registeredAt,
        long locationGeneration,
        String sourceClientId
) {
    public LocationRegistration {
        Objects.requireNonNull(locationId, "locationId");
        Objects.requireNonNull(locationKey, "locationKey");
        Objects.requireNonNull(registeredAt, "registeredAt");
        Objects.requireNonNull(sourceClientId, "sourceClientId");
        if (!Double.isFinite(latitude) || !Double.isFinite(longitude)) {
            throw new IllegalArgumentException("latitude and longitude must be finite");
        }
    }
}
