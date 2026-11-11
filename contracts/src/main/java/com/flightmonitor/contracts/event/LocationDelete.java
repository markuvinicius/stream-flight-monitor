package com.flightmonitor.contracts.event;

import java.time.Instant;
import java.util.Objects;

public record LocationDelete(
        String locationId,
        long locationGeneration,
        Instant deletedAt,
        String reason
) {
    public LocationDelete {
        Objects.requireNonNull(locationId, "locationId");
        Objects.requireNonNull(deletedAt, "deletedAt");
    }
}
