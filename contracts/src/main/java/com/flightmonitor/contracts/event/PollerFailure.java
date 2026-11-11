package com.flightmonitor.contracts.event;

import java.time.Instant;
import java.util.Objects;

public record PollerFailure(
        String locationId,
        long locationGeneration,
        String requestId,
        Instant occurredAt,
        String errorCategory,
        String errorMessage,
        int retryDelaySeconds
) {
    public PollerFailure {
        Objects.requireNonNull(locationId, "locationId");
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(errorCategory, "errorCategory");
    }
}
