package com.flightmonitor.contracts.event;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public record LocationSnapshot(
        String locationId,
        long locationGeneration,
        String requestId,
        Instant capturedAt,
        Instant sourceObservedAt,
        boolean success,
        boolean empty,
        String status,
        List<SourceAircraftSnapshot> aircraft,
        String errorCategory,
        String errorMessage,
        int qualityCount,
        String sourceUrl
) {
    public LocationSnapshot {
        Objects.requireNonNull(locationId, "locationId");
        Objects.requireNonNull(requestId, "requestId");
        Objects.requireNonNull(capturedAt, "capturedAt");
        if (aircraft == null) {
            aircraft = List.of();
        }
    }
}
