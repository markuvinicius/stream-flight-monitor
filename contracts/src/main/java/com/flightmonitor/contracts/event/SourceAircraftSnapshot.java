package com.flightmonitor.contracts.event;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public record SourceAircraftSnapshot(
        String aircraftId,
        String flightNumber,
        Double latitude,
        Double longitude,
        Double speedKts,
        Double altitudeFeet,
        Instant observedAt,
        String source,
        List<String> notes
) {
    public SourceAircraftSnapshot {
        Objects.requireNonNull(aircraftId, "aircraftId");
        Objects.requireNonNull(observedAt, "observedAt");
    }
}
