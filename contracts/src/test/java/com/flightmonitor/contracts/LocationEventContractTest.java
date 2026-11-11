package com.flightmonitor.contracts;

import com.flightmonitor.contracts.event.LocationDelete;
import com.flightmonitor.contracts.event.LocationEventEnvelope;
import com.flightmonitor.contracts.event.LocationEventSerializer;
import com.flightmonitor.contracts.event.LocationEventType;
import com.flightmonitor.contracts.event.LocationLifecycleState;
import com.flightmonitor.contracts.event.LocationRegistration;
import com.flightmonitor.contracts.event.LocationRegistryRecord;
import com.flightmonitor.contracts.event.LocationSnapshot;
import com.flightmonitor.contracts.event.PollerFailure;
import com.flightmonitor.contracts.event.SourceAircraftSnapshot;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocationEventContractTest {

    @Test
    void serializes_and_round_trips_location_event_envelope() {
        var registration = new LocationRegistration(
                "loc-123",
                "client-key-123",
                51.5,
                -0.12,
                Instant.parse("2026-11-10T12:00:00Z"),
                1L,
                "browser-session-1"
        );

        var envelope = new LocationEventEnvelope(
                1,
                LocationEventType.REGISTER,
                "evt-1",
                "loc-123",
                1L,
                Instant.parse("2026-11-10T12:00:01Z"),
                registration
        );

        String json = LocationEventSerializer.toJson(envelope);
        assertTrue(json.contains("\"schemaVersion\":1"));
        assertTrue(json.contains("\"eventType\":\"REGISTER\""));

        var parsed = LocationEventSerializer.fromJson(json, LocationEventEnvelope.class);
        assertEquals("loc-123", parsed.locationId());
        assertEquals(LocationEventType.REGISTER, parsed.eventType());
        assertEquals(1L, parsed.locationGeneration());
    }

    @Test
    void serializes_snapshot_and_registry_records() {
        var aircraft = new SourceAircraftSnapshot(
                "a1",
                "BAW123",
                51.5001,
                -0.1201,
                420.0,
                23000.0,
                Instant.parse("2026-11-10T12:08:00Z"),
                "adsb.fi",
                List.of("ok")
        );

        var snapshot = new LocationSnapshot(
                "loc-123",
                1L,
                "request-9",
                Instant.parse("2026-11-10T12:08:05Z"),
                Instant.parse("2026-11-10T12:08:00Z"),
                true,
                false,
                "ACTIVE",
                List.of(aircraft),
                null,
                null,
                1,
                "https://opendata.adsb.fi/api/v3/lat/51.5/lon/-0.12/dist/25"
        );

        String snapshotJson = LocationEventSerializer.toJson(snapshot);
        assertTrue(snapshotJson.contains("\"requestId\":\"request-9\""));

        var registry = new LocationRegistryRecord(
                "loc-123",
                1L,
                LocationLifecycleState.ACTIVE,
                "evt-9",
                Instant.parse("2026-11-10T12:08:05Z"),
                "browser-session-1"
        );

        String registryJson = LocationEventSerializer.toJson(registry);
        assertTrue(registryJson.contains("\"state\":\"ACTIVE\""));

        var failure = new PollerFailure(
                "loc-123",
                1L,
                "request-10",
                Instant.parse("2026-11-10T12:10:00Z"),
                "TIMEOUT",
                "Source timed out",
                60
        );

        String failureJson = LocationEventSerializer.toJson(failure);
        assertTrue(failureJson.contains("\"errorCategory\":\"TIMEOUT\""));

        var deleteEvent = new LocationDelete(
                "loc-123",
                1L,
                Instant.parse("2026-11-10T12:12:00Z"),
                "user-requested"
        );
        String deleteJson = LocationEventSerializer.toJson(deleteEvent);
        assertTrue(deleteJson.contains("\"reason\":\"user-requested\""));

        Map<String, Object> map = LocationEventSerializer.readAsMap(snapshotJson);
        assertNotNull(map.get("locationId"));
        assertEquals("loc-123", map.get("locationId"));
    }
}
