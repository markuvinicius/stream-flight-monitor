package com.flightmonitor.monitor.lifecycle;

import com.flightmonitor.contracts.event.*;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.*;

class LifecycleWireTest {
    @Test
    void stableNamespaceIdsAndCanonicalCoordinateFingerprints() {
        assertThat(LifecycleWire.locationId("one", "key")).hasSize(64)
                .isEqualTo(LifecycleWire.locationId("one", "key"))
                .isNotEqualTo(LifecycleWire.locationId("two", "key"));
        assertThat(LifecycleWire.fingerprint(-0.0, 0)).isEqualTo(LifecycleWire.fingerprint(0, -0.0));
        LifecycleWire.coordinates(-90, -180);
        LifecycleWire.coordinates(90, 180);
        for (double lon : new double[]{-180.0001, 180.0001, Double.NaN, Double.NEGATIVE_INFINITY}) {
            assertThatThrownBy(() -> LifecycleWire.coordinates(0, lon)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void explicitlyDispatchesObjectPayloadAndRejectsLegacyContractRangeWeakness() {
        String namespace = "flight-local-v1";
        String id = LifecycleWire.locationId(namespace, "key");
        String fingerprint = LifecycleWire.fingerprint(10, 20);
        var registration = new LocationRegistration(id, fingerprint, 10, 20, Instant.EPOCH, 1, namespace);
        var envelope = new LocationEventEnvelope(1, LocationEventType.REGISTER,
                LifecycleWire.operationId(id, fingerprint), id, 1, Instant.EPOCH, registration);
        var roundTrip = LifecycleWire.read(LifecycleWire.json(envelope), LocationEventEnvelope.class);
        assertThat(LifecycleWire.payload(roundTrip, LocationRegistration.class)).isEqualTo(registration);
        LifecycleWire.validate(id, roundTrip, namespace);
        var outOfRange = new LocationRegistration(id, fingerprint, 91, 20, Instant.EPOCH, 1, namespace);
        var invalid = new LocationEventEnvelope(1, LocationEventType.REGISTER, envelope.eventId(),
                id, 1, Instant.EPOCH, outOfRange);
        assertThatThrownBy(() -> LifecycleWire.validate(id, invalid, namespace)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LifecycleWire.validate("wrong-key", envelope, namespace)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> LifecycleWire.validate(id, envelope, "other")).isInstanceOf(IllegalArgumentException.class);
    }
}
