package com.flightmonitor.contracts.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/**
 * Task-3 registry wire schema: existing contracts wrapped without altering their constructors.
 * A projection atomically carries lifecycle state and the retained operation results.
 */
public final class LifecycleWire {
    private static final ObjectMapper JSON = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private LifecycleWire() { }

    /** A result is also the acknowledgement of its exact eventId. No coordinates are retained here. */
    public record Operation(String operationId, String eventId, String locationId, int httpStatus,
                            String code, Instant createdAt, Instant expiresAt) {
        public Operation {
            Objects.requireNonNull(createdAt, "createdAt");
            Objects.requireNonNull(expiresAt, "expiresAt");
            if (operationId == null || !operationId.matches("[a-f0-9]{64}") || !operationId.equals(eventId)
                    || locationId == null || !locationId.matches("[a-f0-9]{64}")
                    || httpStatus < 200 || httpStatus > 599 || code == null || !code.matches("[A-Z][A-Z0-9_]{0,63}")
                    || !expiresAt.equals(createdAt.plus(java.time.Duration.ofHours(24)))) {
                throw new IllegalArgumentException("Invalid lifecycle operation result");
            }
        }
    }

    /** Null registry denotes a rejected, non-admitted registration, not an active location. */
    public record Projection(int projectionVersion, LocationRegistryRecord registry,
                             LocationRegistration activeRegistration, Instant createdAt,
                             Instant keyExpiresAt, List<Operation> operations, Operation admissionRejection,
                             String registrationFingerprint) {
        public Projection(int projectionVersion, LocationRegistryRecord registry,
                          LocationRegistration activeRegistration, Instant createdAt,
                          Instant keyExpiresAt, List<Operation> operations) {
            this(projectionVersion, registry, activeRegistration, createdAt, keyExpiresAt, operations, null,
                    activeRegistration == null ? null : activeRegistration.locationKey());
        }

        public Projection(int projectionVersion, LocationRegistryRecord registry,
                          LocationRegistration activeRegistration, Instant createdAt,
                          Instant keyExpiresAt, List<Operation> operations, Operation admissionRejection) {
            this(projectionVersion, registry, activeRegistration, createdAt, keyExpiresAt, operations,
                    admissionRejection, activeRegistration == null ? null : activeRegistration.locationKey());
        }
        public Projection {
            operations = List.copyOf(operations);
            if (projectionVersion != 1 || operations.size() > 64 || (registry == null && activeRegistration != null)
                    || (registry != null
                    && registry.state() == LocationLifecycleState.DELETED
                    && (activeRegistration != null || registrationFingerprint != null))) {
                throw new IllegalArgumentException("Invalid registry projection");
            }
            if (registry != null && registry.state() == LocationLifecycleState.ACTIVE
                    && (activeRegistration == null || createdAt == null || keyExpiresAt == null
                    || !registry.locationId().equals(activeRegistration.locationId())
                    || registry.locationGeneration() != activeRegistration.locationGeneration()
                    || !activeRegistration.locationKey().equals(registrationFingerprint))) {
                throw new IllegalArgumentException("Invalid active registry projection");
            }
        }
    }

    public static String json(Object value) {
        try {
            return JSON.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid lifecycle serialization", e);
        }
    }

    public static <T> T read(String value, Class<T> type) {
        try {
            return JSON.readValue(value, type);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Malformed lifecycle record", e);
        }
    }

    public static <T> T payload(LocationEventEnvelope event, Class<T> type) {
        if (type == LocationRegistration.class) {
            var tree = JSON.valueToTree(event.payload());
            if (tree == null || !tree.has("latitude") || !tree.get("latitude").isNumber()
                    || !tree.has("longitude") || !tree.get("longitude").isNumber()) {
                throw new IllegalArgumentException("Registration requires numeric coordinates");
            }
        }
        return JSON.convertValue(event.payload(), type);
    }

    public static void coordinates(double latitude, double longitude) {
        if (!Double.isFinite(latitude) || !Double.isFinite(longitude)
                || latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180) {
            throw new IllegalArgumentException("Coordinates must be finite and in range");
        }
    }

    public static String fingerprint(double latitude, double longitude) {
        coordinates(latitude, longitude);
        return hash(Double.toHexString(latitude == 0 ? 0 : latitude) + ":"
                + Double.toHexString(longitude == 0 ? 0 : longitude));
    }

    public static String locationId(String namespace, String key) {
        return hash(namespace + "\u0000" + key);
    }

    public static String operationId(String locationId, String fingerprint) {
        return hash(locationId + "\u0000" + fingerprint);
    }

    private static String hash(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JVM lacks SHA-256", e);
        }
    }

    /** Fail closed on malformed envelopes; no partition-skipping policy is approved. */
    public static void validate(String key, LocationEventEnvelope event, String namespace) {
        if (event.schemaVersion() != 1 || !key.equals(event.locationId())
                || !key.matches("[a-f0-9]{64}") || event.eventId().isBlank()
                || event.eventId().length() > 128 || event.locationGeneration() < 1) {
            throw new IllegalArgumentException("Invalid lifecycle envelope");
        }
        if ((event.eventType() == LocationEventType.REGISTER || event.eventType() == LocationEventType.DELETE)
                && (event.locationGeneration() != 1 || !event.eventId().matches("[a-f0-9]{64}"))) {
            throw new IllegalArgumentException("Invalid lifecycle identity");
        }
        if (event.eventType() == LocationEventType.REGISTER) {
            var registration = payload(event, LocationRegistration.class);
            coordinates(registration.latitude(), registration.longitude());
            String fingerprint = fingerprint(registration.latitude(), registration.longitude());
            if (!key.equals(registration.locationId()) || registration.locationGeneration() != 1
                    || !namespace.equals(registration.sourceClientId())
                    || !fingerprint.equals(registration.locationKey())
                    || !event.eventId().equals(operationId(key, fingerprint))) {
                throw new IllegalArgumentException("Invalid registration payload");
            }
        } else if (event.eventType() == LocationEventType.DELETE) {
            var deletion = payload(event, LocationDelete.class);
            if (!key.equals(deletion.locationId()) || deletion.locationGeneration() != 1
                    || !event.eventId().equals(operationId(key, "DELETE"))) {
                throw new IllegalArgumentException("Invalid deletion payload");
            }
        }
    }
}
