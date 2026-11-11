package com.flightmonitor.monitor.lifecycle;

import com.flightmonitor.contracts.KafkaTopics;
import com.flightmonitor.contracts.event.*;
import com.flightmonitor.contracts.event.LifecycleWire.Projection;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.*;
import java.util.Properties;

import static org.assertj.core.api.Assertions.*;

class LifecycleTopologyTest {
    @TempDir Path directory;
    private TopologyTestDriver driver;
    private TestInputTopic<String, String> input;
    private TestOutputTopic<String, String> output;
    private final Instant now = Instant.parse("2026-11-11T12:00:00Z");
    private final LifecycleSettings settings = new LifecycleSettings(false, null, null, null, 1, 3, 3);

    @BeforeEach
    void start() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "lifecycle-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        properties.put(StreamsConfig.STATE_DIR_CONFIG, directory.toString());
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        driver = new TopologyTestDriver(LifecycleTopology.build(settings), properties, now);
        input = driver.createInputTopic(KafkaTopics.LOCATION_EVENTS, new StringSerializer(), new StringSerializer());
        output = driver.createOutputTopic(KafkaTopics.LOCATION_REGISTRY, new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void stop() { driver.close(); }

    LocationEventEnvelope registration(String key, double lat, double lon) {
        String id = LifecycleWire.locationId(settings.namespace(), key);
        String fingerprint = LifecycleWire.fingerprint(lat, lon);
        return new LocationEventEnvelope(1, LocationEventType.REGISTER,
                LifecycleWire.operationId(id, fingerprint), id, 1, now,
                new LocationRegistration(id, fingerprint, lat, lon, now, 1, settings.namespace()));
    }

    Projection send(LocationEventEnvelope event) {
        input.pipeInput(event.locationId(), LifecycleWire.json(event));
        return LifecycleWire.read(output.readValue(), Projection.class);
    }

    LocationEventEnvelope deletion(String id) {
        return new LocationEventEnvelope(1, LocationEventType.DELETE,
                LifecycleWire.operationId(id, "DELETE"), id, 1, now,
                new LocationDelete(id, 1, now, "user-requested"));
    }

    @Test
    void registrationRetryConflictDeletionAndLateReplayNeverResurrect() {
        var event = registration("key", 12.345, -45.678);
        Projection first = send(event);
        assertThat(first.registry().state()).isEqualTo(LocationLifecycleState.ACTIVE);
        assertThat(first.activeRegistration().latitude()).isEqualTo(12.345);
        assertThat(first.operations().getFirst().httpStatus()).isEqualTo(201);
        assertThat(send(event)).isEqualTo(first);
        Projection conflict = send(registration("key", 90, 180));
        assertThat(conflict.operations().getLast().code()).isEqualTo("IDEMPOTENCY_CONFLICT");
        assertThat(conflict.activeRegistration()).isEqualTo(first.activeRegistration());
        Projection deleted = send(deletion(event.locationId()));
        assertThat(deleted.registry().state()).isEqualTo(LocationLifecycleState.DELETED);
        assertThat(deleted.activeRegistration()).isNull();
        assertThat(LifecycleWire.json(deleted)).doesNotContain("12.345", "-45.678", first.activeRegistration().locationKey());
        assertThat(send(event).registry().state()).isEqualTo(LocationLifecycleState.DELETED);
        assertThat(send(deletion(event.locationId())).registry().state()).isEqualTo(LocationLifecycleState.DELETED);
    }

    @Test
    void activeLimitIsAuthoritativeAndDeletionFreesOnlyActiveCapacity() {
        var one = registration("one", -90, -180);
        send(one);
        assertThat(send(registration("two", 90, 180)).operations().getFirst().code()).isEqualTo("LOCATION_CAPACITY");
        send(deletion(one.locationId()));
        // A rejected operation is idempotent for its own retention interval.
        assertThat(send(registration("two", 90, 180)).registry()).isNull();
        assertThat(send(registration("two", 80, 180)).operations().getLast().code()).isEqualTo("IDEMPOTENCY_CONFLICT");
        assertThat(send(registration("three", 0, 0)).registry().state()).isEqualTo(LocationLifecycleState.ACTIVE);
    }

    @Test
    void wallClockExpiryRetainsTerminalIdentityButRemovesOperationResults() {
        var event = registration("key", 10, 20);
        send(event);
        send(deletion(event.locationId()));
        driver.advanceWallClockTime(Duration.ofHours(24));
        output.readValuesToList();
        var store = driver.<String, String>getKeyValueStore(LifecycleTopology.REGISTRY);
        Projection recovered = LifecycleWire.read(store.get(event.locationId()), Projection.class);
        assertThat(recovered.operations()).isEmpty();
        assertThat(recovered.activeRegistration()).isNull();
        assertThat(recovered.registry().state()).isEqualTo(LocationLifecycleState.DELETED);
        assertThat(send(event).operations().getFirst().httpStatus()).isEqualTo(410);
    }

    @Test
    void expiredActiveRegistrationKeyCannotBeReused() {
        var event = registration("key", 10, 20);
        send(event);
        driver.advanceWallClockTime(Duration.ofHours(24));
        output.readValuesToList();
        assertThat(send(event).operations().getFirst().code()).isEqualTo("IDEMPOTENCY_KEY_EXPIRED");
    }

    @Test
    void reserveDeletionSlotAndPreserveRetainedResultsOnBudgetRejection() {
        var event = registration("key", 10, 20);
        send(event);
        send(registration("key", 11, 20));
        Projection rejected = send(registration("key", 12, 20));
        assertThat(rejected.admissionRejection().code()).isEqualTo("OPERATION_CAPACITY");
        assertThat(rejected.operations()).hasSize(2);
        assertThat(send(deletion(event.locationId())).operations()).hasSize(3);
    }

    @Test
    void snapshotsAreNotLifecycleMutationsAndDoNotResurrect() {
        var event = registration("key", 10, 20);
        send(event);
        send(deletion(event.locationId()));
        input.pipeInput(event.locationId(), LifecycleWire.json(new LocationEventEnvelope(1,
                LocationEventType.SNAPSHOT, "poll-request-1", event.locationId(), 1, now, java.util.Map.of())));
        assertThat(output.isEmpty()).isTrue();
    }

    @Test
    void envelopePayloadMismatchFailsClosed() {
        var valid = registration("key", 10, 20);
        var malformed = new LocationEventEnvelope(1, LocationEventType.REGISTER, valid.eventId(),
                valid.locationId(), 1, now, new LocationDelete(valid.locationId(), 1, now, "wrong-type"));
        assertThatThrownBy(() -> send(malformed)).isInstanceOf(org.apache.kafka.streams.errors.StreamsException.class);
    }

    @Test
    void lifetimeCapacityRetainsEveryDeletedIdentityAndRejectsNewOnes() {
        for (String key : new String[]{"one", "two", "three"}) {
            var event = registration(key, 0, 0);
            send(event);
            send(deletion(event.locationId()));
        }
        Projection overflow = send(registration("four", 0, 0));
        assertThat(overflow.registry()).isNull();
        assertThat(overflow.admissionRejection().code()).isEqualTo("LOCATION_CAPACITY");
        driver.advanceWallClockTime(Duration.ofHours(24));
        output.readValuesToList();
        Projection replay = send(registration("one", 0, 0));
        assertThat(replay.registry().state()).isEqualTo(LocationLifecycleState.DELETED);
        assertThat(replay.operations().getFirst().httpStatus()).isEqualTo(410);
    }

    @Test
    void singleAdmissionPartitionAndStableStoreNamesAreExplicit() {
        String topology = LifecycleTopology.build(settings).describe().toString();
        assertThat(topology).contains("lifecycle-order-v1-repartition", LifecycleTopology.REGISTRY,
                LifecycleTopology.COUNTERS, LifecycleTopology.EXPIRY);
    }
}
