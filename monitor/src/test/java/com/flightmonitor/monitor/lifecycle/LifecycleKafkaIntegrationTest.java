package com.flightmonitor.monitor.lifecycle;

import com.flightmonitor.contracts.KafkaTopics;
import com.flightmonitor.contracts.event.*;
import com.flightmonitor.monitor.MonitorApplication;
import com.flightmonitor.monitor.api.LifecycleController;
import com.flightmonitor.monitor.api.LifecycleErrors;
import com.flightmonitor.monitor.api.LocalApiProtection;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.nio.file.Path;
import java.time.*;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Explicitly opt-in; ordinary application tests never launch Kafka.
 * Exercises actual transactions and a fresh read_committed gate replay.
 */
@EnabledIfSystemProperty(named = "lifecycle.kafka.it", matches = "true")
class LifecycleKafkaIntegrationTest {
    @TempDir Path directory;

    @Test
    void committedLifecycleAbortAndFreshGateRecovery() throws Exception {
        var broker = new EmbeddedKafkaKraftBroker(1, 3);
        broker.brokerProperties(Map.of(
                "transaction.state.log.replication.factor", "1",
                "transaction.state.log.min.isr", "1",
                "offsets.topic.replication.factor", "1",
                "group.initial.rebalance.delay.ms", "0"));
        ConfigurableApplicationContext context = null;
        try {
            broker.afterPropertiesSet();
            context = startMonitor(broker, directory);
            var settings = context.getBean(LifecycleSettings.class);
            Clock clock = Clock.systemUTC();
            var gate = context.getBean(CommittedRegistry.class);
            LifecycleRuntime firstRuntime = context.getBean(LifecycleRuntime.class);
            await(firstRuntime::ready, Duration.ofSeconds(45));
            assertThat(context.getBean(HealthEndpoint.class).healthForPath("readiness").getStatus()).isEqualTo(Status.UP);
            var service = context.getBean(LifecycleService.class);
            var mvc = MockMvcBuilders.standaloneSetup(context.getBean(LifecycleController.class))
                    .setControllerAdvice(context.getBean(LifecycleErrors.class))
                    .addFilters(context.getBean(LocalApiProtection.class)).build();
            mvc.perform(post("/api/locations").header("Host", "127.0.0.1:8080")
                    .header("X-Flight-Monitor", "1").header("Idempotency-Key", "key-one")
                    .contentType("application/json").content("{\"latitude\":10,\"longitude\":20}"))
                    .andExpect(status().isCreated()).andExpect(jsonPath("$.state").value("WAITING_FOR_FIRST_POLL"));

            var registration = service.register("key-one", 10, 20);
            assertThat(service.register("key-one", 10, 20)).isEqualTo(registration);
            assertThatThrownBy(() -> service.register("key-one", 11, 20))
                    .isInstanceOfSatisfying(LifecycleFailure.class, e -> assertThat(e.status()).isEqualTo(409));
            assertThat(service.operation(registration.operationId()).httpStatus()).isEqualTo(201);
            mvc.perform(get("/api/operations/" + registration.operationId()).header("Host", "127.0.0.1:8080"))
                    .andExpect(status().isOk()).andExpect(jsonPath("$.httpStatus").value(201));

            // Write a projection behind an open transaction: the serving gate must not see it.
            String id = LifecycleWire.locationId(settings.namespace(), "aborted-key");
            try (KafkaProducer<String, String> transaction = new KafkaProducer<>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.TRANSACTIONAL_ID_CONFIG, "integration-abort-v1"))) {
                transaction.initTransactions();
                transaction.beginTransaction();
                String eventId = LifecycleWire.operationId(id, LifecycleWire.fingerprint(30, 40));
                Instant now = clock.instant();
                var fake = new LifecycleWire.Projection(1, new LocationRegistryRecord(id, 1,
                        LocationLifecycleState.ACTIVE, eventId, now, settings.namespace()),
                        new LocationRegistration(id, LifecycleWire.fingerprint(30, 40), 30, 40, now,
                                1, settings.namespace()), now, now.plus(Duration.ofHours(24)), java.util.List.of());
                transaction.send(new ProducerRecord<>(KafkaTopics.LOCATION_REGISTRY, id,
                        LifecycleWire.json(fake))).get(10, TimeUnit.SECONDS);
                var abortedRegistration = new LocationEventEnvelope(1, LocationEventType.REGISTER,
                        eventId, id, 1, now, fake.activeRegistration());
                transaction.send(new ProducerRecord<>(KafkaTopics.LOCATION_EVENTS, id,
                        LifecycleWire.json(abortedRegistration))).get(10, TimeUnit.SECONDS);
                Thread.sleep(500);
                assertThat(gate.location(id)).isEmpty();
                transaction.abortTransaction();
            }
            mvc.perform(delete("/api/locations/" + registration.locationId()).header("Host", "127.0.0.1:8080")
                    .header("X-Flight-Monitor", "1")).andExpect(status().isAccepted())
                    .andExpect(jsonPath("$.cleanupPending").value(true));
            assertThatThrownBy(() -> service.requireActive(registration.locationId()))
                    .isInstanceOfSatisfying(LifecycleFailure.class, e -> assertThat(e.status()).isEqualTo(410));
            context.close();
            context = null;

            // Use an empty state directory: recover from changelogs, not the previous RocksDB files.
            context = startMonitor(broker, directory.resolve("cold-recovery"));
            var recoveredGate = context.getBean(CommittedRegistry.class);
            LifecycleRuntime recoveredRuntime = context.getBean(LifecycleRuntime.class);
            await(recoveredRuntime::ready, Duration.ofSeconds(45));
            var recoveredService = context.getBean(LifecycleService.class);
            assertThat(recoveredService.operation(registration.operationId()).httpStatus()).isEqualTo(201);
            assertThat(recoveredGate.location(id)).isEmpty();
            assertThatThrownBy(() -> recoveredService.register("key-one", 10, 20))
                    .isInstanceOfSatisfying(LifecycleFailure.class, e -> assertThat(e.status()).isEqualTo(410));
            assertThat(recoveredService.delete(registration.locationId()).cleanupPending()).isTrue();
            assertThat(recoveredGate.location(registration.locationId()).orElseThrow().activeRegistration()).isNull();
            // Bypass the HTTP gate to prove the restored processor itself rejects old-ID reuse.
            String lateFingerprint = LifecycleWire.fingerprint(31, 41);
            String lateEventId = LifecycleWire.operationId(registration.locationId(), lateFingerprint);
            Instant lateTime = clock.instant();
            var lateRegistration = new LocationEventEnvelope(1, LocationEventType.REGISTER, lateEventId,
                    registration.locationId(), 1, lateTime, new LocationRegistration(
                    registration.locationId(), lateFingerprint, 31, 41, lateTime, 1, settings.namespace()));
            recoveredRuntime.publish(lateRegistration).get(10, TimeUnit.SECONDS);
            await(() -> recoveredGate.operation(lateEventId).isPresent(), Duration.ofSeconds(10));
            assertThat(recoveredGate.operation(lateEventId).orElseThrow().httpStatus()).isEqualTo(410);
            assertThat(recoveredGate.location(registration.locationId()).orElseThrow().registry().state())
                    .isEqualTo(LocationLifecycleState.DELETED);
        } finally {
            if (context != null) {
                context.close();
            }
            broker.destroy();
        }
    }

    private static ConfigurableApplicationContext startMonitor(EmbeddedKafkaKraftBroker broker, Path data) {
        return new SpringApplicationBuilder(MonitorApplication.class).profiles("local")
                .web(WebApplicationType.NONE).run(
                        "--spring.kafka.bootstrap-servers=" + broker.getBrokersAsString(),
                        "--monitor.data-directory=" + data,
                        "--monitor.lifecycle.namespace=integration-local",
                        "--monitor.lifecycle.application-id=integration-lifecycle-v1",
                        "--monitor.lifecycle.max-active=2",
                        "--monitor.lifecycle.max-lifetime=4",
                        "--monitor.lifecycle.max-operations-per-location=8");
    }

    private static void await(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(100);
        }
        assertThat(condition.getAsBoolean()).as("lifecycle readiness before deadline").isTrue();
    }
}
