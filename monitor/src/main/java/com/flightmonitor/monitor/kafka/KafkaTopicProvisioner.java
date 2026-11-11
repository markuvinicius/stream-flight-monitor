package com.flightmonitor.monitor.kafka;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.springframework.core.annotation.Order;

/**
 * Creates and validates the application's Kafka topics before local readiness is reported.
 */
@Component
@Order(0)
@Profile("local")
@ConditionalOnProperty(prefix = "monitor.kafka", name = "topic-bootstrap-enabled", havingValue = "true")
final class KafkaTopicProvisioner implements ApplicationRunner {
    private static final Duration STARTUP_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration ADMIN_CLOSE_TIMEOUT = Duration.ofSeconds(5);
    private static final int REQUEST_TIMEOUT_MS = 5_000;
    private static final int DEFAULT_API_TIMEOUT_MS = 10_000;

    private final String bootstrapServers;

    KafkaTopicProvisioner(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    @Override
    public void run(ApplicationArguments args) {
        provisionTopics();
    }

    private void provisionTopics() {
        Map<String, Object> adminConfiguration = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, DEFAULT_API_TIMEOUT_MS
        );
        long deadlineNanos = System.nanoTime() + STARTUP_TIMEOUT.toNanos();
        Admin admin = Admin.create(adminConfiguration);
        try {
            List<KafkaTopicPolicy> policies = KafkaTopicPolicy.approvedTopics();
            Set<String> existingNames = await(
                    admin.listTopics().names(), deadlineNanos, "listing Kafka topics");

            for (KafkaTopicPolicy policy : policies) {
                if (!existingNames.contains(policy.name())) {
                    createIfMissing(admin, policy.newTopic(), deadlineNanos);
                }
            }

            Map<String, org.apache.kafka.clients.admin.TopicDescription> descriptions = awaitMetadata(
                    () -> admin.describeTopics(policies.stream().map(KafkaTopicPolicy::name).toList()).allTopicNames(),
                    deadlineNanos,
                    "describing Kafka topics");
            Map<ConfigResource, Config> describedConfigs = awaitMetadata(
                    () -> admin.describeConfigs(topicResources(policies)).all(),
                    deadlineNanos,
                    "describing Kafka topic policies");

            for (KafkaTopicPolicy policy : policies) {
                var description = descriptions.get(policy.name());
                ConfigResource resource = new ConfigResource(Type.TOPIC, policy.name());
                Config config = describedConfigs.get(resource);
                if (description == null || config == null) {
                    throw new IllegalStateException(
                            "Kafka topic " + policy.name() + " disappeared during policy validation");
                }
                TopicMetadata actual = metadata(description.partitions(), config);
                List<String> mismatches = policy.mismatches(actual);
                if (!mismatches.isEmpty()) {
                    throw new IllegalStateException(
                            "Kafka topic " + policy.name() + " does not match the approved policy: "
                                    + String.join("; ", mismatches));
                }
            }
        } finally {
            admin.close(ADMIN_CLOSE_TIMEOUT);
        }
    }

    private static void createIfMissing(Admin admin, NewTopic topic, long deadlineNanos) {
        try {
            await(admin.createTopics(List.of(topic)).all(), deadlineNanos, "creating Kafka topic " + topic.name());
        } catch (RuntimeException exception) {
            if (!hasCause(exception, TopicExistsException.class)) {
                throw exception;
            }

            // Another monitor instance may have created it after the initial topic listing.
        }
    }

    /** Topic creation can precede broker-side KRaft metadata/config visibility. */
    static <T> T awaitMetadata(Supplier<? extends Future<T>> request, long deadlineNanos, String operation) {
        while (true) {
            try {
                return await(request.get(), deadlineNanos, operation);
            } catch (UnknownTopicOrPartitionException exception) {
                if (System.nanoTime() >= deadlineNanos) {
                    throw new IllegalStateException("Topic metadata did not become visible while " + operation, exception);
                }
                try {
                    TimeUnit.NANOSECONDS.sleep(Math.min(TimeUnit.MILLISECONDS.toNanos(100),
                            Math.max(1, deadlineNanos - System.nanoTime())));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted while " + operation, interrupted);
                }
            }
        }
    }

    private static Collection<ConfigResource> topicResources(List<KafkaTopicPolicy> policies) {
        return policies.stream()
                .map(policy -> new ConfigResource(Type.TOPIC, policy.name()))
                .toList();
    }

    private static TopicMetadata metadata(
            List<TopicPartitionInfo> partitions,
            Config config
    ) {
        List<Integer> replicationFactors = partitions.stream()
                .map(partition -> partition.replicas().size())
                .toList();
        Map<String, String> values = new HashMap<>();
        for (ConfigEntry entry : config.entries()) {
            if (entry.value() != null
                    && ("cleanup.policy".equals(entry.name()) || "retention.ms".equals(entry.name()))) {
                values.put(entry.name(), entry.value());
            }
        }
        return new TopicMetadata(partitions.size(), replicationFactors, Map.copyOf(values));
    }

    private static <T> T await(Future<T> future, long deadlineNanos, String operation) {
        long remainingNanos = deadlineNanos - System.nanoTime();
        if (remainingNanos <= 0) {
            throw new IllegalStateException("Timed out after " + STARTUP_TIMEOUT + " while " + operation);
        }
        try {
            return future.get(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while " + operation, exception);
        } catch (ExecutionException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IllegalStateException("Failed while " + operation, cause);
        } catch (TimeoutException exception) {
            throw new IllegalStateException("Timed out after " + STARTUP_TIMEOUT + " while " + operation, exception);
        }
    }

    private static boolean hasCause(Throwable exception, Class<? extends Throwable> expectedType) {
        for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
            if (expectedType.isInstance(cause)) {
                return true;
            }
        }
        return false;
    }
}
