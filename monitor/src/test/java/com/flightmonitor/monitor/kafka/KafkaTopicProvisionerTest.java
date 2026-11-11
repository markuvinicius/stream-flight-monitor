package com.flightmonitor.monitor.kafka;

import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

class KafkaTopicProvisionerTest {
    @Test
    void retriesOnlyTransientPostCreationMetadataVisibility() {
        AtomicInteger calls = new AtomicInteger();
        String result = KafkaTopicProvisioner.awaitMetadata(() -> calls.incrementAndGet() == 1
                        ? CompletableFuture.failedFuture(new UnknownTopicOrPartitionException("not visible yet"))
                        : CompletableFuture.completedFuture("ready"),
                System.nanoTime() + Duration.ofSeconds(2).toNanos(), "testing metadata");
        assertThat(result).isEqualTo("ready");
        assertThat(calls.get()).isEqualTo(2);
    }

    @Test
    void authorizationErrorsAreNotRetriedOrHidden() {
        AtomicInteger calls = new AtomicInteger();
        assertThatThrownBy(() -> KafkaTopicProvisioner.awaitMetadata(() -> {
            calls.incrementAndGet();
            return CompletableFuture.failedFuture(new TopicAuthorizationException("forbidden"));
        }, System.nanoTime() + Duration.ofSeconds(2).toNanos(), "testing metadata"))
                .isInstanceOf(TopicAuthorizationException.class);
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void deadlineRemainsBounded() {
        assertThatThrownBy(() -> KafkaTopicProvisioner.awaitMetadata(
                () -> CompletableFuture.failedFuture(new UnknownTopicOrPartitionException("not ready")),
                System.nanoTime() + Duration.ofMillis(20).toNanos(), "testing metadata"))
                .isInstanceOf(IllegalStateException.class);
    }
}
