package com.flightmonitor.monitor.kafka;

import com.flightmonitor.contracts.KafkaTopics;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaTopicPolicyTest {

    @Test
    void defines_the_approved_topics_and_retention_floor() {
        List<KafkaTopicPolicy> policies = KafkaTopicPolicy.approvedTopics();

        assertThat(policies).extracting(KafkaTopicPolicy::name)
                .containsExactly(
                        KafkaTopics.LOCATION_EVENTS,
                        KafkaTopics.LOCATION_REGISTRY,
                        KafkaTopics.POLLER_CHECKPOINT
                );
        assertThat(policies).allSatisfy(policy -> {
            assertThat(policy.partitions()).isEqualTo(3);
            assertThat(policy.replicationFactor()).isEqualTo((short) 1);
        });
        assertThat(policies.get(0).cleanupPolicy()).isEqualTo("delete");
        assertThat(policies.get(0).minimumRetentionMs()).isEqualTo(13L * 60 * 60 * 1_000);
        assertThat(policies.get(1).cleanupPolicy()).isEqualTo("compact");
        assertThat(policies.get(2).cleanupPolicy()).isEqualTo("compact");
    }

    @Test
    void creates_topics_with_the_approved_partition_and_config_values() {
        var eventTopic = KafkaTopicPolicy.approvedTopics().get(0).newTopic();
        var registryTopic = KafkaTopicPolicy.approvedTopics().get(1).newTopic();

        assertThat(eventTopic.numPartitions()).isEqualTo(3);
        assertThat(eventTopic.replicationFactor()).isEqualTo((short) 1);
        assertThat(eventTopic.configs())
                .containsEntry("cleanup.policy", "delete")
                .containsEntry("retention.ms", "46800000");
        assertThat(registryTopic.configs()).containsEntry("cleanup.policy", "compact");
    }

    @Test
    void accepts_matching_metadata_and_a_longer_event_retention() {
        KafkaTopicPolicy events = KafkaTopicPolicy.approvedTopics().get(0);
        TopicMetadata actual = new TopicMetadata(
                3,
                List.of(1, 1, 1),
                Map.of("cleanup.policy", "delete", "retention.ms", Long.toString(14L * 60 * 60 * 1_000))
        );

        assertThat(events.mismatches(actual)).isEmpty();
    }

    @Test
    void accepts_unlimited_event_retention_as_exceeding_the_minimum() {
        KafkaTopicPolicy events = KafkaTopicPolicy.approvedTopics().get(0);
        TopicMetadata actual = new TopicMetadata(
                3,
                List.of(1, 1, 1),
                Map.of("cleanup.policy", "delete", "retention.ms", "-1")
        );

        assertThat(events.mismatches(actual)).isEmpty();
    }

    @Test
    void reports_partition_replication_and_policy_mismatches() {
        KafkaTopicPolicy events = KafkaTopicPolicy.approvedTopics().get(0);
        TopicMetadata actual = new TopicMetadata(
                2,
                List.of(2, 1),
                Map.of("cleanup.policy", "compact", "retention.ms", "3600000")
        );

        assertThat(events.mismatches(actual))
                .contains(
                        "partitions expected 3 but found 2",
                        "replication factor expected 1 on every partition but found [2, 1]",
                        "cleanup.policy expected delete but found compact",
                        "retention.ms must be at least 46800000 but found 3600000"
                );
    }

    @Test
    void compacted_topics_do_not_require_a_delete_retention_override() {
        KafkaTopicPolicy registry = KafkaTopicPolicy.approvedTopics().get(1);
        TopicMetadata actual = new TopicMetadata(
                3,
                List.of(1, 1, 1),
                Map.of("cleanup.policy", "compact")
        );

        assertThat(registry.mismatches(actual)).isEmpty();
    }
}
