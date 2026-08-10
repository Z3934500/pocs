package com.poc.order.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Optional Kafka consumer-group lag collector. The POC currently uses Kafka
 * primarily as a producer transport, so the collector is disabled by default.
 */
@Component
@ConditionalOnProperty(name = "management.kafka.lag.enabled", havingValue = "true")
public class KafkaConsumerLagMetrics {
    private final String groupId;
    private final List<String> topics;
    private final AdminClient adminClient;
    private final Map<String, LagState> states = new LinkedHashMap<>();
    private final Counter refreshFailures;

    public KafkaConsumerLagMetrics(MeterRegistry meterRegistry, Environment environment) {
        this.groupId = environment.getProperty("management.kafka.lag.group-id", "");
        this.topics = configuredTopics(environment);
        this.refreshFailures = Counter.builder("oms_kafka_lag_refresh_failures_total")
                .description("Number of failed Kafka consumer lag refreshes")
                .tag("service", environment.getProperty("spring.application.name", "oms-service"))
                .tag("group", groupId.isBlank() ? "unconfigured" : groupId)
                .register(meterRegistry);

        for (String topic : topics) {
            LagState state = new LagState();
            states.put(topic, state);
            Gauge.builder("oms_kafka_consumer_lag", state, LagState::lag)
                    .description("Total committed-offset lag for a Kafka consumer group and topic")
                    .tag("service", environment.getProperty("spring.application.name", "oms-service"))
                    .tag("group", groupId.isBlank() ? "unconfigured" : groupId)
                    .tag("topic", topic)
                    .register(meterRegistry);
            Gauge.builder("oms_kafka_consumer_lag_up", state, LagState::up)
                    .description("Whether the latest Kafka consumer lag refresh succeeded")
                    .tag("service", environment.getProperty("spring.application.name", "oms-service"))
                    .tag("group", groupId.isBlank() ? "unconfigured" : groupId)
                    .tag("topic", topic)
                    .register(meterRegistry);
            Gauge.builder("oms_kafka_consumer_lag_last_refresh_age_seconds",
                            state, LagState::lastRefreshAgeSeconds)
                    .description("Age of the latest successful Kafka lag refresh")
                    .tag("service", environment.getProperty("spring.application.name", "oms-service"))
                    .tag("group", groupId.isBlank() ? "unconfigured" : groupId)
                    .tag("topic", topic)
                    .register(meterRegistry);
        }

        if (groupId.isBlank() || topics.isEmpty()) {
            this.adminClient = null;
        } else {
            Map<String, Object> properties = new HashMap<>();
            properties.put(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                    environment.getProperty(
                            "management.kafka.lag.bootstrap-servers",
                            environment.getProperty(
                                    "spring.kafka.bootstrap-servers", "localhost:9092")));
            this.adminClient = AdminClient.create(properties);
        }
    }

    @Scheduled(
            initialDelayString = "${management.kafka.lag.initial-delay-ms:10000}",
            fixedDelayString = "${management.kafka.lag.refresh-ms:10000}")
    public void refresh() {
        if (adminClient == null) {
            return;
        }
        try {
            Map<TopicPartition, OffsetAndMetadata> committed = adminClient
                    .listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get();
            Map<TopicPartition, OffsetSpec> latestRequests = new HashMap<>();
            for (TopicPartition partition : committed.keySet()) {
                if (topics.contains(partition.topic())) {
                    latestRequests.put(partition, OffsetSpec.latest());
                }
            }
            Map<TopicPartition, Long> latest = new HashMap<>();
            if (!latestRequests.isEmpty()) {
                adminClient.listOffsets(latestRequests).all().get().forEach(
                        (partition, info) -> latest.put(partition, info.offset()));
            }

            Map<String, Long> lagByTopic = new HashMap<>();
            for (String topic : topics) {
                lagByTopic.put(topic, 0L);
            }
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed.entrySet()) {
                TopicPartition partition = entry.getKey();
                Long endOffset = latest.get(partition);
                if (endOffset != null) {
                    long lag = Math.max(0, endOffset - entry.getValue().offset());
                    lagByTopic.merge(partition.topic(), lag, Long::sum);
                }
            }
            for (String topic : topics) {
                states.get(topic).update(lagByTopic.getOrDefault(topic, 0L));
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            markUnavailable();
            refreshFailures.increment();
        } catch (ExecutionException | RuntimeException exception) {
            markUnavailable();
            refreshFailures.increment();
        }
    }

    private void markUnavailable() {
        states.values().forEach(LagState::markUnavailable);
    }

    private static List<String> configuredTopics(Environment environment) {
        String configured = environment.getProperty("management.kafka.lag.topics", "");
        List<String> topics = new ArrayList<>();
        for (String topic : configured.split(",")) {
            if (!topic.isBlank() && !topics.contains(topic.trim())) {
                topics.add(topic.trim());
            }
        }
        return topics;
    }

    @PreDestroy
    public void close() {
        if (adminClient != null) {
            adminClient.close(Duration.ofSeconds(5));
        }
    }

    private static final class LagState {
        private volatile double lag;
        private volatile boolean up;
        private volatile long updatedAtMillis;

        private void update(long lag) {
            this.lag = lag;
            this.up = true;
            this.updatedAtMillis = System.currentTimeMillis();
        }

        private void markUnavailable() {
            this.up = false;
        }

        private double lag() {
            return lag;
        }

        private double up() {
            return up ? 1 : 0;
        }

        private double lastRefreshAgeSeconds() {
            long updated = updatedAtMillis;
            return updated == 0
                    ? 0
                    : Math.max(0, (System.currentTimeMillis() - updated) / 1000.0);
        }
    }
}
