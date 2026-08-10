package com.poc.inventory.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * Polls native SQS queue attributes and exposes them through Micrometer.
 *
 * Queue URLs are configuration values, not metric labels. The stable queue role
 * label lets Prometheus, HPA or KEDA select the right business queue.
 */
@Component
@ConditionalOnProperty(name = "messaging.transport", havingValue = "sqs")
public class SqsQueueMetrics {
    private final SqsAsyncClient sqsClient;
    private final MeterRegistry meterRegistry;
    private final Map<String, QueueState> states = new LinkedHashMap<>();
    private final Counter refreshFailures;

    public SqsQueueMetrics(
            SqsAsyncClient sqsClient,
            MeterRegistry meterRegistry,
            Environment environment) {
        this.sqsClient = sqsClient;
        this.meterRegistry = meterRegistry;
        this.refreshFailures = Counter.builder("oms_sqs_queue_metrics_refresh_failures_total")
                .description("Number of failed SQS queue metric refreshes")
                .tag("service", environment.getProperty("spring.application.name", "oms-service"))
                .register(meterRegistry);

        for (QueueSpec queue : configuredQueues(environment)) {
            QueueState state = new QueueState();
            state.setQueue(queue);
            states.put(queue.role(), state);
            registerGauges(queue.role(), state, environment.getProperty(
                    "spring.application.name", "oms-service"));
        }
    }

    @Scheduled(
            initialDelayString = "${management.queue.metrics.initial-delay-ms:5000}",
            fixedDelayString = "${management.queue.metrics.refresh-ms:10000}")
    public void refresh() {
        for (Map.Entry<String, QueueState> entry : states.entrySet()) {
            refresh(entry.getKey(), entry.getValue());
        }
    }

    private void refresh(String role, QueueState state) {
        QueueSpec queue = state.queue();
        try {
            GetQueueAttributesResponse response = sqsClient.getQueueAttributes(
                    GetQueueAttributesRequest.builder()
                            .queueUrl(queue.url())
                            .attributeNames(
                                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
                                    QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED)
                            .build()).get();
            Map<QueueAttributeName, String> attributes = response.attributes();
            state.update(
                    parseLong(attributes.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)),
                    parseLong(attributes.get(
                            QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)),
                    parseLong(attributes.get(
                            QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED)));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            state.markUnavailable();
            refreshFailures.increment();
        } catch (ExecutionException | RuntimeException exception) {
            state.markUnavailable();
            refreshFailures.increment();
        }
    }

    private void registerGauges(String role, QueueState state, String service) {
        Gauge.builder("oms_sqs_queue_visible_messages", state, QueueState::visible)
                .description("Approximate number of visible messages in an SQS queue")
                .tag("service", service)
                .tag("queue", role)
                .register(meterRegistry);
        Gauge.builder("oms_sqs_queue_in_flight_messages", state, QueueState::inFlight)
                .description("Approximate number of in-flight SQS messages")
                .tag("service", service)
                .tag("queue", role)
                .register(meterRegistry);
        Gauge.builder("oms_sqs_queue_delayed_messages", state, QueueState::delayed)
                .description("Approximate number of delayed SQS messages")
                .tag("service", service)
                .tag("queue", role)
                .register(meterRegistry);
        Gauge.builder("oms_sqs_queue_depth", state, QueueState::depth)
                .description("Approximate total SQS queue depth")
                .tag("service", service)
                .tag("queue", role)
                .register(meterRegistry);
        Gauge.builder("oms_sqs_queue_metrics_up", state, QueueState::up)
                .description("Whether the latest SQS queue metric refresh succeeded")
                .tag("service", service)
                .tag("queue", role)
                .register(meterRegistry);
        Gauge.builder("oms_sqs_queue_metrics_last_refresh_age_seconds",
                        state, QueueState::lastRefreshAgeSeconds)
                .description("Age of the latest successful SQS queue metric refresh")
                .tag("service", service)
                .tag("queue", role)
                .register(meterRegistry);
    }

    private static List<QueueSpec> configuredQueues(Environment environment) {
        List<QueueSpec> queues = new ArrayList<>();
        addIfConfigured(queues, "domain-events",
                environment.getProperty("messaging.sqs.queue-url", ""));
        addIfConfigured(queues, "inventory-reservation-command",
                environment.getProperty("inventory.reservation.command-queue-url", ""));
        addIfConfigured(queues, "inventory-reservation-result",
                environment.getProperty("inventory.reservation.result-queue-url", ""));
        return queues;
    }

    private static void addIfConfigured(List<QueueSpec> queues, String role, String url) {
        if (url != null && !url.isBlank()) {
            queues.add(new QueueSpec(role, url));
        }
    }

    private static double parseLong(String value) {
        if (value == null || value.isBlank()) {
            return 0;
        }
        try {
            return Math.max(0, Long.parseLong(value));
        } catch (NumberFormatException exception) {
            return 0;
        }
    }

    private record QueueSpec(String role, String url) {
    }

    private static final class QueueState {
        private volatile double visible;
        private volatile double inFlight;
        private volatile double delayed;
        private volatile boolean up;
        private volatile long updatedAtMillis;
        private QueueSpec queue;

        private void setQueue(QueueSpec queue) {
            this.queue = queue;
        }

        private QueueSpec queue() {
            return queue;
        }

        private void update(double visible, double inFlight, double delayed) {
            this.visible = visible;
            this.inFlight = inFlight;
            this.delayed = delayed;
            this.up = true;
            this.updatedAtMillis = System.currentTimeMillis();
        }

        private void markUnavailable() {
            this.up = false;
        }

        private double visible() {
            return visible;
        }

        private double inFlight() {
            return inFlight;
        }

        private double delayed() {
            return delayed;
        }

        private double depth() {
            return visible + inFlight + delayed;
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
