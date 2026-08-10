package com.poc.order.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.contracts.InventoryReservationResult;
import com.poc.order.service.OrderWorkflowService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

/** Resumes the Order Saga after Inventory Service applies a queued command. */
@Component
@ConditionalOnProperty(name = "inventory.reservation.mode", havingValue = "sqs-fifo")
public class SqsInventoryReservationResultConsumer {
    private static final Logger log = LoggerFactory.getLogger(
            SqsInventoryReservationResultConsumer.class);

    private final SqsAsyncClient sqsClient;
    private final ObjectMapper objectMapper;
    private final OrderWorkflowService workflowService;
    private final MeterRegistry meterRegistry;
    private final String resultQueueUrl;
    private final int batchSize;
    private final int waitSeconds;

    public SqsInventoryReservationResultConsumer(
            SqsAsyncClient sqsClient,
            ObjectMapper objectMapper,
            OrderWorkflowService workflowService,
            MeterRegistry meterRegistry,
            Environment environment) {
        this.sqsClient = sqsClient;
        this.objectMapper = objectMapper;
        this.workflowService = workflowService;
        this.meterRegistry = meterRegistry;
        this.resultQueueUrl = environment.getRequiredProperty(
                "inventory.reservation.result-queue-url");
        this.batchSize = Math.max(1, Math.min(environment.getProperty(
                "inventory.reservation.result-consumer-batch-size", Integer.class, 10), 10));
        this.waitSeconds = Math.max(0, Math.min(environment.getProperty(
                "inventory.reservation.result-consumer-wait-seconds", Integer.class, 10), 20));
    }

    @Scheduled(fixedDelayString = "${inventory.reservation.result-consumer-poll-ms:250}")
    public void consume() {
        try {
            List<Message> messages = sqsClient.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(resultQueueUrl)
                    .maxNumberOfMessages(batchSize)
                    .waitTimeSeconds(waitSeconds)
                    .build()).get().messages();
            for (Message message : messages) {
                process(message);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException exception) {
            log.warn("failed to receive inventory reservation results", exception.getCause());
            increment("receive_failed");
        }
    }

    private void process(Message message) {
        try {
            InventoryReservationResult result = objectMapper.readValue(
                    message.body(), InventoryReservationResult.class);
            workflowService.onReservationResult(result);
            delete(message);
            increment("processed");
        } catch (JsonProcessingException exception) {
            // Keep malformed messages visible to the configured SQS DLQ.
            increment("invalid");
            log.error("invalid inventory reservation result message={}",
                    message.messageId(), exception);
        } catch (RuntimeException exception) {
            // Do not delete when payment, commit or compensation needs retry.
            increment("retry");
            log.warn("inventory reservation result will be retried message={}",
                    message.messageId(), exception);
        }
    }

    private void delete(Message message) {
        try {
            sqsClient.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(resultQueueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build()).get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while deleting inventory result", exception);
        } catch (ExecutionException exception) {
            throw new IllegalStateException("failed to delete inventory result", exception);
        }
    }

    private void increment(String outcome) {
        Counter.builder("oms_inventory_reservation_result_total")
                .tag("service", "order")
                .tag("outcome", outcome)
                .register(meterRegistry)
                .increment();
    }
}
