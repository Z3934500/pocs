package com.poc.inventory.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.contracts.InventoryReservationQueueCommand;
import com.poc.contracts.InventoryReservationResult;
import com.poc.contracts.ReserveInventoryCommand;
import com.poc.inventory.service.InventoryService;
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
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/**
 * Applies strict-order inventory commands one message at a time per SQS
 * message group. Database idempotency remains authoritative after redelivery.
 */
@Component
@ConditionalOnProperty(name = "inventory.reservation.mode", havingValue = "sqs-fifo")
public class SqsInventoryReservationCommandConsumer {
    private static final Logger log = LoggerFactory.getLogger(
            SqsInventoryReservationCommandConsumer.class);

    private final SqsAsyncClient sqsClient;
    private final ObjectMapper objectMapper;
    private final InventoryService inventoryService;
    private final MeterRegistry meterRegistry;
    private final String commandQueueUrl;
    private final String resultQueueUrl;
    private final boolean resultFifo;
    private final int batchSize;
    private final int waitSeconds;

    public SqsInventoryReservationCommandConsumer(
            SqsAsyncClient sqsClient,
            ObjectMapper objectMapper,
            InventoryService inventoryService,
            MeterRegistry meterRegistry,
            Environment environment) {
        this.sqsClient = sqsClient;
        this.objectMapper = objectMapper;
        this.inventoryService = inventoryService;
        this.meterRegistry = meterRegistry;
        this.commandQueueUrl = environment.getRequiredProperty(
                "inventory.reservation.command-queue-url");
        this.resultQueueUrl = environment.getRequiredProperty(
                "inventory.reservation.result-queue-url");
        this.resultFifo = environment.getProperty(
                "inventory.reservation.result-fifo", Boolean.class, true);
        this.batchSize = Math.max(1, Math.min(environment.getProperty(
                "inventory.reservation.consumer-batch-size", Integer.class, 10), 10));
        this.waitSeconds = Math.max(0, Math.min(environment.getProperty(
                "inventory.reservation.consumer-wait-seconds", Integer.class, 10), 20));
    }

    @Scheduled(fixedDelayString = "${inventory.reservation.consumer-poll-ms:250}")
    public void consume() {
        try {
            List<Message> messages = sqsClient.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(commandQueueUrl)
                    .maxNumberOfMessages(batchSize)
                    .waitTimeSeconds(waitSeconds)
                    .build()).get().messages();
            for (Message message : messages) {
                process(message);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException exception) {
            log.warn("failed to receive inventory reservation commands", exception.getCause());
            increment("receive_failed");
        }
    }

    private void process(Message message) {
        InventoryReservationQueueCommand command;
        try {
            command = objectMapper.readValue(
                    message.body(), InventoryReservationQueueCommand.class);
        } catch (JsonProcessingException exception) {
            // Let the queue redrive policy/DLQ retain malformed messages.
            log.error("invalid inventory reservation command message={}", message.messageId(), exception);
            increment("invalid");
            return;
        }

        try {
            if (command.action() == InventoryReservationQueueCommand.Action.CANCEL) {
                inventoryService.cancelIfPresent(command.orderId());
                delete(message);
                increment("cancelled");
                return;
            }

            ReserveInventoryCommand reserveCommand = command.reserveCommand();
            if (reserveCommand == null) {
                throw new IllegalArgumentException("reserve command is missing");
            }

            InventoryReservationResult result;
            try {
                result = InventoryReservationResult.reserved(
                        inventoryService.reserve(reserveCommand),
                        reserveCommand.idempotencyKey());
            } catch (IllegalArgumentException | IllegalStateException businessFailure) {
                result = InventoryReservationResult.failed(
                        reserveCommand.orderId(),
                        reserveCommand.sku(),
                        reserveCommand.qty(),
                        businessFailure.getMessage(),
                        reserveCommand.idempotencyKey());
            }
            publishResult(result);
            delete(message);
            increment(result.status() == null ? "rejected" : "reserved");
        } catch (RuntimeException exception) {
            // Do not delete transient failures; SQS visibility timeout and DLQ
            // policy provide retry and poison-message isolation.
            increment("retry");
            log.warn("inventory reservation command will be retried message={}",
                    message.messageId(), exception);
        }
    }

    private void publishResult(InventoryReservationResult result) {
        try {
            SendMessageRequest.Builder request = SendMessageRequest.builder()
                    .queueUrl(resultQueueUrl)
                    .messageBody(objectMapper.writeValueAsString(result));
            if (resultFifo) {
                request.messageGroupId("order:" + result.orderId())
                        .messageDeduplicationId("inventory-result:" + result.idempotencyKey());
            }
            sqsClient.sendMessage(request.build()).get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while publishing inventory result", exception);
        } catch (ExecutionException | JsonProcessingException exception) {
            throw new IllegalStateException("failed to publish inventory result", exception);
        }
    }

    private void delete(Message message) {
        try {
            sqsClient.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(commandQueueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build()).get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while deleting inventory command", exception);
        } catch (ExecutionException exception) {
            throw new IllegalStateException("failed to delete inventory command", exception);
        }
    }

    private void increment(String outcome) {
        Counter.builder("oms_inventory_reservation_queue_total")
                .tag("service", "inventory")
                .tag("outcome", outcome)
                .register(meterRegistry)
                .increment();
    }
}
