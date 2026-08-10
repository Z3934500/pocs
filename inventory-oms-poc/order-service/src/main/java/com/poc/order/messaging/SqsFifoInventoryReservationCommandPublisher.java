package com.poc.order.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.contracts.InventoryReservationQueueCommand;
import com.poc.order.entity.OrderOutboxEvent;
import java.util.concurrent.ExecutionException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

/** SQS FIFO command publisher used only by the opt-in strict-order path. */
@Component
@ConditionalOnProperty(name = "inventory.reservation.mode", havingValue = "sqs-fifo")
public class SqsFifoInventoryReservationCommandPublisher
        implements InventoryReservationCommandPublisher {
    private final SqsAsyncClient sqsClient;
    private final ObjectMapper objectMapper;
    private final String queueUrl;

    public SqsFifoInventoryReservationCommandPublisher(
            SqsAsyncClient sqsClient,
            ObjectMapper objectMapper,
            Environment environment) {
        this.sqsClient = sqsClient;
        this.objectMapper = objectMapper;
        this.queueUrl = environment.getRequiredProperty(
                "inventory.reservation.command-queue-url");
    }

    @Override
    public void publish(OrderOutboxEvent event) {
        try {
            InventoryReservationQueueCommand command = objectMapper.readValue(
                    event.getPayloadJson(), InventoryReservationQueueCommand.class);
            SendMessageRequest request = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(event.getPayloadJson())
                    .messageGroupId(command.messageGroupId())
                    .messageDeduplicationId(event.getEventId().toString())
                    .build();
            sqsClient.sendMessage(request).get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "interrupted while publishing inventory reservation command", exception);
        } catch (ExecutionException | JsonProcessingException exception) {
            throw new IllegalStateException(
                    "inventory reservation command publish failed", exception);
        }
    }
}
