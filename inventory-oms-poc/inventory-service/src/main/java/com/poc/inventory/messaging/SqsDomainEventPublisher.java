package com.poc.inventory.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.contracts.DomainEvent;
import java.util.concurrent.ExecutionException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

@Component
@ConditionalOnProperty(name = "messaging.transport", havingValue = "sqs")
public class SqsDomainEventPublisher implements DomainEventPublisher {
    private final SqsAsyncClient sqsClient;
    private final ObjectMapper objectMapper;
    private final String queueUrl;
    private final boolean fifo;

    public SqsDomainEventPublisher(
            SqsAsyncClient sqsClient,
            ObjectMapper objectMapper,
            Environment environment) {
        this.sqsClient = sqsClient;
        this.objectMapper = objectMapper;
        this.queueUrl = environment.getRequiredProperty("messaging.sqs.queue-url");
        this.fifo = environment.getProperty("messaging.sqs.fifo", Boolean.class, false);
    }

    @Override
    public void publish(DomainEvent event) {
        try {
            SendMessageRequest.Builder request = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(objectMapper.writeValueAsString(event));
            if (fifo) {
                request.messageGroupId(event.sqsMessageGroupId())
                        .messageDeduplicationId(event.sqsMessageDeduplicationId());
            }
            sqsClient.sendMessage(request.build()).get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while publishing SQS event " + event.eventId(), exception);
        } catch (ExecutionException | JsonProcessingException exception) {
            throw new IllegalStateException("SQS publish failed for event " + event.eventId(), exception);
        }
    }
}
