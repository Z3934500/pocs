package com.poc.inventory.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.poc.contracts.DomainEvent;
import java.util.concurrent.ExecutionException;
import org.springframework.core.env.Environment;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "messaging.transport", havingValue = "kafka")
public class KafkaDomainEventPublisher implements DomainEventPublisher {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public KafkaDomainEventPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            Environment environment) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = environment.getRequiredProperty("messaging.topic");
    }

    @Override
    public void publish(DomainEvent event) {
        try {
            kafkaTemplate.send(topic, event.kafkaKey(), objectMapper.writeValueAsString(event)).get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while publishing Kafka event " + event.eventId(), exception);
        } catch (ExecutionException | JsonProcessingException exception) {
            throw new IllegalStateException("Kafka publish failed for event " + event.eventId(), exception);
        }
    }
}
