package com.poc.order.messaging;

import com.poc.contracts.DomainEvent;

/** Transport port; production implementation publishes to Kafka/MSK or SQS. */
public interface DomainEventPublisher { void publish(DomainEvent event); }