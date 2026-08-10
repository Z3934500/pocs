package com.poc.inventory.messaging;

import com.poc.contracts.DomainEvent;

/** Transport port; production can implement this with KafkaTemplate or SqsAsyncClient. */
public interface DomainEventPublisher { void publish(DomainEvent event); }