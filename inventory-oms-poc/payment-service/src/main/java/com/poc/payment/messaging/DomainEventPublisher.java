package com.poc.payment.messaging;

import com.poc.contracts.DomainEvent;

/** Transport port; production implementation targets Kafka/MSK or SQS. */
public interface DomainEventPublisher { void publish(DomainEvent event); }