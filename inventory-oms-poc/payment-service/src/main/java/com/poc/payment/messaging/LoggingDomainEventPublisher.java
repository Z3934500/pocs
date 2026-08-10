package com.poc.payment.messaging;

import com.poc.contracts.DomainEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/** Local transport seam. A real adapter must return only after broker acknowledgement. */
@Component
@ConditionalOnProperty(name = "messaging.transport", havingValue = "logging", matchIfMissing = true)
public class LoggingDomainEventPublisher implements DomainEventPublisher {
    private static final Logger log = LoggerFactory.getLogger(LoggingDomainEventPublisher.class);
    @Override public void publish(DomainEvent event) {
        log.info("Published payment event eventId={} type={} aggregateId={} schemaVersion={} partitionKey={}",
                event.eventId(), event.eventType(), event.aggregateId(), event.schemaVersion(), event.partitionKey());
    }
}