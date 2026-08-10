package com.poc.reservation.config;

import com.poc.reservation.service.ReservationService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class ReservationTimeoutScheduler {

    private static final Logger log = LoggerFactory.getLogger(ReservationTimeoutScheduler.class);
    private final ReservationService service;
    private final MeterRegistry meterRegistry;
    private final Counter expiredCounter;
    private final Timer expirationTimer;

    public ReservationTimeoutScheduler(ReservationService service, MeterRegistry meterRegistry) {
        this.service = service;
        this.meterRegistry = meterRegistry;
        this.expiredCounter = Counter.builder("oms_reservation_expiration_total")
            .description("Reservations released by the bounded expiration fallback")
            .tag("service", "reservation")
            .register(meterRegistry);
        this.expirationTimer = Timer.builder("oms_reservation_expiration_duration")
            .description("Duration of one bounded reservation expiration scan")
            .tag("service", "reservation")
            .register(meterRegistry);
    }

    @Scheduled(
        initialDelayString = "${oms.reservation.expiration-scan-ms:60000}",
        fixedDelayString = "${oms.reservation.expiration-scan-ms:60000}"
    )
    public void releaseExpiredReservations() {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            int count = service.expireReservations();
            if (count > 0) {
                expiredCounter.increment(count);
                log.info("Released {} expired inventory reservations", count);
            }
        } finally {
            sample.stop(expirationTimer);
        }
    }
}
