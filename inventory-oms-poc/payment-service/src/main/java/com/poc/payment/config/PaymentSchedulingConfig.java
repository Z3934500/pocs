package com.poc.payment.config;

import org.springframework.context.annotation.*;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/** Bounded scheduler for the Payment Outbox relay. */
@Configuration
@EnableScheduling
public class PaymentSchedulingConfig {
    @Bean(name = "taskScheduler")
    ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler(); scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("payment-scheduler-"); scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(15); scheduler.setRemoveOnCancelPolicy(true); return scheduler;
    }
}