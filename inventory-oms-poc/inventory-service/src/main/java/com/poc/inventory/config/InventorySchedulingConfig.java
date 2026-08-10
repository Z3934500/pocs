package com.poc.inventory.config;

import org.springframework.context.annotation.*;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.retry.annotation.EnableRetry;

/** Bounded scheduler for expiry scanning and the local Outbox relay. */
@Configuration
@EnableScheduling
@EnableRetry
public class InventorySchedulingConfig {
    @Bean(name = "taskScheduler")
    ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2); scheduler.setThreadNamePrefix("inventory-scheduler-");
        scheduler.setWaitForTasksToCompleteOnShutdown(true); scheduler.setAwaitTerminationSeconds(15);
        scheduler.setRemoveOnCancelPolicy(true); return scheduler;
    }
}
