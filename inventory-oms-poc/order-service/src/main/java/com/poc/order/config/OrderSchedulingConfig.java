package com.poc.order.config;

import org.springframework.context.annotation.*;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/** Bounded scheduler for the Order Outbox relay. */
@Configuration
@EnableScheduling
public class OrderSchedulingConfig {
    @Bean(name="taskScheduler") ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler=new ThreadPoolTaskScheduler(); scheduler.setPoolSize(2); scheduler.setThreadNamePrefix("order-scheduler-"); scheduler.setWaitForTasksToCompleteOnShutdown(true); scheduler.setAwaitTerminationSeconds(15); scheduler.setRemoveOnCancelPolicy(true); return scheduler;
    }
}