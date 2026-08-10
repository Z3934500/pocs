package com.poc.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

// [kb-land] Source: RATE_LIMIT_LOAD_TEST_PLAN.md
// ConfigurationPropertiesScan picks up RateLimitProperties (and any future @ConfigurationProperties).
/** Order bounded context and Saga orchestration entry point. */
@SpringBootApplication
@ConfigurationPropertiesScan
public class OrderServiceApplication {
    public static void main(String[] args) { SpringApplication.run(OrderServiceApplication.class, args); }
}