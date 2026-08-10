package com.poc.inventory.seckill;

import com.poc.contracts.SeckillReserveCommand;
import com.poc.inventory.service.InventoryService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "seckill.redis.enabled", havingValue = "true")
public class RedisSeckillStreamConsumer {
    private static final Logger log = LoggerFactory.getLogger(RedisSeckillStreamConsumer.class);

    private final StringRedisTemplate redisTemplate;
    private final InventoryService inventoryService;
    private final String consumerGroup;
    private final String consumerName;
    private final List<String> skus;
    private final int batchSize;
    private final long blockMs;
    private final int maxDeliveryAttempts;
    private final long dlqTtlSeconds;
    private final long dlqMaxLength;
    private final MeterRegistry meterRegistry;

    public RedisSeckillStreamConsumer(
            StringRedisTemplate redisTemplate,
            InventoryService inventoryService,
            MeterRegistry meterRegistry,
            Environment environment) {
        this.redisTemplate = redisTemplate;
        this.inventoryService = inventoryService;
        this.meterRegistry = meterRegistry;
        this.consumerGroup = environment.getProperty(
                "seckill.redis.consumer-group", "inventory-persistence");
        this.consumerName = environment.getProperty(
                "seckill.redis.consumer-name", "inventory-service");
        this.skus = Arrays.stream(environment.getProperty("seckill.skus", "").split(","))
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .toList();
        this.batchSize = environment.getProperty("seckill.redis.consumer-batch-size", Integer.class, 100);
        this.blockMs = environment.getProperty("seckill.redis.consumer-block-ms", Long.class, 50L);
        this.maxDeliveryAttempts = environment.getProperty(
                "seckill.redis.max-delivery-attempts", Integer.class, 8);
        this.dlqTtlSeconds = environment.getProperty(
                "seckill.redis.dlq-ttl-seconds", Long.class, Duration.ofDays(1).toSeconds());
        this.dlqMaxLength = environment.getProperty(
                "seckill.redis.dlq-max-length", Long.class, 10_000L);
        registerRedisGauges();
    }

    @Scheduled(fixedDelayString = "${seckill.redis.consumer-poll-ms:100}")
    public void consume() {
        for (String sku : skus) {
            consumeStream(streamKey(sku), sku);
        }
    }

    private void consumeStream(String streamKey, String configuredSku) {
        if (!Boolean.TRUE.equals(redisTemplate.hasKey(streamKey))) {
            return;
        }
        ensureGroup(streamKey);
        StreamReadOptions options = StreamReadOptions.empty()
                .count(batchSize)
                .block(Duration.ofMillis(blockMs));
        process(redisTemplate.opsForStream().read(
                Consumer.from(consumerGroup, consumerName),
                options,
                StreamOffset.create(streamKey, ReadOffset.from("0-0"))), streamKey, configuredSku);
        process(redisTemplate.opsForStream().read(
                Consumer.from(consumerGroup, consumerName),
                options,
                StreamOffset.create(streamKey, ReadOffset.lastConsumed())), streamKey, configuredSku);
    }

    private void process(
            List<MapRecord<String, Object, Object>> records,
            String streamKey,
            String configuredSku) {
        if (records == null) {
            return;
        }
        for (MapRecord<String, Object, Object> record : records) {
            boolean persisted = false;
            String outcome = "failed";
            Timer.Sample sample = Timer.start(meterRegistry);
            try {
                Map<Object, Object> values = record.getValue();
                String sku = value(values, "sku");
                if (!configuredSku.equals(sku)) {
                    throw new IllegalArgumentException("stream SKU does not match configured SKU");
                }
                inventoryService.reserveFromSeckill(new SeckillReserveCommand(
                        value(values, "orderId"),
                        value(values, "userId"),
                        sku,
                        Integer.parseInt(value(values, "qty")),
                        value(values, "idempotencyKey")));
                persisted = true;
                redisTemplate.opsForStream().acknowledge(streamKey, consumerGroup, record.getId());
                redisTemplate.opsForHash().delete(attemptKey(configuredSku), record.getId().getValue());
                increment("inventory.seckill.stream.acknowledged");
                outcome = "acknowledged";
            } catch (RuntimeException exception) {
                increment("inventory.seckill.stream.failed");
                if (persisted || !moveToDeadLetterAfterLimit(
                        record, streamKey, configuredSku, exception)) {
                    log.error("failed to persist Redis seckill record stream={} id={}",
                            streamKey, record.getId(), exception);
                } else {
                    outcome = "dead_letter";
                }
            } finally {
                Timer processingTimer = Timer.builder("oms_seckill_stream_processing")
                        .description("Redis Stream record processing duration")
                        .tag("service", "inventory")
                        .tag("outcome", outcome)
                        .register(meterRegistry);
                sample.stop(processingTimer);
            }
        }
    }

    private boolean moveToDeadLetterAfterLimit(
            MapRecord<String, Object, Object> record,
            String streamKey,
            String sku,
            RuntimeException exception) {
        String attemptKey = attemptKey(sku);
        Long attempts = redisTemplate.opsForHash().increment(
                attemptKey, record.getId().getValue(), 1);
        redisTemplate.expire(attemptKey, Duration.ofHours(2));
        if (attempts == null || attempts < maxDeliveryAttempts) {
            return false;
        }

        Map<String, String> deadLetter = new HashMap<>();
        record.getValue().forEach((key, value) ->
                deadLetter.put(String.valueOf(key), String.valueOf(value)));
        deadLetter.put("error", truncate(exception.getMessage()));
        deadLetter.put("sourceStreamId", record.getId().getValue());
        String deadLetterKey = deadLetterKey(sku);
        redisTemplate.opsForStream().add(deadLetterKey, deadLetter);
        redisTemplate.opsForStream().trim(
                deadLetterKey,
                Math.max(1, Math.min(dlqMaxLength, 100_000L)),
                true);
        redisTemplate.expire(deadLetterKey, Duration.ofSeconds(Math.max(1, dlqTtlSeconds)));

        String qtyValue = deadLetter.get("qty");
        if (qtyValue != null) {
            inventoryService.releaseSeckillQuota(sku, Integer.parseInt(qtyValue));
        }
        redisTemplate.opsForStream().acknowledge(streamKey, consumerGroup, record.getId());
        redisTemplate.opsForHash().delete(attemptKey, record.getId().getValue());
        increment("inventory.seckill.stream.dead_letter");
        return true;
    }

    private void ensureGroup(String streamKey) {
        try {
            redisTemplate.opsForStream().createGroup(
                    streamKey,
                    ReadOffset.latest(),
                    consumerGroup);
        } catch (RuntimeException exception) {
            if (exception.getMessage() == null
                    || !exception.getMessage().contains("BUSYGROUP")) {
                throw exception;
            }
        }
    }

    private static String streamKey(String sku) {
        return "oms:seckill:stream:{" + sku + "}";
    }

    private static String attemptKey(String sku) {
        return "oms:seckill:attempts:{" + sku + "}";
    }

    private static String deadLetterKey(String sku) {
        return "oms:seckill:dlq:{" + sku + "}";
    }

    private void registerRedisGauges() {
        for (String sku : skus) {
            Gauge.builder("oms_seckill_stream_length", () -> streamSize(streamKey(sku)))
                    .description("Current Redis Stream length for configured seckill SKU")
                    .tag("service", "inventory")
                    .tag("sku", sku)
                    .register(meterRegistry);
            Gauge.builder("oms_seckill_dlq_depth", () -> streamSize(deadLetterKey(sku)))
                    .description("Current Redis seckill dead-letter stream length")
                    .tag("service", "inventory")
                    .tag("sku", sku)
                    .register(meterRegistry);
            Gauge.builder("oms_seckill_stream_pending", () -> pendingMessages(streamKey(sku)))
                    .description("Current Redis Stream pending entries for the consumer group")
                    .tag("service", "inventory")
                    .tag("sku", sku)
                    .tag("consumer_group", consumerGroup)
                    .register(meterRegistry);
        }
    }

    private double streamSize(String key) {
        try {
            Long size = redisTemplate.opsForStream().size(key);
            return size == null ? 0 : size.doubleValue();
        } catch (RuntimeException exception) {
            return 0;
        }
    }

    private double pendingMessages(String key) {
        try {
            PendingMessagesSummary summary = redisTemplate.opsForStream()
                    .pending(key, consumerGroup);
            return summary == null ? 0 : summary.getTotalPendingMessages();
        } catch (RuntimeException exception) {
            return 0;
        }
    }

    private static String truncate(String message) {
        if (message == null || message.isBlank()) {
            return "unknown";
        }
        return message.length() <= 500 ? message : message.substring(0, 500);
    }

    private static String value(Map<Object, Object> values, String key) {
        Object value = values.get(key);
        return value == null ? null : value.toString();
    }

    private void increment(String metricName) {
        Counter.builder(metricName)
                .tag("service", "inventory")
                .register(meterRegistry)
                .increment();
    }
}
