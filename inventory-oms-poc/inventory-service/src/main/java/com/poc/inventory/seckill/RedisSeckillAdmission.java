package com.poc.inventory.seckill;

import com.poc.contracts.SeckillReserveCommand;
import com.poc.inventory.service.InventoryService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

/**
 * Redis/Lua is an admission layer, not the system of record. The Lua script
 * atomically checks quota, deduplicates the user/request, decrements Redis and
 * appends a Redis Stream record. The stream consumer later persists the
 * reservation against a DB quota already removed from normal stock.
 */
@Component
@ConditionalOnProperty(name = "seckill.redis.enabled", havingValue = "true")
public class RedisSeckillAdmission implements SeckillAdmission {
    private static final DefaultRedisScript<String> ADMIT_SCRIPT =
            new DefaultRedisScript<>("""
                    local duplicateOrder = redis.call('HGET', KEYS[3], ARGV[5])
                    if duplicateOrder then
                      return 'DUPLICATE:' .. duplicateOrder
                    end
                    if redis.call('SISMEMBER', KEYS[2], ARGV[2]) == 1 then
                      return 'DUPLICATE_USER'
                    end
                    local available = redis.call('GET', KEYS[1])
                    if not available then
                      return 'NOT_INITIALIZED'
                    end
                    if tonumber(available) < tonumber(ARGV[4]) then
                      return 'SOLD_OUT'
                    end
                    redis.call('DECRBY', KEYS[1], ARGV[4])
                    redis.call('SADD', KEYS[2], ARGV[2])
                    redis.call('HSET', KEYS[3], ARGV[5], ARGV[1])
                    local streamId = redis.call(
                      'XADD', KEYS[4], 'MAXLEN', '~', ARGV[7], '*',
                      'orderId', ARGV[1],
                      'userId', ARGV[2],
                      'sku', ARGV[3],
                      'qty', ARGV[4],
                      'idempotencyKey', ARGV[5])
                    redis.call('EXPIRE', KEYS[2], ARGV[6])
                    redis.call('EXPIRE', KEYS[3], ARGV[6])
                    return 'ACCEPTED:' .. streamId
                    """, String.class);

    private final StringRedisTemplate redisTemplate;
    private final InventoryService inventoryService;
    private final MeterRegistry meterRegistry;
    private final long dedupTtlSeconds;
    private final long streamMaxLength;

    public RedisSeckillAdmission(
            StringRedisTemplate redisTemplate,
            InventoryService inventoryService,
            MeterRegistry meterRegistry,
            org.springframework.core.env.Environment environment) {
        this.redisTemplate = redisTemplate;
        this.inventoryService = inventoryService;
        this.meterRegistry = meterRegistry;
        this.dedupTtlSeconds = environment.getProperty(
                "seckill.redis.dedup-ttl-seconds", Long.class, Duration.ofHours(2).toSeconds());
        this.streamMaxLength = environment.getProperty(
                "seckill.redis.stream-max-length", Long.class, 100_000L);
    }

    @Override
    public SeckillAdmissionResponse initializeQuota(String sku, int qty) {
        validateSku(sku);
        if (qty <= 0) {
            throw new IllegalArgumentException("qty must be positive");
        }
        inventoryService.allocateSeckillQuota(sku, qty);
        String stockKey = key("stock", sku);
        try {
            redisTemplate.delete(List.of(
                    key("users", sku),
                    key("requests", sku),
                    key("stream", sku)));
            redisTemplate.opsForValue().set(stockKey, Integer.toString(qty));
            return SeckillAdmissionResponse.accepted("quota=" + qty);
        } catch (RuntimeException exception) {
            // A timeout makes SET ambiguous. Release only when Redis confirms
            // that no quota key exists; reconciliation handles the unknown case.
            if (redisTemplate.opsForValue().get(stockKey) == null) {
                inventoryService.releaseSeckillQuota(sku, qty);
            }
            throw exception;
        }
    }

    @Override
    public SeckillAdmissionResponse reserve(SeckillReserveCommand command) {
        validate(command);
        validateSku(command.sku());
        String result;
        try {
            result = redisTemplate.execute(
                    ADMIT_SCRIPT,
                    List.of(
                            key("stock", command.sku()),
                            key("users", command.sku()),
                            key("requests", command.sku()),
                            key("stream", command.sku())),
                    command.orderId(),
                    command.userId(),
                    command.sku(),
                    Integer.toString(command.qty()),
                    command.idempotencyKey(),
                    Long.toString(dedupTtlSeconds),
                    Long.toString(Math.max(1, Math.min(streamMaxLength, 1_000_000L))));
        } catch (RuntimeException exception) {
            increment("error");
            throw exception;
        }
        if (result == null) {
            increment("error");
            throw new IllegalStateException("Redis Lua admission returned no result");
        }
        increment(admissionOutcome(result));
        if (result.startsWith("ACCEPTED:")) {
            return SeckillAdmissionResponse.accepted(result.substring("ACCEPTED:".length()));
        }
        return SeckillAdmissionResponse.rejected(result);
    }

    private static String key(String type, String sku) {
        return "oms:seckill:" + type + ":{" + sku + "}";
    }

    private static void validate(SeckillReserveCommand command) {
        if (command == null
                || blank(command.orderId())
                || blank(command.userId())
                || blank(command.sku())
                || blank(command.idempotencyKey())
                || command.qty() <= 0) {
            throw new IllegalArgumentException("invalid seckill command");
        }
    }

    private static void validateSku(String sku) {
        if (blank(sku) || sku.contains("{") || sku.contains("}")) {
            throw new IllegalArgumentException("sku must be a non-empty key-safe value");
        }
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    private void increment(String outcome) {
        Counter.builder("oms_seckill_admission_total")
                .description("Redis/Lua seckill admission outcomes")
                .tag("service", "inventory")
                .tag("outcome", outcome)
                .register(meterRegistry)
                .increment();
    }

    private static String admissionOutcome(String result) {
        if (result.startsWith("ACCEPTED:")) {
            return "accepted";
        }
        if (result.startsWith("DUPLICATE:")) {
            return "duplicate_request";
        }
        return switch (result) {
            case "DUPLICATE_USER" -> "duplicate_user";
            case "NOT_INITIALIZED" -> "not_initialized";
            case "SOLD_OUT" -> "sold_out";
            default -> "rejected";
        };
    }
}
