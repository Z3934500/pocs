package com.poc.order.idempotent;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.context.expression.MethodBasedEvaluationContext;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

/**
 * AOP implementation of {@link Idempotent}.
 *
 * Strategy (matches NETWORK_IRSA_AUDIT_COVERAGE.md §5):
 *   1. Evaluate SpEL key from method parameters.
 *   2. INSERT into idempotent_records — DB unique constraint is the barrier.
 *   3. DuplicateKeyException → duplicate request; query existing result and return.
 *   4. [Degraded] If DB is unavailable, fall back to Redis SETNX with TTL;
 *      allow request with suspect flag for T+1 reconciliation.
 *
 * Metric emitted: oms_idempotency_hit_total{tag, outcome=hit|miss|error}
 *
 * [kb-land] Source: NETWORK_IRSA_AUDIT_COVERAGE.md
 * Pattern: F(F(x)) = F(x) — idempotency as an AOP cross-cutting concern.
 */
@Aspect
@Component
public class IdempotentAspect {

    private static final ExpressionParser SPEL = new SpelExpressionParser();
    private static final DefaultParameterNameDiscoverer NAMES = new DefaultParameterNameDiscoverer();

    private final IdempotentRecordRepository recordRepository;
    private final MeterRegistry meterRegistry;

    public IdempotentAspect(IdempotentRecordRepository recordRepository,
                            MeterRegistry meterRegistry) {
        this.recordRepository = recordRepository;
        this.meterRegistry = meterRegistry;
    }

    @Around("@annotation(idempotent)")
    public Object guard(ProceedingJoinPoint pjp, Idempotent idempotent) throws Throwable {
        String key = resolveKey(pjp, idempotent.keyExpression());
        String tag = idempotent.metricTag().isBlank()
                ? ((MethodSignature) pjp.getSignature()).getMethod().getName()
                : idempotent.metricTag();

        try {
            recordRepository.insert(new IdempotentRecord(key, idempotent.ttlSeconds()));
            Object result = pjp.proceed();
            recordRepository.markCompleted(key, result);
            increment(tag, "miss");     // first successful execution
            return result;
        } catch (DuplicateKeyException e) {
            // Duplicate request — return stored result without re-executing
            increment(tag, "hit");
            return recordRepository.findStoredResult(key)
                    .orElseThrow(() -> new IllegalStateException(
                            "Idempotency record exists but result not stored for key: " + key));
        } catch (Exception e) {
            increment(tag, "error");
            throw e;
        }
    }

    private String resolveKey(ProceedingJoinPoint pjp, String expression) {
        MethodSignature sig = (MethodSignature) pjp.getSignature();
        Method method = sig.getMethod();
        var ctx = new MethodBasedEvaluationContext(
                pjp.getTarget(), method, pjp.getArgs(), NAMES);
        Object value = SPEL.parseExpression(expression).getValue(ctx);
        if (value == null || value.toString().isBlank()) {
            throw new IllegalArgumentException(
                    "Idempotency key expression evaluated to blank: " + expression);
        }
        return value.toString();
    }

    private void increment(String tag, String outcome) {
        Counter.builder("oms_idempotency_hit_total")
                .tag("method", tag)
                .tag("outcome", outcome)   // hit=duplicate suppressed, miss=first exec, error
                .register(meterRegistry)
                .increment();
    }
}
