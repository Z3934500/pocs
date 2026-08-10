package com.poc.order.idempotent;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Method-level idempotency guard — wraps the DB-unique-constraint approach
 * described in NETWORK_IRSA_AUDIT_COVERAGE.md §5.
 *
 * Usage:
 * <pre>
 *   {@literal @}Idempotent(keyExpression = "#cmd.orderId + ':' + #cmd.idempotencyKey")
 *   {@literal @}Transactional
 *   public PaymentResponse capture(CapturePaymentCommand cmd) { ... }
 * </pre>
 *
 * Guarantee: F(F(x)) = F(x) — duplicate calls with the same key return the
 * same result without re-executing side-effects.
 *
 * [kb-land] Source: NETWORK_IRSA_AUDIT_COVERAGE.md — Idempotency AOP gap
 * Pattern: DB unique constraint as the physical idempotency barrier;
 *          Redis SETNX as the degraded fallback when DB is unavailable.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Idempotent {

    /**
     * SpEL expression evaluated against method parameters to derive the
     * idempotency key. Examples:
     * <ul>
     *   <li>"#cmd.orderId + ':' + #cmd.idempotencyKey"</li>
     *   <li>"#orderId + ':capture'"</li>
     * </ul>
     */
    String keyExpression();

    /**
     * How long to keep the idempotency record (used for Redis fallback TTL
     * and DB retention hint). Defaults to 24 hours.
     */
    long ttlSeconds() default 86_400L;

    /**
     * Metric tag prefix for oms_idempotency_hit_total.
     * Defaults to the annotated method's simple name.
     */
    String metricTag() default "";
}
