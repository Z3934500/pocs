package com.poc.inventory.seckill;

import com.poc.contracts.SeckillReserveCommand;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(
        name = "seckill.redis.enabled",
        havingValue = "false",
        matchIfMissing = true)
public class DisabledSeckillAdmission implements SeckillAdmission {
    @Override
    public SeckillAdmissionResponse initializeQuota(String sku, int qty) {
        throw new IllegalStateException("Redis seckill admission is disabled");
    }

    @Override
    public SeckillAdmissionResponse reserve(SeckillReserveCommand command) {
        throw new IllegalStateException("Redis seckill admission is disabled");
    }
}
