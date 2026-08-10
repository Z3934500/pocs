package com.poc.inventory.seckill;

import com.poc.contracts.SeckillReserveCommand;

public interface SeckillAdmission {
    SeckillAdmissionResponse initializeQuota(String sku, int qty);

    SeckillAdmissionResponse reserve(SeckillReserveCommand command);
}
