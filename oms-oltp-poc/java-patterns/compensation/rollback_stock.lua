-- rollback_stock.lua
-- Idempotent stock compensation — reverses a seckill deduction.
--
-- KEYS[1] = oms:seckill:stock:{sku}      (stock counter)
-- KEYS[2] = oms:seckill:users:{sku}      (dedup user set)
-- KEYS[3] = oms:seckill:requests:{sku}   (idempotency hash)
-- ARGV[2] = userId
-- ARGV[4] = quantity to restore
-- ARGV[5] = idempotencyKey
--
-- WHY SISMEMBER GUARD:
-- Without it, a network-retry double-call would fire INCRBY twice,
-- pushing stock above its original value (e.g. 100 → deduct → 99 →
-- compensate → 100 → compensate again → 101). Manufactured oversell.
--
-- The guard makes this script idempotent: if the user is no longer
-- in the dedup set, compensation already ran — return immediately.

local existed = redis.call('SISMEMBER', KEYS[2], ARGV[2])
if existed == 0 then
    return 'ALREADY_ROLLED_BACK'   -- safe to call any number of times
end

-- User confirmed present → reverse the deduction atomically.
redis.call('INCRBY', KEYS[1], ARGV[4])   -- restore stock counter
redis.call('SREM',   KEYS[2], ARGV[2])   -- remove user from dedup set
redis.call('HDEL',   KEYS[3], ARGV[5])   -- remove idempotency key record

return 'ROLLED_BACK'
