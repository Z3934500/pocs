package com.poc.inventory.seckill;

public record SeckillAdmissionResponse(String status, String detail) {
    public static SeckillAdmissionResponse accepted(String detail) {
        return new SeckillAdmissionResponse("ACCEPTED", detail);
    }

    public static SeckillAdmissionResponse rejected(String status) {
        return new SeckillAdmissionResponse(status, status);
    }
}
