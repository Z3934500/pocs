package com.poc.inventory.controller;

import com.poc.contracts.*;
import com.poc.inventory.entity.*;
import com.poc.inventory.seckill.SeckillAdmission;
import com.poc.inventory.seckill.SeckillAdmissionResponse;
import com.poc.inventory.service.InventoryService;
import org.springframework.web.bind.annotation.*;
import java.util.List;

/** Internal API; production protects it with service-to-service IAM/mTLS or JWT. */
@RestController
@RequestMapping("/internal/inventory")
public class InventoryController {
    private final InventoryService inventoryService;
    private final SeckillAdmission seckillAdmission;

    public InventoryController(
            InventoryService inventoryService,
            SeckillAdmission seckillAdmission) {
        this.inventoryService = inventoryService;
        this.seckillAdmission = seckillAdmission;
    }
    @PostMapping("/reservations")
    public InventoryReservationResponse reserve(@RequestBody ReserveInventoryCommand command) { return inventoryService.reserve(command); }
    @PostMapping("/reservations/{orderId}/commit")
    public InventoryReservationResponse commit(@PathVariable String orderId) { return inventoryService.commit(orderId); }
    @PostMapping("/reservations/{orderId}/release")
    public InventoryReservationResponse release(@PathVariable String orderId, @RequestParam(required = false) String reason) { return inventoryService.release(orderId, reason); }
    @GetMapping("/stock/{sku}")
    public InventoryStock stock(@PathVariable String sku) { return inventoryService.getStock(sku); }
    @GetMapping("/outbox")
    public List<InventoryOutboxEvent> pendingOutbox() { return inventoryService.pendingOutbox(); }

    /**
     * Protected operational endpoint: allocate quota before opening a flash
     * sale. In production this is admin/IAM protected and audited.
     */
    @PostMapping("/seckill/quota/{sku}")
    public SeckillAdmissionResponse allocateSeckillQuota(
            @PathVariable String sku,
            @RequestParam int qty) {
        return seckillAdmission.initializeQuota(sku, qty);
    }

    @PostMapping("/seckill/reservations")
    public SeckillAdmissionResponse seckillReserve(
            @RequestBody SeckillReserveCommand command) {
        return seckillAdmission.reserve(command);
    }
}
