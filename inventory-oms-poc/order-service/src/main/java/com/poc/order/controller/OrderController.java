package com.poc.order.controller;

import com.poc.order.dto.CreateOrderRequest;
import com.poc.order.entity.OrderAggregate;
import com.poc.order.service.OrderWorkflowService;
import org.springframework.web.bind.annotation.*;

/** Public Order API; it is the only entry point that orchestrates payment and inventory. */
@RestController
@RequestMapping("/orders")
public class OrderController {
    private final OrderWorkflowService workflowService;
    public OrderController(OrderWorkflowService workflowService) { this.workflowService = workflowService; }
    @PostMapping
    public OrderAggregate place(@RequestHeader(value="Idempotency-Key", required=false) String headerKey, @RequestBody CreateOrderRequest request) { return workflowService.placeOrder(request, headerKey); }
    @GetMapping("/{orderId}")
    public OrderAggregate get(@PathVariable String orderId) { return workflowService.get(orderId); }
    @PostMapping("/{orderId}/cancel")
    public OrderAggregate cancel(@PathVariable String orderId, @RequestParam(required=false) String reason) { return workflowService.cancel(orderId, reason); }
}