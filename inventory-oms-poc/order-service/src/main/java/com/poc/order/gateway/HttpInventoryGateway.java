package com.poc.order.gateway;

import com.poc.contracts.InventoryReservationResponse;
import com.poc.contracts.ReserveInventoryCommand;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/** REST adapter for Inventory Service; the Saga handles network failures. */
@Component
public class HttpInventoryGateway implements InventoryGateway {

    private final RestClient client;

    public HttpInventoryGateway(
            RestClient.Builder builder,
            @Value("${clients.inventory.base-url}") String baseUrl) {
        this.client = builder.baseUrl(baseUrl).build();
    }

    @Override
    public InventoryReservationResponse reserve(ReserveInventoryCommand command) {
        return client.post()
                .uri("/internal/inventory/reservations")
                .body(command)
                .retrieve()
                .body(InventoryReservationResponse.class);
    }

    @Override
    public InventoryReservationResponse commit(String orderId) {
        return client.post()
                .uri("/internal/inventory/reservations/{orderId}/commit", orderId)
                .retrieve()
                .body(InventoryReservationResponse.class);
    }

    @Override
    public InventoryReservationResponse release(String orderId, String reason) {
        return client.post()
                .uri(uriBuilder -> uriBuilder
                        .path("/internal/inventory/reservations/{orderId}/release")
                        .queryParam("reason", reason)
                        .build(orderId))
                .retrieve()
                .body(InventoryReservationResponse.class);
    }
}