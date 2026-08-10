package com.poc.contracts;

/**
 * Durable command carried by the optional strict-order inventory queue.
 *
 * <p>The queue is partitioned by {@code sku} in this PoC. A production
 * implementation should use the smallest business competition scope, such
 * as warehouse plus SKU, and assign a sequence at the ingress boundary when
 * business time order matters.</p>
 */
public record InventoryReservationQueueCommand(
        Action action,
        String orderId,
        String sku,
        ReserveInventoryCommand reserveCommand) {

    public enum Action {
        RESERVE,
        CANCEL
    }

    public static InventoryReservationQueueCommand reserve(ReserveInventoryCommand command) {
        if (command == null) {
            throw new IllegalArgumentException("reserve command is required");
        }
        return new InventoryReservationQueueCommand(
                Action.RESERVE,
                command.orderId(),
                command.sku(),
                command);
    }

    public static InventoryReservationQueueCommand cancel(String orderId, String sku) {
        return new InventoryReservationQueueCommand(Action.CANCEL, orderId, sku, null);
    }

    public String messageGroupId() {
        return "sku:" + sku;
    }
}
