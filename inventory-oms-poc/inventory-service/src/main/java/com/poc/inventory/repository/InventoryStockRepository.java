package com.poc.inventory.repository;

import com.poc.inventory.entity.InventoryStock;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import java.util.Optional;

public interface InventoryStockRepository extends JpaRepository<InventoryStock, String> {
    /** Serializes all concurrent mutations for one SKU. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select stock from InventoryStock stock where stock.sku = :sku")
    Optional<InventoryStock> findBySkuForUpdate(@Param("sku") String sku);
}