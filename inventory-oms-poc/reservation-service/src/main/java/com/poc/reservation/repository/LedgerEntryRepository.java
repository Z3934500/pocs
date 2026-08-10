package com.poc.reservation.repository;

import com.poc.reservation.entity.LedgerEntry;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface LedgerEntryRepository extends JpaRepository<LedgerEntry, String> {
    List<LedgerEntry> findByLedgerTxnIdOrderByAccountCode(String ledgerTxnId);
}