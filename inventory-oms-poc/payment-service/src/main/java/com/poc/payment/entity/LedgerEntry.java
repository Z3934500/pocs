package com.poc.payment.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.Immutable;
import java.time.LocalDateTime;

/** Append-only accounting entry. Corrections are new entries, never updates. */
@Entity
@Immutable
@Table(name = "payment_ledger_entry", uniqueConstraints = @UniqueConstraint(
    name = "uk_payment_ledger_txn_account", columnNames = {"ledger_txn_id", "account_code"}))
public class LedgerEntry {
    @Id private String entryId;
    @Column(name = "ledger_txn_id", nullable = false, updatable = false) private String ledgerTxnId;
    @Column(name = "order_id", nullable = false, updatable = false) private String orderId;
    @Column(name = "payment_id", nullable = false, updatable = false) private String paymentId;
    @Column(name = "account_code", nullable = false, updatable = false) private String accountCode;
    @Enumerated(EnumType.STRING) @Column(nullable = false, updatable = false) private LedgerDirection direction;
    @Column(nullable = false, updatable = false) private long amountCents;
    @Column(nullable = false, length = 3, updatable = false) private String currency;
    @Column(nullable = false, updatable = false) private LocalDateTime createdAt;
    protected LedgerEntry() { }
    public LedgerEntry(String entryId, String ledgerTxnId, String orderId, String paymentId, String accountCode,
                       LedgerDirection direction, long amountCents, String currency) {
        this.entryId = entryId; this.ledgerTxnId = ledgerTxnId; this.orderId = orderId; this.paymentId = paymentId;
        this.accountCode = accountCode; this.direction = direction; this.amountCents = amountCents; this.currency = currency;
    }
    @PrePersist void onCreate() { createdAt = LocalDateTime.now(); }
    public String getEntryId() { return entryId; }
    public String getLedgerTxnId() { return ledgerTxnId; }
    public String getOrderId() { return orderId; }
    public String getPaymentId() { return paymentId; }
    public String getAccountCode() { return accountCode; }
    public LedgerDirection getDirection() { return direction; }
    public long getAmountCents() { return amountCents; }
    public String getCurrency() { return currency; }
    public LocalDateTime getCreatedAt() { return createdAt; }
}