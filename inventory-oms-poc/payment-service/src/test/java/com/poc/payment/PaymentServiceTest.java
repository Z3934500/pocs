package com.poc.payment;

import com.poc.contracts.*;
import com.poc.payment.entity.*;
import com.poc.payment.repository.*;
import com.poc.payment.service.PaymentService;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.*;
import java.util.stream.Collectors;

@SpringBootTest
class PaymentServiceTest {
    @Autowired PaymentService service;
    @Autowired PaymentTransactionRepository paymentRepository;
    @Autowired PaymentOutboxRepository outboxRepository;
    @Autowired LedgerEntryRepository ledgerRepository;

    @BeforeEach void reset() { outboxRepository.deleteAll(); ledgerRepository.deleteAll(); paymentRepository.deleteAll(); }

    @Test void captureIsIdempotentAndWritesBalancedLedger() {
        CapturePaymentCommand command = new CapturePaymentCommand("ORDER-1", "pay-1", "provider-1", 1200, "CNY", true);
        PaymentResponse first = service.capture(command); PaymentResponse retry = service.capture(command);
        assertEquals(first.paymentId(), retry.paymentId()); assertEquals(PaymentStatus.CAPTURED, first.status()); assertEquals(2, ledgerRepository.count()); assertEquals(1, outboxRepository.count());
        assertEquals(Set.of(LedgerDirection.DEBIT, LedgerDirection.CREDIT), ledgerRepository.findAll().stream().map(LedgerEntry::getDirection).collect(Collectors.toSet()));
    }

    @Test void refundIsACompensatingPaymentState() {
        PaymentResponse captured = service.capture(new CapturePaymentCommand("ORDER-2", "pay-2", "provider-2", 900, "CNY", true));
        assertEquals(PaymentStatus.REFUNDED, service.refund(new RefundPaymentCommand(captured.paymentId(), "refund-2")).status());
        assertEquals(PaymentStatus.REFUNDED, service.get(captured.paymentId()).status()); assertEquals(4, ledgerRepository.count());
    }

    @Test void failedGatewayDoesNotWriteSuccessLedger() {
        PaymentResponse failed = service.capture(new CapturePaymentCommand("ORDER-3", "pay-3", "provider-3", 900, "CNY", false));
        assertEquals(PaymentStatus.FAILED, failed.status()); assertEquals(0, ledgerRepository.count());
    }
}