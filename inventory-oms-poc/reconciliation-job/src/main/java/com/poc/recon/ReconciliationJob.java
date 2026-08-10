package com.poc.recon;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@SpringBootApplication
public class ReconciliationJob {

    public static void main(String[] args) {
        SpringApplication.run(ReconciliationJob.class, args);
    }

    @Component
    public static class JobRunner implements CommandLineRunner {

        private final JdbcTemplate jdbc;

        public JobRunner(JdbcTemplate jdbc) {
            this.jdbc = jdbc;
        }

        @Override
        public void run(String... args) {
            System.out.println("Inventory reconciliation:");
            System.out.println(jdbc.queryForList("""
                SELECT s.sku,
                       s.reserved_qty,
                       COALESCE(SUM(CASE WHEN r.status = 'RESERVED' THEN r.qty ELSE 0 END), 0) AS reservation_qty,
                       CASE WHEN s.reserved_qty = COALESCE(SUM(CASE WHEN r.status = 'RESERVED' THEN r.qty ELSE 0 END), 0)
                            THEN 'OK' ELSE 'MISMATCH' END AS reconciliation_status
                FROM inventory_stock s
                LEFT JOIN inventory_reservation r ON r.sku = s.sku
                GROUP BY s.sku, s.reserved_qty
                ORDER BY s.sku
                """));

            System.out.println("Payment ledger reconciliation:");
            System.out.println(jdbc.queryForList("""
                SELECT ledger_txn_id,
                       SUM(CASE WHEN direction = 'DEBIT' THEN amount_cents ELSE 0 END) AS debit_cents,
                       SUM(CASE WHEN direction = 'CREDIT' THEN amount_cents ELSE 0 END) AS credit_cents,
                       CASE WHEN SUM(CASE WHEN direction = 'DEBIT' THEN amount_cents ELSE 0 END)
                                  = SUM(CASE WHEN direction = 'CREDIT' THEN amount_cents ELSE 0 END)
                            THEN 'BALANCED' ELSE 'MISMATCH' END AS ledger_status
                FROM payment_ledger_entry
                GROUP BY ledger_txn_id
                ORDER BY ledger_txn_id
                """));
        }
    }
}