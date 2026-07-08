package com.poc.recon;

import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
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
            System.out.println("Running reconciliation SQL:");

            String sql = """
                SELECT sku, SUM(qty)
                FROM inventory_reservation
                WHERE status = 'Pending'
                GROUP BY sku
            """;

            var results = jdbc.queryForList(sql);
            System.out.println(results);
        }
    }
}
