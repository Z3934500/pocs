run:
	mvn -pl reservation-service spring-boot:run

order:
	mvn -pl order-service spring-boot:run

inventory:
	mvn -pl inventory-service spring-boot:run

payment:
	mvn -pl payment-service spring-boot:run

split:
	docker compose -f docker-compose.microservices.yml up --build

recon:
	mvn -pl reconciliation-job spring-boot:run

test:
	mvn -B -ntp test