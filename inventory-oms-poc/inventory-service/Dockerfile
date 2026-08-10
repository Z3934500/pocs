FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY pom.xml .
COPY common-contracts/pom.xml common-contracts/pom.xml
COPY inventory-service/pom.xml inventory-service/pom.xml
COPY common-contracts common-contracts
COPY inventory-service/src inventory-service/src
RUN mvn -B -pl inventory-service -am -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
RUN groupadd --system app && useradd --system --gid app app
COPY --from=build /workspace/inventory-service/target/inventory-service-1.0.0.jar /app/app.jar
RUN mkdir -p /app/data && chown -R app:app /app
USER app
EXPOSE 8082
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]