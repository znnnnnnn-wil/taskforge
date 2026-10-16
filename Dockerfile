FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
RUN chown 10001:10001 /app
COPY --chown=10001:10001 target/taskforge-1.0.0.jar app.jar
COPY --chown=10001:10001 scripts/healthcheck.sh /app/healthcheck.sh
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=70", "-jar", "/app/app.jar"]
