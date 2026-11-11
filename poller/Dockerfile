FROM eclipse-temurin:21.0.13_9-jdk-jammy@sha256:4b1a74fbffbe6b4e7b6f0171a472478282fb463790a51edf10e465d578f22b69 AS build
WORKDIR /workspace
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
COPY contracts/pom.xml contracts/pom.xml
COPY contracts/src contracts/src
COPY monitor/pom.xml monitor/pom.xml
COPY poller/pom.xml poller/pom.xml
COPY poller/src poller/src
RUN sh ./mvnw -B -pl poller -am -DskipTests package

FROM eclipse-temurin:21.0.13_9-jre-jammy@sha256:1eac8b8fee11ca7d6725a48bc5d41992ed1561504ebae25490d2d062847e8269
WORKDIR /app
RUN mkdir -p /var/lib/flight-poller /app/healthcheck
COPY --from=build /workspace/poller/target/poller.jar /app/poller.jar
COPY --from=build /workspace/poller/target/classes/com/flightmonitor/poller/HealthProbe.class /app/healthcheck/com/flightmonitor/poller/HealthProbe.class
RUN chown -R 10001:10001 /var/lib/flight-poller /app
USER 10001:10001
ENV JAVA_TOOL_OPTIONS="-Xms64m -Xmx192m -XX:MaxDirectMemorySize=64m"
EXPOSE 8080
HEALTHCHECK --interval=20s --timeout=3s --start-period=40s --retries=3 CMD ["java", "-cp", "/app/healthcheck", "com.flightmonitor.poller.HealthProbe"]
ENTRYPOINT ["java", "-jar", "/app/poller.jar"]
