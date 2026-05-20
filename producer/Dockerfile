# syntax=docker/dockerfile:1.7
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
RUN apt-get update \
  && apt-get install -y --no-install-recommends maven \
  && rm -rf /var/lib/apt/lists/*
COPY pom.xml .
COPY common/pom.xml common/
COPY producer/pom.xml producer/
COPY consumer/pom.xml consumer/
RUN mvn -pl producer -am -B dependency:go-offline
COPY common common
COPY producer producer
RUN mvn -pl producer -am -B -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build --chown=1001:0 /workspace/producer/target/producer-0.1.0-SNAPSHOT.jar app.jar
RUN chmod g+rx app.jar
USER 1001
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
