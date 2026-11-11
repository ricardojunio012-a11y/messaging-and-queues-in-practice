# Stage 1: build with Maven
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml ./
RUN mvn -q dependency:go-offline
COPY src ./src
RUN mvn -q package -DskipTests

# Stage 2: final image with only the Java runtime. Not the alpine variant: RocksDB,
# where Kafka Streams keeps the state of the windows, needs glibc's libstdc++.
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/target/app.jar ./app.jar
CMD ["java", "-jar", "app.jar"]
