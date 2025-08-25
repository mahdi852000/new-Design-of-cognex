# Use OpenJDK 21 slim image
FROM eclipse-temurin:21-jdk-jammy

# Set working directory inside container
WORKDIR /app

# Copy the Fat JAR into container
COPY target/akka-java-demo-1.0.0-shaded.jar /app/akka-java-demo.jar

# Expose Metrics port
EXPOSE 9402

# Command to run the JAR
ENTRYPOINT ["java", "-jar", "akka-java-demo.jar"]
