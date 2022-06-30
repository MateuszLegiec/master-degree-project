FROM openjdk:8-jdk-alpine
COPY target/*.jar kafka-consumer.jar
ENTRYPOINT ["java","-jar","/kafka-consumer.jar"]
