FROM openjdk:8-jdk-alpine
COPY target/*.jar rabbit-consumer.jar
ENTRYPOINT ["java","-jar","rabbit-consumer.jar"]
