FROM openjdk:8-jdk-alpine
COPY target/*.jar kafka-producer.jar
ENTRYPOINT ["java","-jar","/kafka-producer.jar"]
