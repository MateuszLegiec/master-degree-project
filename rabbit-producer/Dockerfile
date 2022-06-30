FROM openjdk:8-jdk-alpine
COPY target/*.jar rabbit-producer.jar
ENTRYPOINT ["java","-jar","/rabbit-producer.jar"]
