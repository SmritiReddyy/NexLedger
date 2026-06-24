FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

RUN addgroup -S nexledger && adduser -S nexledger -G nexledger

COPY target/ledger-service-1.0.0.jar app.jar

RUN chown nexledger:nexledger app.jar
USER nexledger

EXPOSE 8080

ENV JAVA_OPTS="-Xms512m -Xmx1g -XX:+UseG1GC -XX:MaxGCPauseMillis=100"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
