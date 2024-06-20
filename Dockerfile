FROM adoptopenjdk:8-jdk-openj9 AS GRADLE_BUILD
RUN mkdir -p ./build/libs/
RUN curl -L http://search.maven.org/remotecontent?filepath=org/jolokia/jolokia-jvm/1.6.2/jolokia-jvm-1.6.2-agent.jar -o ./jolokia-jvm-agent.jar
COPY ./ ./
RUN ./gradlew build

FROM eclipse-temurin:8u412-b08-jre
RUN apt-get update && apt-get upgrade -y curl
COPY --from=GRADLE_BUILD ./build/libs/ /opt/firehose/bin
COPY --from=GRADLE_BUILD ./jolokia-jvm-agent.jar /opt/firehose
COPY --from=GRADLE_BUILD ./src/main/resources/log4j.xml /opt/firehose/etc/log4j.xml
COPY --from=GRADLE_BUILD ./src/main/resources/logback.xml /opt/firehose/etc/logback.xml
WORKDIR /opt/firehose
CMD ["java", "-cp", "bin/*:/work-dir/*", "com.gotocompany.firehose.launch.Main", "-server", "-Dlogback.configurationFile=etc/firehose/logback.xml", "-Xloggc:/var/log/firehose"]
