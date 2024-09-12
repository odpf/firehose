FROM adoptopenjdk:8-jdk-openj9 AS GRADLE_BUILD
RUN mkdir -p ./build/libs/
RUN curl -L http://search.maven.org/remotecontent?filepath=org/jolokia/jolokia-jvm/1.6.2/jolokia-jvm-1.6.2-agent.jar -o ./jolokia-jvm-agent.jar
RUN curl -L https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar -o ./jmx_prometheus_javaagent.jar
COPY ./ ./
RUN ./gradlew build

FROM openjdk:8-jre
COPY --from=GRADLE_BUILD ./build/libs/ /opt/firehose/bin
COPY --from=GRADLE_BUILD ./jolokia-jvm-agent.jar /opt/firehose
COPY --from=GRADLE_BUILD ./jmx_prometheus_javaagent.jar /opt/firehose
COPY --from=GRADLE_BUILD ./src/main/resources/log4j.xml /opt/firehose/etc/log4j.xml
COPY --from=GRADLE_BUILD ./src/main/resources/logback.xml /opt/firehose/etc/logback.xml
COPY ./jmx_exporter_config.yml /opt/firehose/etc/jmx_exporter_config.yml
WORKDIR /opt/firehose
EXPOSE 8778/tcp
EXPOSE 9404/tcp
CMD ["java", \
    "-javaagent:/opt/firehose/jmx_prometheus_javaagent.jar=9404:/opt/firehose/etc/jmx_exporter_config.yml", \
    "-cp", "bin/*:/work-dir/*", \
    "org.raystack.firehose.launch.Main", \
    "-server", \
    "-Dlogback.configurationFile=etc/firehose/logback.xml", \
    "-Xloggc:/var/log/firehose"]