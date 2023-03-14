# Deployment

Firehose can deployed locally, inside a Docker container or in a Kubernetes cluster. The following external services must be installed and launched before deploying Firehose on any platform -

- Apache Kafka Server 2.3+
- Stencil Server as schema registry
- Telegraf as the StatsD host
- InfluxDB for storing metrics
- Grafana for metrics visualization
- destination Sink server

Refer the [Development Guide](../contribute/development.md) section on how to set up and configure the above services. For instructions on how to set up visualization of Firehose metrics , refer the [Monitoring ](../concepts/monitoring.md#setting-up-grafana-with-firehose)section.

## Deploy on Docker

Use the Docker hub to download Firehose [docker image](https://hub.docker.com/r/gotocompany/firehose/). You need to have Docker installed in your system. Follow[ this guide](https://www.docker.com/products/docker-desktop) on how to install and set up Docker in your system.

```text
# Download docker image from docker hub
$ docker pull gotocompany/firehose

# Run the following docker command for a simple log sink.
$ docker run -e SOURCE_KAFKA_BROKERS=127.0.0.1:6667 -e SOURCE_KAFKA_CONSUMER_GROUP_ID=kafka-consumer-group-id -e SOURCE_KAFKA_TOPIC=sample-topic -e SINK_TYPE=log -e SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET=latest -e INPUT_SCHEMA_PROTO_CLASS=com.github.firehose.sampleLogProto.SampleLogMessage -e SCHEMA_REGISTRY_STENCIL_ENABLE=true -e SCHEMA_REGISTRY_STENCIL_URLS=http://localhost:9000/artifactory/proto-descriptors/latest/gotocompany/firehose:latest
```

**Note:** Make sure your protos \(.jar file\) are located in `work-dir`, this is required for Filter functionality to work.

## Deploy on Kubernetes

Kubernetes is an open-source container-orchestration system for automating computer application deployment, scaling, and management. Follow [this guide](https://kubernetes.io/docs/setup/) on how to set up and configure a Kubernetes cluster.

Then create a Firehose deployment using the Helm chart available [here](https://github.com/goto/charts/tree/main/stable/firehose). The Helm chart Deployment also includes Telegraf container which works as a metrics aggregator and pushes StatsD metrics to InfluxDB. Make sure to configure all Kafka, sink and filter parameters in `values.yaml` file before deploying the Helm chart.

Description and default values for each parameter can be found[ here](https://github.com/goto/charts/tree/main/stable/firehose#values).

## Deploy locally

Firehose needs Java SE Development Kit 8 to be installed and configured in `JAVA_HOME` environment variable.

```text
# Clone the repo
$ git clone https://github.com/goto/firehose.git

# Build the jar
$ ./gradlew clean build

# Configure env variables
$ cat env/local.properties

# Run the Firehose
$ ./gradlew runConsumer
```

**Note:** Sample configuration for other sinks along with some advanced configurations can be found [here](https://github.com/goto/firehose/blob/main/docs/reference/configuration.md)
