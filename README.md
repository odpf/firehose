# Firehose

![build workflow](https://github.com/goto/firehose/actions/workflows/build.yml/badge.svg)
![package workflow](https://github.com/goto/firehose/actions/workflows/package.yml/badge.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache)](LICENSE)
[![Version](https://img.shields.io/github/v/release/goto/firehose?logo=semantic-release)](Version)

Firehose is a cloud native service for delivering real-time streaming data to destinations such as service endpoints (HTTP or GRPC) & managed databases (Postgres, InfluxDB, Redis, Elasticsearch, Prometheus and MongoDB). With Firehose, you don't need to write applications or manage resources. It can be scaled up to match the throughput of your data. If your data is present in Kafka, Firehose delivers it to the destination(SINK) that you specified.

<p align="center"><img src="./docs/static/assets/overview.svg" /></p>

## Key Features

Discover why users choose Firehose as their main Kafka Consumer

- **Sinks:** Firehose supports sinking stream data to :

  - log console
  - MongoDB
  - Prometheus
  - HTTP
  - GRPC
  - PostgresDB(JDBC)
  - InfluxDB
  - Elasticsearch
  - Redis
  - Bigquery
  - BigTable  
  - Blob Storage/Object Storage :
    - Google Cloud Storage

- **Scale:** Firehose scales in an instant, both vertically and horizontally for high performance streaming sink and zero data drops.
- **Extensibility:** Add your own sink to firehose with a clearly defined interface or choose from already provided ones.
- **Runtime:** Firehose can run inside VMs or containers in a fully managed runtime environment like kubernetes.
- **Metrics:** Always know what’s going on with your deployment with built-in [monitoring](./docs/assets/firehose-grafana-dashboard.json) of throughput, response times, errors and more.

To know more, follow the detailed [documentation](docs)

## Usage

Explore the following resources to get started with Firehose:

- [Guides](docs/docs/guides) provides guidance on [creating Firehose](docs/guides/overview.md) with different sinks.
- [Concepts](docs/docs/concepts) describes all important Firehose concepts.
- [Reference](docs/docs/reference) contains details about configurations, metrics and other aspects of Firehose.
- [Contribute](docs/docs/contribute/contribution.md) contains resources for anyone who wants to contribute to Firehose.

## Run with Docker

Use the docker hub to download firehose [docker image](https://hub.docker.com/r/gotocompany/firehose/). You need to have docker installed in your system.

```
# Download docker image from docker hub
$ docker pull gotocompany/firehose

# Run the following docker command for a simple log sink.
$ docker run -e SOURCE_KAFKA_BROKERS=127.0.0.1:6667 -e SOURCE_KAFKA_CONSUMER_GROUP_ID=kafka-consumer-group-id -e SOURCE_KAFKA_TOPIC=sample-topic -e SINK_TYPE=log -e SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET=latest -e INPUT_SCHEMA_PROTO_CLASS=com.github.firehose.sampleLogProto.SampleLogMessage -e SCHEMA_REGISTRY_STENCIL_ENABLE=true -e SCHEMA_REGISTRY_STENCIL_URLS=http://localhost:9000/artifactory/proto-descriptors/latest/gotocompany/firehose:latest
```

**Note:** Make sure your protos (.jar file) are located in `work-dir`, this is required for Filter functionality to work.

## Run with Kubernetes

- Create a firehose deployment using the helm chart available [here](https://github.com/goto/charts/tree/main/stable/firehose)
- Deployment also includes telegraf container which pushes stats metrics

## Running locally

```sh
# Clone the repo
$ git clone https://github.com/goto/firehose.git

# Build the jar
$ ./gradlew clean build

# Configure env variables
$ cat env/local.properties

# Run the Firehose
$ ./gradlew runConsumer
```

**Note:** Sample configuration for other sinks along with some advanced configurations can be found [here](/docs/reference/configuration.md)

## Running tests

```sh
# Running unit tests
$ ./gradlew test

# Run code quality checks
$ ./gradlew checkstyleMain checkstyleTest

#Cleaning the build
$ ./gradlew clean
```

## Contribute

Development of Firehose happens in the open on GitHub, and we are grateful to the community for contributing bugfixes and improvements. Read below to learn how you can take part in improving Firehose.

Read our [contributing guide](docs/docs/contribute/contribution.md) to learn about our development process, how to propose bugfixes and improvements, and how to build and test your changes to Firehose.

To help you get your feet wet and get you familiar with our contribution process, we have a list of [good first issues](https://github.com/goto/firehose/labels/good%20first%20issue) that contain bugs which have a relatively limited scope. This is a great place to get started.

## Credits

This project exists thanks to all the [contributors](https://github.com/goto/firehose/graphs/contributors).

## License

Firehose is [Apache 2.0](LICENSE) licensed.
