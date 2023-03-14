# Development Guide

The following guide will help you quickly run Firehose in your local machine. The main components of Firehose are:

- Consumer: Handles data consumption from Kafka.
- Sink: Package which handles sinking data.
- Metrics: Handles the metrics via StatsD client

## Requirements

### Development environment

Java SE Development Kit 8 is required to build, test and run Firehose service. Oracle JDK 8 can be downloaded from [here](https://www.oracle.com/in/java/technologies/javase/javase-jdk8-downloads.html). Extract the tarball to your preferred installation directory and configure your `PATH` environment variable to point to the `bin` sub-directory in the JDK 8 installation directory. For example -

```bash
export PATH=~/Downloads/jdk1.8.0_291/bin:$PATH
```

### Environment Variables

Firehose environment variables can be configured in either of the following ways -

- append a new line at the end of `env/local.properties` file. Variables declared in `local.properties` file are automatically added to the environment during runtime.
- run `export SAMPLE_VARIABLE=287` on a UNIX shell, to directly assign the required environment variable.

### Kafka Server

Apache Kafka server service must be set up, from which Firehose's Kafka consumer will pull messages. Kafka Server version greater than 2.4 is currently supported by Firehose. Kafka Server URL and port address, as well as other Kafka-specific parameters must be configured in the corresponding environment variables as defined in the [Generic configuration](../advance/generic) section.

Read the[ official guide](https://kafka.apache.org/quickstart) on how to install and configure Apache Kafka Server.

### Destination Sink Server

The sink to which Firehose will stream Kafka's data to, must have its corresponding server set up and configured. The URL and port address of the database server / HTTP/GRPC endpoint , along with other sink - specific parameters must be configured the environment variables corresponding to that particular sink.

Configuration parameter variables of each sink can be found in the [Configurations](../advance/generic/) section.

### Schema Registry

When `INPUT_SCHEMA_DATA_TYPE is set to protobuf`, firehose uses Stencil Server as its Schema Registry for hosting Protobuf descriptors. The environment variable `SCHEMA_REGISTRY_STENCIL_ENABLE` must be set to `true` . Stencil server URL must be specified in the variable `SCHEMA_REGISTRY_STENCIL_URLS` . The Proto Descriptor Set file of the Kafka messages must be uploaded to the Stencil server.

Refer [this guide](https://github.com/goto/stencil/tree/master/server#readme) on how to set up and configure the Stencil server, and how to generate and upload Proto descriptor set file to the server.

### Monitoring

Firehose sends critical metrics via StatsD client. Refer the[ Monitoring](../concepts/monitoring.md#setting-up-grafana-with-firehose) section for details on how to setup Firehose with Grafana. Alternatively, you can set up any other visualization platform for monitoring Firehose. Following are the typical requirements -

- StatsD host \(e.g. Telegraf\) for aggregation of metrics from Firehose StatsD client
- A time-series database \(e.g. InfluxDB\) to store the metrics
- GUI visualization dashboard \(e.g. Grafana\) for detailed visualisation of metrics

## Running locally

- The following guides provide a simple way to run firehose with a log sink locally.
- It uses the TestMessage (src/test/proto/TestMessage.proto) proto schema, which has already been provided for testing purposes.

```bash
# Clone the repo
$ git clone https://github.com/goto/firehose.git

# Build the jar
$ ./gradlew clean build

# Configure env variables
$ cat env/local.properties
```
### Configure env/local.properties

Set the generic variables in the local.properties file.

```text
KAFKA_RECORD_PARSER_MODE = message
SINK_TYPE = log
INPUT_SCHEMA_DATA_TYPE=protobuf
INPUT_SCHEMA_PROTO_CLASS = com.gotocompany.firehose.consumer.TestMessage
```
Set the variables which specify the kafka server, topic name, and group-id of the kafka consumer - the standard values are used here.
```text
SOURCE_KAFKA_BROKERS = localhost:9092
SOURCE_KAFKA_TOPIC = test-topic
SOURCE_KAFKA_CONSUMER_GROUP_ID = sample-group-id
```

### Stencil Workaround
Firehose uses [Stencil](https://github.com/goto/stencil) as the schema-registry which enables dynamic proto schemas. For the sake of this
quick-setup guide, we can work our way around Stencil setup by setting up a simple local HTTP server which can provide the static descriptor for TestMessage schema.


- Install a server service - like [this](https://github.com/http-party/http-server) one.

- Generate the descriptor for TestMessage by running the command on terminal -
```shell
./gradlew generateTestProto
```
- The above should generate a file (src/test/resources/__files/descriptors.bin), move this to a new folder at a separate location, and start the HTTP-server there so that this file can be fetched at the runtime.
- If you are using [this](https://github.com/http-party/http-server), use this command after moving the file to start server at the default port number 8080.
```shell
http-server
```
- Because we are not using the schema-registry in the default mode, the following lines should also be added in env/local.properties to specify the new location to fetch descriptor from.
```text
SCHEMA_REGISTRY_STENCIL_ENABLE = true
SCHEMA_REGISTRY_STENCIL_URLS = http://localhost:8080/descriptors.bin
SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH = false
SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY = LONG_POLLING
```

### Run Firehose Log Sink

- Make sure that your kafka server and local HTTP server containing the descriptor is up and running.
- Run the firehose consumer through the gradlew task:
```shell
./gradlew runConsumer
```


**Note:** Sample configuration for other sinks along with some advanced configurations can be found [here](../advance/generic/)

### Running tests

```bash
# Running unit tests
$ ./gradlew test

# Run code quality checks
$ ./gradlew checkstyleMain checkstyleTest

#Cleaning the build
$ ./gradlew clean
```

## Style Guide

### Java

We conform to the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html). Maven can helpfully take care of that for you before you commit:

## Making a pull request

#### Incorporating upstream changes from master

Our preference is the use of git rebase instead of git merge. Signing commits

```bash
# Include -s flag to signoff
$ git commit -s -m "feat: my first commit"
```

#### Good practices to keep in mind

- Follow the [conventional commit](https://www.conventionalcommits.org/en/v1.0.0/) format for all commit messages.
- Fill in the description based on the default template configured when you first open the PR
- Include kind label when opening the PR
- Add WIP: to PR name if more work needs to be done prior to review
- Avoid force-pushing as it makes reviewing difficult
