# FAQs

## What problems does Firehose solve?

Every micro-service needs its own sink to be developed for such common operations as streaming data from Kafka to data lakes or other endpoints, along with real-time filtering, parsing, and monitoring of the sink.

With Firehose, you don't need to write sink code for every such microservice, or manage resources to sink data from Kafka server to your database/service endpoint. Having provided all the configuration parameters of the sink, Firehose will create, manage and monitor one for you. It also automatically scales to match the throughput of your data and requires no ongoing administration.

## Which Java versions does Firehose work with?

Firehose has been built and tested to work with Java SE Development Kit 1.8.

## How does the execution work?

Firehose has the capability to run parallelly on threads. Each thread does the following:

- Get messages from Kafka
- Filter the messages \(optional\)
- Push these messages to sink
- All the existing sink types follow the same contract/lifecycle defined in `AbstractSink.java`. It consists of two stages:
  - Prepare: Transformation over-filtered messages’ list to prepare the sink-specific insert/update client requests.
  - Execute: Requests created in the Prepare stage are executed at this step and a list of failed messages is returned \(if any\) for retry.
- In case push fails and DLQ is:
  - enabled: Firehose keeps on retrying for the configured number of attempts before the messages got pushed to DLQ Kafka topic
  - disabled: Firehose keeps on retrying until it receives a success code
- Captures telemetry and success/failure events and send them to Telegraf
- Repeat the process

## Can I do any transformations\(for example filter\) before sending the data to sink?

Yes, Firehose provides JEXL based filters based on the fields in key or message of the Kafka record. Read the [Filters](../concepts/filters.md) section for further details.

## How to optimize parallelism based on input rate of Kafka messages?

You can increase the workers in the Firehose which will effectively multiply the number of records being processed by Firehose. However, please be mindful of the fact that your sink also needs to be able to process this higher volume of data being pushed to it. Because if it is not, then this will only compound the problem of increasing lag.

Adding some sort of a filter condition in the Firehose to ignore unnecessary messages in the topic would help you bring down the volume of data being processed by the sink.

## What is the retry mechanism in Firehose? What kind of retry strategies are supported ?

In case push fails and DLQ \(Dead Letter Queue\) is:

- enabled: Firehose keeps on retrying for the configured number of attempts before the messages got pushed to DLQ Kafka topic
- disabled: Firehose keeps on retrying until it receives a success code

## Which Kafka Client configs are available ?

Firehose provides various Kafka client configurations. Refer [Generic Configurations](../advance/generic) section for details on configuration related to Kafka Consumer.

## What all data formats are supported ?

Elasticsearch , Bigquery and MongoDB sink support both JSON and Protobuf as the input schema. For other sinks, we currently support only Protobuf. Support for JSON and Avro is planned and incorporated in our roadmap. Please refer to our Roadmap section for more details.

Protocol buffers are Google's language-neutral, platform-neutral, extensible mechanism for serializing structured data. Data streams on Kafka topics are bound to a Protobuf schema.

Follow the instructions in [this article](https://developers.google.com/protocol-buffers/docs/javatutorial) on how to create, compile and serialize a Protobuf object to send it to a binary OutputStream. Refer [this guide](https://developers.google.com/protocol-buffers/docs/proto3) for detailed Protobuf syntax and rules to create a `.proto` file

## Is there any code snippet which shows how i can produce sample message in supported data format ?

Following is an example to demonstrate how to create a Protobuf message and then produce it to a Kafka cluster. Firstly, create a `.proto` file containing all the required field names and their corresponding integer tags. Save it in a new file named `person.proto`

```text
syntax = "proto2";

package tutorial;

option java_multiple_files = true;
option java_package = "com.example.tutorial.protos";
option java_outer_classname = "PersonProtos";

message Person {
  optional string name = 1;
  optional int32 id = 2;
  optional string email = 3;

  enum PhoneType {
    MOBILE = 0;
    HOME = 1;
    WORK = 2;
  }

  message PhoneNumber {
    optional string number = 1;
    optional PhoneType type = 2 [default = HOME];
  }

  repeated PhoneNumber phones = 4;
}
```

Next, compile your `.proto` file using Protobuf compiler i.e. `protoc`.This will generate Person ,PersonOrBuilder and PersonProtos Java source files. Specify the source directory \(where your application's source code lives – the current directory is used if you don't provide a value\), the destination directory \(where you want the generated code to go; often the same as `$SRC_DIR`\), and the path to your `.proto`

```text
protoc -I=$SRC_DIR --java_out=$DST_DIR $SRC_DIR/person.proto
```

Lastly, add the following lines in your Java code to generate a POJO \(Plain Old Java Object\) of the Person proto class and serialize it to a byte array, using the `toByteArray()` method of the [com.google.protobuf.GeneratedMessageV3 ](https://www.javadoc.io/static/com.google.protobuf/protobuf-java/3.5.1/com/google/protobuf/GeneratedMessageV3.html) class. The byte array is then sent to the Kafka cluster by the producer.

```java
KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties);

Person john = Person.newBuilder()
                .setId(87182872)
                .setName("John Doe")
                .setEmail("jdoe@example.com")
                .addPhones(
                        Person.PhoneNumber.newBuilder()
                                .setNumber("555-4321")
                                .setType(Person.PhoneType.HOME))
                .build();

producer.send(new ProducerRecord<byte[], byte[]>(topicName, john.toByteArray()));
```

Refer [https://developers.google.com/protocol-buffers](https://developers.google.com/protocol-buffers) for more info on how to create protobufs.

## Can we select particular fields from the incoming message ?

Firehose will send all the fields of the incoming messages to the specified sink. But you can configure your sink destination/ database to consume only the required fields.

## How can I handle consumer lag ?

- When it comes to decreasing the topic lag, it often helps to have the environment variable - [`SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS`](../advance/generic.md#source_kafka_consumer_config_max_poll_records) to be increased from the default of 500 to something higher which will tell the Kafka Consumer to consume more messages in a single poll.
- Additionally, you can increase the workers in the Firehose which will effectively multiply the number of records being processed by Firehose.
- Alternatively, if your underlying sink is not able to handle increased \(or default\) volume of data being pushed to it, adding some sort of a filter condition in the Firehose to ignore unnecessary messages in the topic would help you bring down the volume of data being processed by the sink.

## What is Stencil in context of Firehose ?

Stencil API is a dynamic schema registry for hosting and managing versions of Protobuf descriptors. The schema handling i.e., find the mapped schema for the topic, downloading the descriptors, and dynamically being notified of/updating with the latest schema is abstracted through the Stencil library.

The Stencil Client is a proprietary library that provides an abstraction layer, for schema handling. Schema Caching, dynamic schema updates are features of the stencil client library.

Refer [this article](https://goto.gitbook.io/stencil/) for further information of the features, configuration and deployment instructions of the Stencil API. Source code of Stencil Server and Client API can be found in its [Github repository](https://github.com/goto/stencil).

## How do I configure Protobuf needed to consume ?

Generated Protobuf Descriptors are hosted behind an Stencil server artifactory/HTTP endpoint. This endpoint URL and the ProtoDescriptor class that the Firehose deployment should use to deserialize raw data with is configured in Firehose in the environment variables`SCHEMA_REGISTRY_STENCIL_URLS`and`INPUT_SCHEMA_PROTO_CLASS` respectively .

The Proto Descriptor Set of the Kafka messages must be uploaded to the Stencil server. Refer [this guide](https://github.com/goto/stencil/tree/master/server#readme) on how to setup and configure the Stencil server.

## Can we select particular fields from the input message ?

No, all fields from the input key/message will be sent by Firehose to the Sink. But you can configure your service endpoint or database to consume only those fields which are required.

## Why Protobuf ? Can it support other formats like JSON and Avro ?

Protocol buffers are Google's language-neutral, platform-neutral, extensible mechanism for serializing structured data. Data streams on Kafka topics are bound to a Protobuf schema. Protobuf is much more lightweight that other schema formats like JSON, since it encodes the keys in the message to integers.

Elasticsearch, Bigquery and MongoDB sink support both JSON and Protobuf as the input schema.

For other sinks, we currently support only Protobuf. Support for JSON and Avro is planned and incorporated in our roadmap. Please refer to our Roadmap section for more details.

## Will I have any data loss if my Firehose fails ?

After a batch of messages is sent successfully, Firehose commits the offset before the consumer polls another batch from Kafka. Thus, failed messages are not committed.

So, when Firehose is restarted, the Kafka Consumer automatically starts pulling messages from the last committed offset of the consumer group. So, no data loss occurs when an instance of Firehose fails.

## How does Firehose handle failed messages ?

In case push fails and DLQ \(Dead Letter Queue\) is:

- enabled: Firehose keeps on retrying for the configured number of attempts before the messages got pushed to DLQ Kafka topic
- disabled: Firehose keeps on retrying until it receives a success code

## How does commits for Kafka consumer works ?

After the messages are pulled successfully, Firehose commits the offset to the Kafka cluster.

If`SOURCE_KAFKA_ASYNC_COMMIT_ENABLE` is set to `true`then the KafkaConsumer commits the offset asynchronously and logs to the metric `SOURCE_KAFKA_MESSAGES_COMMIT_TOTAL` incrementing the counter `FAILURE_TAG` or `SUCCESS_TAG` depending on whether the commit was a success / failure.

If`SOURCE_KAFKA_ASYNC_COMMIT_ENABLE` is set to `false`then the KafkaConsumer commits the offset synchronously and execution is blocked until the commit either succeeds or throws an exception.

## What all metrics are available to monitor the Kafka consumer?

Firehose exposes critical metrics to monitor the health of your delivery streams and take any necessary actions. Refer the [Metrics](metrics.md) section for further details on each metric.

## What happens if my Firehose gets restarted?

When Firehose is restarted, the Kafka Consumer automatically starts pulling messages from the last committed offset of the consumer group specified by the variable `SOURCE_KAFKA_CONSUMER_GROUP_ID`

## How to configure the filter for a proto field based on some data?

The environment variables `FILTER_DATA_SOURCE` , `FILTER_JEXL_EXPRESSION` and `FILTER_SCHEMA_PROTO_CLASS` need to be set for filters to work. The required filters need to be written in JEXL expression format. Refer [Using Filters](../guides/json-based-filters.md) section for more details.

## Can I perform basic arithmetic operations in filters?

Yes, you can combine multiple fields of the key/message protobuf in a single JEXL expression and perform any arithmetic or logical operations between them. e.g - `sampleKey.getTime().getSeconds() * 1000 + sampleKey.getTime().getMillis() > 22809`

## Does log sink work for any complex data type e.g. array?

Log Sink uses Logback and SL4J lobrary for logging to standard output. Thus, it'll be able to log any complex data type by printing the String returned by the toString\(\) method of the object. Log sink will also work for arrays and be able to print all the array elements in comma-separated format, e.g. `[4, 3, 8]`

## What are the use-cases of log sink?

Firehose provides a log sink to make it easy to consume messages in [standard output](https://en.wikipedia.org/wiki/Standard_streams#Standard_output_%28stdout%29). Log sink can be used for debugging purposes and experimenting with various filters. It can also be used to test the latency and overall performance of the Firehose.
