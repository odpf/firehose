# Creating Firehose

This page contains how-to guides for creating Firehose with different sinks along with their features.

## Create a Log Sink

Firehose provides a log sink to make it easy to consume messages in [standard output](https://en.wikipedia.org/wiki/Standard_streams#Standard_output_%28stdout%29). A log sink firehose requires the following [variables](../advance/generic.md) to be set. Firehose log sink can work in key as well as message parsing mode configured through [`KAFKA_RECORD_PARSER_MODE`](../advance/generic.md#kafka_record_parser_mode)

An example log sink configurations:

```text
SOURCE_KAFKA_BROKERS=localhost:9092
SOURCE_KAFKA_TOPIC=test-topic
KAFKA_RECOED_CONSUMER_GROUP_ID=sample-group-id
KAFKA_RECORD_PARSER_MODE=message
SINK_TYPE=log
INPUT_SCHEMA_DATA_TYPE=protobuf
INPUT_SCHEMA_PROTO_CLASS=com.tests.TestMessage
```

Sample output of a Firehose log sink:

```text
2021-03-29T08:43:05,998Z [pool-2-thread-1] INFO  com.gotocompany.firehose.Consumer- Execution successful for 1 records
2021-03-29T08:43:06,246Z [pool-2-thread-1] INFO  com.gotocompany.firehose.Consumer - Pulled 1 messages
2021-03-29T08:43:06,246Z [pool-2-thread-1] INFO  com.gotocompany.firehose.sink.log.LogSink -
================= DATA =======================
sample_field: 81179979
sample_field_2: 9897987987
event_timestamp {
  seconds: 1617007385
  nanos: 964581040
}
```

## Define generic configurations

- These are the configurations that remain common across all the Sink Types.
- You don’t need to modify them necessarily, It is recommended to use them with the default values. More details [here](../advance/generic#standard).

## Create an HTTP Sink

Firehose [HTTP](https://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol) sink allows users to read data from Kafka and write to an HTTP endpoint. it requires the following [variables](../sinks/http-sink.md#http-sink) to be set. You need to create your own HTTP endpoint so that the Firehose can send data to it.

Note: HTTP sink type is deprecated from Firehose version 0.8.11 onwards. Please consider using HTTPV2 sink type instead.

### Supported methods

Firehose supports `PUT` and `POST` verbs in its HTTP sink. The method can be configured using [`SINK_HTTP_REQUEST_METHOD`](../sinks/http-sink.md#sink_http_request_method).

### Authentication

Firehose HTTP sink supports [OAuth](https://en.wikipedia.org/wiki/OAuth) authentication. OAuth can be enabled for the HTTP sink by setting [`SINK_HTTP_OAUTH2_ENABLE`](../sinks/http-sink.md#sink_http_oauth2_enable)

```text
SINK_HTTP_OAUTH2_ACCESS_TOKEN_URL: https://sample-oauth.my-api.com/oauth2/token  # OAuth2 Token Endpoint.
SINK_HTTP_OAUTH2_CLIENT_NAME: client-name  # OAuth2 identifier issued to the client.
SINK_HTTP_OAUTH2_CLIENT_SECRET: client-secret # OAuth2 secret issued for the client.
SINK_HTTP_OAUTH2_SCOPE: User:read, sys:info  # Space-delimited scope overrides.
```

### Retries

Firehose allows for retrying to sink messages in case of failure of HTTP service. The HTTP error code ranges to retry can be configured with [`SINK_HTTP_RETRY_STATUS_CODE_RANGES`](../sinks/http-sink.md#sink_http_retry_status_code_ranges). HTTP request timeout can be configured with [`SINK_HTTP_REQUEST_TIMEOUT_MS`](../sinks/http-sink.md#sink_http_request_timeout_ms)

### Templating

Firehose HTTP sink supports payload templating using [`SINK_HTTP_JSON_BODY_TEMPLATE`](../sinks/http-sink.md#sink_http_json_body_template) configuration. It uses [JsonPath](https://github.com/json-path/JsonPath) for creating Templates which is a DSL for basic JSON parsing. Playground for this: [https://jsonpath.com/](https://jsonpath.com/), where users can play around with a given JSON to extract out the elements as required and validate the `jsonpath`. The template works only when the output data format [`SINK_HTTP_DATA_FORMAT`](../sinks/http-sink.md#sink_http_data_format) is JSON.

_**Creating Templates:**_

This is really simple. Find the paths you need to extract using the JSON path. Create a valid JSON template with the static field names + the paths that need to extract. \(Paths name starts with $.\). Firehose will simply replace the paths with the actual data in the path of the message accordingly. Paths can also be used on keys, but be careful that the element in the key must be a string data type.

One sample configuration\(On XYZ proto\) : `{"test":"$.routes[0]", "$.order_number" : "xxx"}` If you want to dump the entire JSON as it is in the backend, use `"$._all_"` as a path.

Limitations:

- Works when the input DATA TYPE is a protobuf, not a JSON.
- Supports only on messages, not keys.
- validation on the level of valid JSON template. But after data has been replaced the resulting string may or may not be a valid JSON. Users must do proper testing/validation from the service side.
- If selecting fields from complex data types like repeated/messages/map of proto, the user must do filtering based first as selecting a field that does not exist would fail.

## Create a JDBC sink

- Supports only PostgresDB as of now.
- Data read from Kafka is written to the PostgresDB database and it requires the following [variables](../sinks/jdbc-sink.md#jdbc-sink) to be set.

_**Note: Schema \(Table, Columns, and Any Constraints\) being used in firehose configuration must exist in the Database already.**_

## Create an InfluxDB sink

- Data read from Kafka is written to the InfluxDB time-series database and it requires the following [variables](../sinks/influxdb-sink.md#influx-sink) to be set.

_**Note:**_ [_**DATABASE**_](../sinks/influxdb-sink.md#sink_influx_db_name) _**and**_ [_**RETENTION POLICY**_](../sinks/influxdb-sink.md#sink_influx_retention_policy) _**being used in firehose configuration must exist already in the Influx, It’s outside the scope of a firehose and won’t be generated automatically.**_

## Create a Redis sink

- it requires the following [variables](../sinks/redis-sink.md) to be set.
- Redis sink can be created in 2 different modes based on the value of [`SINK_REDIS_DATA_TYPE`](../sinks/redis-sink.md#sink_redis_data_type): HashSet or List
  - `Hashset`: For each message, an entry of the format `key : field : value` is generated and pushed to Redis. field and value are generated on the basis of the config [`INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING`](../sinks/redis-sink.md#-input_schema_proto_to_column_mapping-2)
  - `List`: For each message entry of the format `key : value` is generated and pushed to Redis. Value is fetched for the proto index provided in the config [`SINK_REDIS_LIST_DATA_PROTO_INDEX`](../sinks/redis-sink.md#sink_redis_list_data_proto_index)
- The `key` is picked up from a field in the message itself.
- Redis sink also supports different [Deployment Types](../sinks/redis-sink.md#sink_redis_deployment_type) `Standalone` and `Cluster`.
- Limitation: Firehose Redis sink only supports HashSet and List entries as of now.

## Create an Elasticsearch sink

- it requires the following [variables](../sinks/elasticsearch-sink.md) to be set.
- In the Elasticsearch sink, each message is converted into a document in the specified index with the Document type and ID as specified by the user.
- Elasticsearch sink supports reading messages in both JSON and Protobuf formats.
- Using [Routing Key](../sinks/elasticsearch-sink.md#sink_es_routing_key_name) one can route documents to a particular shard in Elasticsearch.

## Create a GRPC sink

- Data read from Kafka is written to a GRPC endpoint and it requires the following [variables](../sinks/grpc-sink.md) to be set.
- You need to create your own GRPC endpoint so that the Firehose can send data to it. The response proto should have a field “success” with value as true or false.

## Create an MongoDB sink

- it requires the following [variables](../sinks/mongo-sink.md) to be set.
- In the MongoDB sink, each message is converted into a BSON Document and then inserted/updated/upserted into the specified Mongo Collection
- MongoDB sink supports reading messages in both JSON and Protobuf formats.

## Create a Blob sink

- it requires the following [variables](../sinks/blob-sink.md) to be set.
- Only support google cloud storage for now.
- Only support writing protobuf message to apache parquet file format for now.
- The protobuf message need to have a `google.protobuf.Timestamp` field as partitioning timestamp, `event_timestamp` field is usually being used.
- Google cloud credential with some google cloud storage permission is required to run this sink.

## Create a Bigquery sink

- it requires the following [variables](../sinks/bigquery-sink.md) to be set.
- For INPUT_SCHEMA_DATA_TYPE = protobuf, this sink will generate bigquery schema from protobuf message schema and update bigquery table with the latest generated schema.
   - The protobuf message of a `google.protobuf.Timestamp` field might be needed when table partitioning is enabled.
- For INPUT_SCHEMA_DATA_TYPE = json, this sink will generate bigquery schema by infering incoming json. In future we will add support for json schema as well coming from stencil.
   - The timestamp column is needed incase of partition table. It can be generated at the time of ingestion by setting the config. Please refer to config `SINK_BIGQUERY_ADD_EVENT_TIMESTAMP_ENABLE` in [depot bigquery sink config section](https://github.com/goto/depot/blob/main/docs/reference/configuration/bigquery-sink.md#sink_bigquery_add_event_timestamp_enable)
- Google cloud credential with some bigquery permission is required to run this sink.

## Create a Bigtable sink

- it requires the following environment  [variables](https://github.com/goto/depot/blob/main/docs/reference/configuration/bigtable.md) ,which are required by Depot library, to be set along with the generic firehose variables.

## Create an HTTPV2 Sink

- HttpV2 Sink is implemented in Firehose using the Http sink connector implementation in Depot library. For details on all the features supported by HttpV2 Sink, please refer the Depot documentation [here](https://github.com/goto/depot/blob/main/docs/sinks/http.md).
- it requires the following environment  [variables](https://github.com/goto/depot/blob/main/docs/reference/configuration/http.md) ,which are required by Depot library, to be set along with the generic firehose variables.

If you'd like to connect to a sink which is not yet supported, you can create a new sink by following the [contribution guidelines](../contribute/contribution.md)


