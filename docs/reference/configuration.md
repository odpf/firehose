# Configurations

This page contains reference for all the application configurations for Firehose.

## Table of Contents
- [Generic](#-generic)
- [HTTP Sink](#-http-sink)
- [JDBC Sink](#-jdbc-sink)
- [Influx Sink](#-influx-sink)
- [Redis Sink](#-redis-sink)
- [Elasticsearch Sink](#-elasticsearch-sink)
- [GRPC Sink](#-grpc-sink)
- [Prometheus Sink](#-prometheus-sink)
- [Standard](#-standard)

## <a name="Generic" /> Generic
A log sink firehose requires the following variables to be set

#### <a name="SOURCE_KAFKA_BROKERS" /> `SOURCE_KAFKA_BROKERS`
Deifnes the bootstrap server of kafka brokers to consume from.
* Example value: `localhost:9092`
* Type: `required`

#### <a name="SOURCE_KAFKA_TOPIC" /> `SOURCE_KAFKA_TOPIC`
Defines the list of kafka topics to consume from.
* Example value: `test-topic`
* Type: `required`

#### <a name="SOURCE_KAFKA_CONSUMER_GROUP_ID" /> `SOURCE_KAFKA_CONSUMER_GROUP_ID`
Deifnes the Kafka consumer group ID for your Firehose deployment.
* Example value: `sample-group-id`
* Type: `required`

#### <a name="KAFKA_RECORD_PARSER_MODE" /> `KAFKA_RECORD_PARSER_MODE`
Decides whether to parse key or message (as per your input proto) from incoming data.
* Example value: `message`
* Type: `required`

#### <a name="SINK_TYPE" /> `SINK_TYPE`
Deifnes the Firehose sink type.
* Example value: `log`
* Type: `required`

#### <a name="INPUT_SCHEMA_PROTO_CLASS" /> `INPUT_SCHEMA_PROTO_CLASS`
Deifnes the fully qualified name of the input proto class.
* Example value: `com.tests.TestMessage`
* Type: `required`

## <a name="HTTP Sink" /> HTTP Sink
An Http sink Firehose (`SINK_TYPE`=`http`) requires the following variables to be set along with Generic ones.

#### <a name="SINK_HTTP_SERVICE_URL" /> `SINK_HTTP_SERVICE_URL`
The HTTP endpoint of the service to which this consumer should PUT/POST data. This can be configured as per the requirement, a constant or a dynamic one (which extract given field values from each message and use that as the endpoint)<br>If service url is constant, messages will be sent as batches while in case of dynamic one each message will be sent as a separate request (Since they’d be having different endpoints).
* Example value: `http://http-service.test.io`
* Example value: `http://http-service.test.io/test-field/%%s,6` This will take the value with index 6 from proto and create the endpoint as per the template
* Type: `required`

#### <a name="SINK_HTTP_REQUEST_METHOD" /> `SINK_HTTP_REQUEST_METHOD`
Defines the HTTP verb supported by the endpoint, Supports PUT and POST verbs as of now.
* Example value: `post`
* Type: `required`
* Default value: `put`

#### <a name="SINK_HTTP_REQUEST_TIMEOUT_MS" /> `SINK_HTTP_REQUEST_TIMEOUT_MS`
Defines the connection timeout for the request in millis.
* Example value: `10000`
* Type: `required`
* Default value: `10000`

#### <a name="SINK_HTTP_MAX_CONNECTIONS" /> `SINK_HTTP_MAX_CONNECTIONS`
Defines the maximum number of HTTP connections.
* Example value: `10`
* Type: `required`
* Default value: `10`

#### <a name="SINK_HTTP_RETRY_STATUS_CODE_RANGES" /> `SINK_HTTP_RETRY_STATUS_CODE_RANGES`
Deifnes the range of HTTP status codes for which retry will be attempted.
* Example value: `400-600`
* Type: `optional`
* Default value: `400-600`

#### <a name="SINK_HTTP_DATA_FORMAT" /> `SINK_HTTP_DATA_FORMAT`
If set to `proto`, the log message will be sent as Protobuf byte strings. Otherwise, the log message will be deserialized into readable JSON strings.
* Example value: `JSON`
* Type: `required`
* Default value: `proto`

#### <a name="SINK_HTTP_HEADERS" /> `SINK_HTTP_HEADERS`
Deifnes the HTTP headers required to push the data to the above URL.
* Example value: `Authorization:auth_token, Accept:text/plain`
* Type: `optional`

#### <a name="SINK_HTTP_JSON_BODY_TEMPLATE" /> `SINK_HTTP_JSON_BODY_TEMPLATE`
Deifnes a template for creating a custom request body from the fields of a protobuf message. This should be a valid JSON itself.
* Example value: `{"test":"$.routes[0]", "$.order_number" : "xxx"}`
* Type: `optional`

#### <a name="SINK_HTTP_PARAMETER_SOURCE" /> `SINK_HTTP_PARAMETER_SOURCE`
Defines the source from which the fields should be parsed. This field should be present in order to use this feature.
* Example value: `Key`
* Example value: `Message`
* Type: `optional`
* Default value: `None`

#### <a name="SINK_HTTP_PARAMETER_PLACEMENT" /> `SINK_HTTP_PARAMETER_PLACEMENT`
Deifnes the fields parsed can be passed in query parameters or in headers.
* Example value: `Header`
* Example value: `Query`
* Type: `optional`

#### <a name="SINK_HTTP_PARAMETER_SCHEMA_PROTO_CLASS" /> `SINK_HTTP_PARAMETER_SCHEMA_PROTO_CLASS`
Defines the fully qualified name of the proto class which is to be used for parametrised http sink.
* Example value: `com.tests.TestMessage`
* Type: `optional`

#### <a name="INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING" /> `INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING`
Defines the mapping of the proto fields to header/query fields in JSON format.
* Example value: `{"1":"order_number","2":"event_timestamp","3":"driver_id"}`
* Type: `optional`

#### <a name="SINK_HTTP_OAUTH2_ENABLE" /> `SINK_HTTP_OAUTH2_ENABLE`
Enable/Disable OAuth2 support for HTTP sink.
* Example value: `true`
* Type: `optional`
* Default value: `false`

#### <a name="SINK_HTTP_OAUTH2_ACCESS_TOKEN_URL" /> `SINK_HTTP_OAUTH2_ACCESS_TOKEN_URL`
Defines the OAuth2 Token Endpoint.
* Example value: `https://sample-oauth.my-api.com/oauth2/token`
* Type: `optional`

#### <a name="SINK_HTTP_OAUTH2_CLIENT_NAME" /> `SINK_HTTP_OAUTH2_CLIENT_NAME`
Defines the OAuth2 identifier issued to the client.
* Example value: `client-name`
* Type: `optional`

#### <a name="SINK_HTTP_OAUTH2_CLIENT_SECRET" /> `SINK_HTTP_OAUTH2_CLIENT_SECRET`
Defines the OAuth2 secret issued for the client.
* Example value: `client-secret`
* Type: `optional`

#### <a name="SINK_HTTP_OAUTH2_SCOPE" /> `SINK_HTTP_OAUTH2_SCOPE`
Space-delimited scope overrides. If scope override is not provided, no scopes will be granted to the token.
* Example value: `User:read, sys:info`
* Type: `optional`

## <a name="JDBC Sink" /> JDBC Sink
A JDBC sink firehose (`SINK_TYPE`=`jdbc`) requires the following variables to be set along with Generic ones

#### <a name="SINK_JDBC_URL" /> `SINK_JDBC_URL`
Deifnes the PostgresDB URL, it's usually the hostname followed by port.
* Example value: `jdbc:postgresql://localhost:5432/postgres`
* Type: `required`

#### <a name="SINK_JDBC_TABLE_NAME" /> `SINK_JDBC_TABLE_NAME`
Defines the name of the table in which the data should be dumped.
* Example value: `public.customers`
* Type: `required`

#### <a name="SINK_JDBC_USERNAME" /> `SINK_JDBC_USERNAME`
Defines the username to connect to DB.
* Example value: `root`
* Type: `required`

#### <a name="SINK_JDBC_PASSWORD" /> `SINK_JDBC_PASSWORD`
Defines the password to connect to DB.
* Example value: `root`
* Type: `required`

#### <a name="INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING" /> `INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING`
Defines the mapping of fields in DB and the corresponding proto index from where the value will be extracted. This is a JSON field.
* Example value: `{"6":"customer_id","1":"service_type","5":"event_timestamp"}` Proto field value with index 1 will be stored in a column named service_type in DB and so on
* Type: `required`

#### <a name="SINK_JDBC_UNIQUE_KEYS" /> `SINK_JDBC_UNIQUE_KEYS`
Defines a comma-separated column names having a unique constraint on the table.
* Example value: `customer_id`
* Type: `optional`

#### <a name="SINK_JDBC_CONNECTION_POOL_TIMEOUT_MS" /> `SINK_JDBC_CONNECTION_POOL_TIMEOUT_MS`
Defines a database connection timeout in milliseconds.
* Example value: `1000`
* Type: `required`
* Default value: `1000`

#### <a name="SINK_JDBC_CONNECTION_POOL_IDLE_TIMEOUT_MS" /> `SINK_JDBC_CONNECTION_POOL_IDLE_TIMEOUT_MS`
Defines a database connection pool idle connection timeout in milliseconds.

* Example value: `60000`
* Type: `required`
* Default value: `60000`

#### <a name="SINK_JDBC_CONNECTION_POOL_MIN_IDLE" /> `SINK_JDBC_CONNECTION_POOL_MIN_IDLE`
Defines the minimum number of idle connections in the pool to maintain.
* Example value: `0`
* Type: `required`
* Default value: `0`

#### <a name="SINK_JDBC_CONNECTION_POOL_MAX_SIZE" /> `SINK_JDBC_CONNECTION_POOL_MAX_SIZE`
Defines the maximum size for the database connection pool.
* Example value: `10`
* Type: `required`
* Default value: `10`

## <a name="Influx Sink" /> Influx Sink
An Influx sink firehose (`SINK_TYPE`=`influxdb`) requires the following variables to be set along with Generic ones

#### <a name="SINK_INFLUX_URL" /> `SINK_INFLUX_URL`
InfluxDB URL, it's usually the hostname followed by port.
* Example value: `http://localhost:8086`
* Type: `required`

#### <a name="SINK_INFLUX_USERNAME" /> `SINK_INFLUX_USERNAME`
Defines the username to connect to DB.
* Example value: `root`
* Type: `required`

#### <a name="SINK_INFLUX_PASSWORD" /> `SINK_INFLUX_PASSWORD`
Defines the password to connect to DB.
* Example value: `root`
* Type: `required`

#### <a name="SINK_INFLUX_FIELD_NAME_PROTO_INDEX_MAPPING" /> `SINK_INFLUX_FIELD_NAME_PROTO_INDEX_MAPPING`
Defines the mapping of fields and the corresponding proto index which can be used to extract the field value from the proto message. This is a JSON field. Note that Influx keeps a single value for each unique set of tags and timestamps. If a new value comes with the same tag and timestamp from the source, it will override the existing one.
* Example value: `"2":"order_number","1":"service_type", "4":"status"`
	- Proto field value with index 2 will be stored in a field named 'order_number' in Influx and so on\
* Type: `required`

#### <a name="SINK_INFLUX_TAG_NAME_PROTO_INDEX_MAPPING" /> `SINK_INFLUX_TAG_NAME_PROTO_INDEX_MAPPING`
Defines the mapping of tags and the corresponding proto index from which the value for the tags can be obtained. If the tags contain existing fields from field name mapping it will not be overridden. They will be duplicated. If ENUMS are present then they must be added here. This is a JSON field.
* Example value: `{"6":"customer_id"}`
* Type: `optional`

#### <a name="SINK_INFLUX_PROTO_EVENT_TIMESTAMP_INDEX" /> `SINK_INFLUX_PROTO_EVENT_TIMESTAMP_INDEX`
Defines the proto index of a field that can be used as the timestamp.
* Example value: `5`
* Type: `required`

#### <a name="SINK_INFLUX_DB_NAME" /> `SINK_INFLUX_DB_NAME`
Defines the InfluxDB database name where data will be dumped.
* Example value: `status`
* Type: `required`

#### <a name="SINK_INFLUX_RETENTION_POLICY" /> `SINK_INFLUX_RETENTION_POLICY`
Defines the retention policy for influx database.
* Example value: `quarterly`
* Type: `required`
* Default value: `autogen`

#### <a name="SINK_INFLUX_MEASUREMENT_NAME" /> `SINK_INFLUX_MEASUREMENT_NAME`
This field is used to give away the name of the measurement that needs to be used by the sink. Measurement is another name for tables and it will be auto-created if does not exist at the time Firehose pushes the data to the influx.
* Example value: `customer-booking`
* Type: `required`

## <a name="Redis Sink" /> Redis Sink
A Redis sink firehose (`SINK_TYPE`=`redis`) requires the following variables to be set along with Generic ones

#### <a name="SINK_REDIS_URLS" /> `SINK_REDIS_URLS`
REDIS instance hostname/IP address followed by its port.
* Example value: `localhos:6379,localhost:6380`
* Type: `required`

#### <a name="SINK_REDIS_DATA_TYPE" /> `SINK_REDIS_DATA_TYPE`
To select whether you want to push your data as a HashSet or as a List.
* Example value: `Hashset`
* Type: `required`
* Default value: `List`

#### <a name="SINK_REDIS_KEY_TEMPLATE" /> `SINK_REDIS_KEY_TEMPLATE`
The string that will act as the key for each Redis entry. This key can be configured as per the requirement, a constant or can extract value from each message and use that as the Redis key.
* Example value: `Service\_%%s,1`
This will take the value with index 1 from proto and create the Redis keys as per the template\
* Type: `required`

#### <a name="INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING" /> `INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING`
This is the field that decides what all data will be stored in the HashSet for each message.
* Example value: `{"6":"customer_id",  "2":"order_num"}`
* Type: `required (For Hashset)`

#### <a name="SINK_REDIS_LIST_DATA_PROTO_INDEX" /> `SINK_REDIS_LIST_DATA_PROTO_INDEX`
This field decides what all data will be stored in the List for each message.
* Example value: `6`
This will get the value of the field with index 6 in your proto and push that to the Redis list with the corresponding keyTemplate\
* Type: `required (For List)`

#### <a name="SINK_REDIS_TTL_TYPE" /> `SINK_REDIS_TTL_TYPE`
* Example value: `DURATION`
* Type: `optional`
* Default value: `DISABLE`

* Choice of Redis TTL type.It can be:\
	- `DURATION`: After which the Key will be expired and removed from Redis (UNIT- seconds)\
	- `EXACT_TIME`: Precise UNIX timestamp after which the Key will be expired

#### <a name="SINK_REDIS_TTL_VALUE" /> `SINK_REDIS_TTL_VALUE`
Redis TTL value in Unix Timestamp for `EXACT_TIME` TTL type, In Seconds for `DURATION` TTL type.
* Example value: `100000`
* Type: `optional`
* Default value: `0`

#### <a name="SINK_REDIS_DEPLOYMENT_TYPE" /> `SINK_REDIS_DEPLOYMENT_TYPE`
The Redis deployment you are using. At present, we support `Standalone` and `Cluster` types.
* Example value: `Standalone`
* Type: `required`
* Default value: `Standalone`

## <a name="Elasticsearch Sink" /> Elasticsearch Sink
An ES sink firehose (`SINK_TYPE`=`elasticsearch`) requires the following variables to be set along with Generic ones

#### <a name="SINK_ES_CONNECTION_URLS" /> `SINK_ES_CONNECTION_URLS`
Elastic search connection URL/URLs to connect. Multiple URLs could be given in a comma separated format.
* Example value: `localhost1:9200`
* Type: `required`

#### <a name="SINK_ES_INDEX_NAME" /> `SINK_ES_INDEX_NAME`
The name of the index to which you want to write the documents. If it does not exist, it will be created.
* Example value: `sample_index`
* Type: `required`

#### <a name="SINK_ES_TYPE_NAME" /> `SINK_ES_TYPE_NAME`
Defines the type name of the Document in Elasticsearch.
* Example value: `Customer`
* Type: `required`

#### <a name="SINK_ES_ID_FIELD" /> `SINK_ES_ID_FIELD`
The identifier field of the document in Elasticsearch. This should be the key of the field present in the message (JSON or Protobuf) and it has to be a unique, non-null field. So the value of this field in the message will be used as the ID of the document in Elasticsearch while writing the document.
* Example value: `customer_id`
* Type: `required`

#### <a name="SINK_ES_MODE_UPDATE_ONLY_ENABLE" /> `SINK_ES_MODE_UPDATE_ONLY_ENABLE`
Elasticsearch sink can be created in 2 modes: `Upsert mode` or `UpdateOnly mode`. If this config is set:
    - `TRUE`: Firehose will run on UpdateOnly mode which will only UPDATE the already existing documents in the Elasticsearch index.
    - `FALSE`: Firehose will run on Upsert mode, UPDATING the existing documents and also INSERTING any new ones.
* Example value: `TRUE`
* Type: `required`
* Default value: `FALSE`

#### <a name="SINK_ES_INPUT_MESSAGE_TYPE" /> `SINK_ES_INPUT_MESSAGE_TYPE`
Indicates if the Kafka topic contains JSON or Protocol Buffer messages.
* Example value: `PROTOBUF`
* Type: `required`
* Default value: `JSON`

#### <a name="SINK_ES_PRESERVE_PROTO_FIELD_NAMES_ENABLE" /> `SINK_ES_PRESERVE_PROTO_FIELD_NAMES_ENABLE`
Whether or not the protobuf field names should be preserved in the Elasticsearch document. If false the fields will be converted to camel case.
* Example value: `FALSE`
* Type: `required`
* Default value: `TRUE`


#### <a name="SINK_ES_REQUEST_TIMEOUT_MS" /> `SINK_ES_REQUEST_TIMEOUT_MS`
Defines the request timeout of the elastic search endpoint. The value specified is in milliseconds.
* Example value: `60000`
* Type: `required`
* Default value: `60000`

#### <a name="SINK_ES_SHARDS_ACTIVE_WAIT_COUNT" /> `SINK_ES_SHARDS_ACTIVE_WAIT_COUNT`
Defines the number of shard copies that must be active before proceeding with the operation. This can be set to any non-negative value less than or equal to the total number of shard copies (number of replicas + 1).
* Example value: `1`
* Type: `required`
* Default value: `1`

#### <a name="SINK_ES_RETRY_STATUS_CODE_BLACKLIST" /> `SINK_ES_RETRY_STATUS_CODE_BLACKLIST`
List of comma-separated status codes for which firehose should not retry in case of UPDATE ONLY mode is TRUE
* Example value: `404,400`
* Type: `optional`

#### <a name="SINK_ES_ROUTING_KEY_NAME" /> `SINK_ES_ROUTING_KEY_NAME`
Defines the proto field whose value will be used for routing documents to a particular shard in Elasticsearch. If empty, Elasticsearch uses the ID field of the doc by default.
* Example value: `service_type`
* Type: `optional`

## <a name="GRPC Sink" /> GRPC Sink
A GRPC sink firehose (`SINK_TYPE`=`grpc`) requires the following variables to be set along with Generic ones

#### <a name="SINK_GRPC_SERVICE_HOST" /> `SINK_GRPC_SERVICE_HOST`
Defines the host of the GRPC service.
* Example value: `http://grpc-service.sample.io`
* Type: `required`

#### <a name="SINK_GRPC_SERVICE_PORT" /> `SINK_GRPC_SERVICE_PORT`
Defines the port of the GRPC service.
* Example value: `8500`
* Type: `required`

#### <a name="SINK_GRPC_METHOD_URL" /> `SINK_GRPC_METHOD_URL`
Defines the URL of the GRPC method that needs to be called.
* Example value: `com.tests.SampleServer/SomeMethod`
* Type: `required`

#### <a name="SINK_GRPC_RESPONSE_SCHEMA_PROTO_CLASS" /> `SINK_GRPC_RESPONSE_SCHEMA_PROTO_CLASS`
Defines the Proto which would be the response of the GRPC Method.
* Example value: `com.tests.SampleGrpcResponse`
* Type: `required`

## <a name="Prometheus Sink" /> Prometheus Sink
A Prometheus sink firehose (`SINK_TYPE`=`prometheus`) requires the following variables to be set along with Generic ones.

#### <a name="SINK_PROM_SERVICE_URL" /> `SINK_PROM_SERVICE_URL`
Defines the HTTP/Cortex endpoint of the service to which this consumer should POST data.
* Example value: `http://localhost:9009/api/prom/push`
* Type: `required`

#### <a name="SINK_PROM_REQUEST_TIMEOUT_MS" /> `SINK_PROM_REQUEST_TIMEOUT_MS`
Defines the connection timeout for the request in millis.
* Example value: `10000`
* Type: `required`
* Default value: `10000`

#### <a name="SINK_PROM_RETRY_STATUS_CODE_RANGES" /> `SINK_PROM_RETRY_STATUS_CODE_RANGES`
Defines the range of HTTP status codes for which retry will be attempted.
* Example value: `400-600`
* Type: `optional`
* Default value: `400-600`

#### <a name="SINK_PROM_REQUEST_LOG_STATUS_CODE_RANGES" /> `SINK_PROM_REQUEST_LOG_STATUS_CODE_RANGES`
Defines the range of HTTP status codes for which retry will be attempted.
* Example value: `400-499`
* Type: `optional`
* Default value: `400-499`

#### <a name="SINK_PROM_HEADERS" /> `SINK_PROM_HEADERS`
Defines the HTTP headers required to push the data to the above URL.
* Example value: `Authorization:auth_token, Accept:text/plain`
* Type: `optional`

#### <a name="SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING" /> `SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING`
The mapping of fields and the corresponding proto index which will be set as the metric name on Cortex. This is a JSON field.
* Example value: `{"2":"tip_amount","1":"feedback_ratings"}`
	- Proto field value with index 2 will be stored as metric named `tip_amount` in Cortex and so on
* Type: `required`

#### <a name="SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING" /> `SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING`
The mapping of fields and the corresponding proto index which will be set as the label name on Cortex. This is a JSON field.
* Example value: `{"6":"customer_id"}`
* Type: `optional`

#### <a name="SINK_PROM_WITH_EVENT_TIMESTAMP" /> `SINK_PROM_WITH_EVENT_TIMESTAMP`
If set to true, metric timestamp will using event timestamp otherwise it will using timestamp when firehose push to endpoint.
* Example value: `false`
* Type: `optional`
* Default value: `false`

#### <a name="SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX" /> `SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX`
Defines the proto index of a field that can be used as the timestamp.
* Example value: `2`
* Type: `required (if SINK_PROM_WITH_EVENT_TIMESTAMP=true)`

## <a name="Standard" /> Standard

#### <a name="SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS" /> `SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS`
Defines the maximum number of records, the consumer will fetch from Kafka in one request.
* Example value: `500`
* Type: `required`
* Default value: `500`

#### <a name="RETRY_EXPONENTIAL_BACKOFF_INITIAL_MS" /> `RETRY_EXPONENTIAL_BACKOFF_INITIAL_MS`
Initial expiry time in milliseconds for exponential backoff policy.
* Example value: `10`
* Type: `required`
* Default value: `10`

#### <a name="RETRY_EXPONENTIAL_BACKOFF_RATE" /> `RETRY_EXPONENTIAL_BACKOFF_RATE`
Backoff rate for exponential backoff policy.
* Example value: `2`
* Type: `required`
* Default value: `2`

#### <a name="RETRY_EXPONENTIAL_BACKOFF_MAX_MS" /> `RETRY_EXPONENTIAL_BACKOFF_MAX_MS`
Maximum expiry time in milliseconds for exponential backoff policy.
* Example value: `60000`
* Type: `required`
* Default value: `60000`
