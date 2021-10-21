# Configuration

This page contains reference for all the application configurations for Firehose.

## Table of Contents

* [Generic](configuration.md#generic)
* [Kafka Consumer ](configuration.md#kafka-consumer)
* [HTTP Sink](configuration.md#http-sink)
* [JDBC Sink](configuration.md#jdbc-sink)
* [Influx Sink](configuration.md#influx-sink)
* [Redis Sink](configuration.md#redis-sink)
* [Elasticsearch Sink](configuration.md#elasticsearch-sink)
* [GRPC Sink](configuration.md#grpc-sink)
* [Prometheus Sink](configuration.md#prometheus-sink)
* [MongoDB Sink](configuration.md#mongodb-sink)
* [Blob Sink](configuration.md#blob-sink)
* [Retries](configuration.md#retries)

## Generic

All sinks in Firehose requires the following variables to be set

### `KAFKA_RECORD_PARSER_MODE`

Decides whether to parse key or message \(as per your input proto\) from incoming data.

* Example value: `message`
* Type: `required`

### `SINK_TYPE`

Defines the Firehose sink type.

* Example value: `log`
* Type: `required`

### `INPUT_SCHEMA_PROTO_CLASS`

Defines the fully qualified name of the input proto class.

* Example value: `com.tests.TestMessage`
* Type: `required`

### `SCHEMA_REGISTRY_STENCIL_ENABLE`

Defines whether to enable Stencil Schema registry

* Example value: `true`
* Type: `optional`
* Default value: `false`

### `SCHEMA_REGISTRY_STENCIL_URLS`

Defines the URL of the Proto Descriptor set file in the Stencil Server

* Example value: `http://localhost:8000/v1/namespaces/quickstart/descriptors/example/versions/latest`
* Type: `optional`

### `LOG_LEVEL`

Defines the Firehose log level.

* Example value: `INFO`
* Type: `optional`
* Default value: `INFO`

## Kafka Consumer

### `SOURCE_KAFKA_BROKERS`

Defines the bootstrap server of Kafka brokers to consume from.

* Example value: `localhost:9092`
* Type: `required`

### `SOURCE_KAFKA_TOPIC`

Defines the list of Kafka topics to consume from.

* Example value: `test-topic`
* Type: `required`

### `SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS`

Defines the batch size of Kafka messages

* Example value: `705`
* Type: `optional`
* Default value: `500`

### `SOURCE_KAFKA_ASYNC_COMMIT_ENABLE`

Defines whether to enable async commit for Kafka consumer

* Example value: `false`
* Type: `optional`
* Default value: `true`

### `SOURCE_KAFKA_CONSUMER_CONFIG_SESSION_TIMEOUT_MS`

Defines the duration of session timeout in milliseconds

* Example value: `700`
* Type: `optional`
* Default value: `10000`

### `SOURCE_KAFKA_COMMIT_ONLY_CURRENT_PARTITIONS_ENABLE`

Defines whether to commit only current partitions

* Example value: `false`
* Type: `optional`
* Default value: `true`

### `SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE`

Defines whether to enable auto commit for Kafka consumer

* Example value: `true`
* Type: `optional`
* Default value: `false`

### `SOURCE_KAFKA_CONSUMER_GROUP_ID`

Defines the Kafka consumer group ID for your Firehose deployment.

* Example value: `sample-group-id`
* Type: `required`

### `SOURCE_KAFKA_POLL_TIMEOUT_MS`

Defines the duration of poll timeout for Kafka messages in milliseconds

* Example value: `80000`
* Type: `required`
* Default: `9223372036854775807`

### `SOURCE_KAFKA_CONSUMER_CONFIG_METADATA_MAX_AGE_MS`

Defines the maximum age of config metadata in milliseconds

* Example value: `700`
* Type: `optional`
* Default value: `500`

## HTTP Sink

An Http sink Firehose \(`SINK_TYPE`=`http`\) requires the following variables to be set along with Generic ones.

### `SINK_HTTP_SERVICE_URL`

The HTTP endpoint of the service to which this consumer should PUT/POST data. This can be configured as per the requirement, a constant or a dynamic one \(which extract given field values from each message and use that as the endpoint\)  
If service url is constant, messages will be sent as batches while in case of dynamic one each message will be sent as a separate request \(Since they’d be having different endpoints\).

* Example value: `http://http-service.test.io`
* Example value: `http://http-service.test.io/test-field/%%s,6` This will take the value with index 6 from proto and create the endpoint as per the template
* Type: `required`

### `SINK_HTTP_REQUEST_METHOD`

Defines the HTTP verb supported by the endpoint, Supports PUT and POST verbs as of now.

* Example value: `post`
* Type: `required`
* Default value: `put`

### `SINK_HTTP_REQUEST_TIMEOUT_MS`

Defines the connection timeout for the request in millis.

* Example value: `10000`
* Type: `required`
* Default value: `10000`

### `SINK_HTTP_MAX_CONNECTIONS`

Defines the maximum number of HTTP connections.

* Example value: `10`
* Type: `required`
* Default value: `10`

### `SINK_HTTP_RETRY_STATUS_CODE_RANGES`

Deifnes the range of HTTP status codes for which retry will be attempted.

* Example value: `400-600`
* Type: `optional`
* Default value: `400-600`

### `SINK_HTTP_DATA_FORMAT`

If set to `proto`, the log message will be sent as Protobuf byte strings. Otherwise, the log message will be deserialized into readable JSON strings.

* Example value: `JSON`
* Type: `required`
* Default value: `proto`

### `SINK_HTTP_HEADERS`

Deifnes the HTTP headers required to push the data to the above URL.

* Example value: `Authorization:auth_token, Accept:text/plain`
* Type: `optional`

### `SINK_HTTP_JSON_BODY_TEMPLATE`

Deifnes a template for creating a custom request body from the fields of a protobuf message. This should be a valid JSON itself.

* Example value: `{"test":"$.routes[0]", "$.order_number" : "xxx"}`
* Type: `optional`

### `SINK_HTTP_PARAMETER_SOURCE`

Defines the source from which the fields should be parsed. This field should be present in order to use this feature.

* Example value: `Key`
* Example value: `Message`
* Type: `optional`
* Default value: `disabled`

### `SINK_HTTP_PARAMETER_PLACEMENT`

Deifnes the fields parsed can be passed in query parameters or in headers.

* Example value: `Header`
* Example value: `Query`
* Type: `optional`

### `SINK_HTTP_PARAMETER_SCHEMA_PROTO_CLASS`

Defines the fully qualified name of the proto class which is to be used for parametrised http sink.

* Example value: `com.tests.TestMessage`
* Type: `optional`

### `INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING`

Defines the mapping of the proto fields to header/query fields in JSON format.

* Example value: `{"1":"order_number","2":"event_timestamp","3":"driver_id"}`
* Example value: `{"1":"event_timestamp","2":{"1":"customer_email"}}` Here field with index 2 is a complex data type 
* Type: `optional`

### `SINK_HTTP_OAUTH2_ENABLE`

Enable/Disable OAuth2 support for HTTP sink.

* Example value: `true`
* Type: `optional`
* Default value: `false`

### `SINK_HTTP_OAUTH2_ACCESS_TOKEN_URL`

Defines the OAuth2 Token Endpoint.

* Example value: `https://sample-oauth.my-api.com/oauth2/token`
* Type: `optional`

### `SINK_HTTP_OAUTH2_CLIENT_NAME`

Defines the OAuth2 identifier issued to the client.

* Example value: `client-name`
* Type: `optional`

### `SINK_HTTP_OAUTH2_CLIENT_SECRET`

Defines the OAuth2 secret issued for the client.

* Example value: `client-secret`
* Type: `optional`

### `SINK_HTTP_OAUTH2_SCOPE`

Space-delimited scope overrides. If scope override is not provided, no scopes will be granted to the token.

* Example value: `User:read, sys:info`
* Type: `optional`

## JDBC Sink

A JDBC sink Firehose \(`SINK_TYPE`=`jdbc`\) requires the following variables to be set along with Generic ones

### `SINK_JDBC_URL`

Deifnes the PostgresDB URL, it's usually the hostname followed by port.

* Example value: `jdbc:postgresql://localhost:5432/postgres`
* Type: `required`

### `SINK_JDBC_TABLE_NAME`

Defines the name of the table in which the data should be dumped.

* Example value: `public.customers`
* Type: `required`

### `SINK_JDBC_USERNAME`

Defines the username to connect to DB.

* Example value: `root`
* Type: `required`

### `SINK_JDBC_PASSWORD`

Defines the password to connect to DB.

* Example value: `root`
* Type: `required`

### `INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING`

Defines the mapping of fields in DB and the corresponding proto index from where the value will be extracted. This is a JSON field.

* Example value: `{"6":"customer_id","1":"service_type","5":"event_timestamp"}` Proto field value with index 1 will be stored in a column named service\_type in DB and so on
* Example value: `{"1":"event_timestamp","2":{"1":"customer_email"}}` Here field with index 2 is a complex data type 
* Type: `required`

### `SINK_JDBC_UNIQUE_KEYS`

Defines a comma-separated column names having a unique constraint on the table.

* Example value: `customer_id`
* Type: `optional`

### `SINK_JDBC_CONNECTION_POOL_TIMEOUT_MS`

Defines a database connection timeout in milliseconds. Follow this [connectionTimeout](https://github.com/brettwooldridge/HikariCP#frequently-used) for more details on using this configuration.

* Example value: `1000`
* Type: `required`
* Default value: `30000`

### `SINK_JDBC_CONNECTION_POOL_IDLE_TIMEOUT_MS`

Defines a database connection pool idle connection timeout in milliseconds. It needs to have a minimum value of `10000` else it will use the default value. Follow this [idleTimeout](https://github.com/brettwooldridge/HikariCP#frequently-used) for more details on using this configuration.

* Example value: `60000`
* Type: `required`
* Default value: `600000`

### `SINK_JDBC_CONNECTION_POOL_MIN_IDLE`

Defines the minimum number of idle connections in the pool to maintain. Follow this [minimumIdle](https://github.com/brettwooldridge/HikariCP#frequently-used) for more details on using this configuration.

* Example value: `10`
* Type: `required`
* Default value: `10`

### `SINK_JDBC_CONNECTION_POOL_MAX_SIZE`

Defines the maximum size for the database connection pool. Follow this [maximumPoolSize](https://github.com/brettwooldridge/HikariCP#frequently-used) for more details on using this configuration.

* Example value: `10`
* Type: `required`
* Default value: `10`

## Influx Sink

An Influx sink Firehose \(`SINK_TYPE`=`influxdb`\) requires the following variables to be set along with Generic ones

### `SINK_INFLUX_URL`

InfluxDB URL, it's usually the hostname followed by port.

* Example value: `http://localhost:8086`
* Type: `required`

### `SINK_INFLUX_USERNAME`

Defines the username to connect to DB.

* Example value: `root`
* Type: `required`

### `SINK_INFLUX_PASSWORD`

Defines the password to connect to DB.

* Example value: `root`
* Type: `required`

### `SINK_INFLUX_FIELD_NAME_PROTO_INDEX_MAPPING`

Defines the mapping of fields and the corresponding proto index which can be used to extract the field value from the proto message. This is a JSON field. Note that Influx keeps a single value for each unique set of tags and timestamps. If a new value comes with the same tag and timestamp from the source, it will override the existing one.

* Example value: `"2":"order_number","1":"service_type", "4":"status"`
  * Proto field value with index 2 will be stored in a field named 'order\_number' in Influx and so on\
* Type: `required`

### `SINK_INFLUX_TAG_NAME_PROTO_INDEX_MAPPING`

Defines the mapping of tags and the corresponding proto index from which the value for the tags can be obtained. If the tags contain existing fields from field name mapping it will not be overridden. They will be duplicated. If ENUMS are present then they must be added here. This is a JSON field.

* Example value: `{"6":"customer_id"}`
* Type: `optional`

### `SINK_INFLUX_PROTO_EVENT_TIMESTAMP_INDEX`

Defines the proto index of a field that can be used as the timestamp.

* Example value: `5`
* Type: `required`

### `SINK_INFLUX_DB_NAME`

Defines the InfluxDB database name where data will be dumped.

* Example value: `status`
* Type: `required`

### `SINK_INFLUX_RETENTION_POLICY`

Defines the retention policy for influx database.

* Example value: `quarterly`
* Type: `required`
* Default value: `autogen`

### `SINK_INFLUX_MEASUREMENT_NAME`

This field is used to give away the name of the measurement that needs to be used by the sink. Measurement is another name for tables and it will be auto-created if does not exist at the time Firehose pushes the data to the influx.

* Example value: `customer-booking`
* Type: `required`

## Redis Sink

A Redis sink Firehose \(`SINK_TYPE`=`redis`\) requires the following variables to be set along with Generic ones

### `SINK_REDIS_URLS`

REDIS instance hostname/IP address followed by its port.

* Example value: `localhos:6379,localhost:6380`
* Type: `required`

### `SINK_REDIS_DATA_TYPE`

To select whether you want to push your data as a HashSet or as a List.

* Example value: `Hashset`
* Type: `required`
* Default value: `List`

### `SINK_REDIS_KEY_TEMPLATE`

The string that will act as the key for each Redis entry. This key can be configured as per the requirement, a constant or can extract value from each message and use that as the Redis key.

* Example value: `Service\_%%s,1`

  This will take the value with index 1 from proto and create the Redis keys as per the template\

* Type: `required`

### `INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING`

This is the field that decides what all data will be stored in the HashSet for each message.

* Example value: `{"6":"customer_id",  "2":"order_num"}`
* Example value: `{"1":"event_timestamp","2":{"1":"customer_email"}}` Here field with index 2 is a complex data type
* Type: `required (For Hashset)`

### `SINK_REDIS_LIST_DATA_PROTO_INDEX`

This field decides what all data will be stored in the List for each message.

* Example value: `6`

  This will get the value of the field with index 6 in your proto and push that to the Redis list with the corresponding keyTemplate\

* Type: `required (For List)`

### `SINK_REDIS_TTL_TYPE`

* Example value: `DURATION`
* Type: `optional`
* Default value: `DISABLE`
* Choice of Redis TTL type.It can be:\
  * `DURATION`: After which the Key will be expired and removed from Redis \(UNIT- seconds\)\
  * `EXACT_TIME`: Precise UNIX timestamp after which the Key will be expired

### `SINK_REDIS_TTL_VALUE`

Redis TTL value in Unix Timestamp for `EXACT_TIME` TTL type, In Seconds for `DURATION` TTL type.

* Example value: `100000`
* Type: `optional`
* Default value: `0`

### `SINK_REDIS_DEPLOYMENT_TYPE`

The Redis deployment you are using. At present, we support `Standalone` and `Cluster` types.

* Example value: `Standalone`
* Type: `required`
* Default value: `Standalone`

## Elasticsearch Sink

An ES sink Firehose \(`SINK_TYPE`=`elasticsearch`\) requires the following variables to be set along with Generic ones

### `SINK_ES_CONNECTION_URLS`

Elastic search connection URL/URLs to connect. Multiple URLs could be given in a comma separated format.

* Example value: `localhost1:9200`
* Type: `required`

### `SINK_ES_INDEX_NAME`

The name of the index to which you want to write the documents. If it does not exist, it will be created.

* Example value: `sample_index`
* Type: `required`

### `SINK_ES_TYPE_NAME`

Defines the type name of the Document in Elasticsearch.

* Example value: `Customer`
* Type: `required`

### `SINK_ES_ID_FIELD`

The identifier field of the document in Elasticsearch. This should be the key of the field present in the message \(JSON or Protobuf\) and it has to be a unique, non-null field. So the value of this field in the message will be used as the ID of the document in Elasticsearch while writing the document.

* Example value: `customer_id`
* Type: `required`

### `SINK_ES_MODE_UPDATE_ONLY_ENABLE`

Elasticsearch sink can be created in 2 modes: `Upsert mode` or `UpdateOnly mode`. If this config is set:

* `TRUE`: Firehose will run on UpdateOnly mode which will only UPDATE the already existing documents in the Elasticsearch index.
* `FALSE`: Firehose will run on Upsert mode, UPDATING the existing documents and also INSERTING any new ones.
  * Example value: `TRUE`
  * Type: `required`
  * Default value: `FALSE`

### `SINK_ES_INPUT_MESSAGE_TYPE`

Indicates if the Kafka topic contains JSON or Protocol Buffer messages.

* Example value: `PROTOBUF`
* Type: `required`
* Default value: `JSON`

### `SINK_ES_PRESERVE_PROTO_FIELD_NAMES_ENABLE`

Whether or not the protobuf field names should be preserved in the Elasticsearch document. If false the fields will be converted to camel case.

* Example value: `FALSE`
* Type: `required`
* Default value: `TRUE`

### `SINK_ES_REQUEST_TIMEOUT_MS`

Defines the request timeout of the elastic search endpoint. The value specified is in milliseconds.

* Example value: `60000`
* Type: `required`
* Default value: `60000`

### `SINK_ES_SHARDS_ACTIVE_WAIT_COUNT`

Defines the number of shard copies that must be active before proceeding with the operation. This can be set to any non-negative value less than or equal to the total number of shard copies \(number of replicas + 1\).

* Example value: `1`
* Type: `required`
* Default value: `1`

### `SINK_ES_RETRY_STATUS_CODE_BLACKLIST`

List of comma-separated status codes for which Firehose should not retry in case of UPDATE ONLY mode is TRUE

* Example value: `404,400`
* Type: `optional`

### `SINK_ES_ROUTING_KEY_NAME`

Defines the proto field whose value will be used for routing documents to a particular shard in Elasticsearch. If empty, Elasticsearch uses the ID field of the doc by default.

* Example value: `service_type`
* Type: `optional`

## GRPC Sink

A GRPC sink Firehose \(`SINK_TYPE`=`grpc`\) requires the following variables to be set along with Generic ones

### `SINK_GRPC_SERVICE_HOST`

Defines the host of the GRPC service.

* Example value: `http://grpc-service.sample.io`
* Type: `required`

### `SINK_GRPC_SERVICE_PORT`

Defines the port of the GRPC service.

* Example value: `8500`
* Type: `required`

### `SINK_GRPC_METHOD_URL`

Defines the URL of the GRPC method that needs to be called.

* Example value: `com.tests.SampleServer/SomeMethod`
* Type: `required`

### `SINK_GRPC_RESPONSE_SCHEMA_PROTO_CLASS`

Defines the Proto which would be the response of the GRPC Method.

* Example value: `com.tests.SampleGrpcResponse`
* Type: `required`

## Prometheus Sink

A Prometheus sink Firehose \(`SINK_TYPE`=`prometheus`\) requires the following variables to be set along with Generic ones.

### `SINK_PROM_SERVICE_URL`

Defines the HTTP/Cortex endpoint of the service to which this consumer should POST data.

* Example value: `http://localhost:9009/api/prom/push`
* Type: `required`

### `SINK_PROM_REQUEST_TIMEOUT_MS`

Defines the connection timeout for the request in millis.

* Example value: `10000`
* Type: `required`
* Default value: `10000`

### `SINK_PROM_RETRY_STATUS_CODE_RANGES`

Defines the range of HTTP status codes for which retry will be attempted.

* Example value: `400-600`
* Type: `optional`
* Default value: `400-600`

### `SINK_PROM_REQUEST_LOG_STATUS_CODE_RANGES`

Defines the range of HTTP status codes for which the request will be logged.

* Example value: `400-499`
* Type: `optional`
* Default value: `400-499`

### `SINK_PROM_HEADERS`

Defines the HTTP headers required to push the data to the above URL.

* Example value: `Authorization:auth_token, Accept:text/plain`
* Type: `optional`

### `SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING`

The mapping of fields and the corresponding proto index which will be set as the metric name on Cortex. This is a JSON field.

* Example value: `{"2":"tip_amount","1":"feedback_ratings"}`
  * Proto field value with index 2 will be stored as metric named `tip_amount` in Cortex and so on
* Type: `required`

### `SINK_PROM_LABEL_NAME_PROTO_INDEX_MAPPING`

The mapping of proto fields to metric lables. This is a JSON field. Each metric defined in `SINK_PROM_METRIC_NAME_PROTO_INDEX_MAPPING` will have all the labels defined here.

* Example value: `{"6":"customer_id"}`
* Type: `optional`

### `SINK_PROM_WITH_EVENT_TIMESTAMP`

If set to true, metric timestamp will using event timestamp otherwise it will using timestamp when Firehose push to endpoint.

* Example value: `false`
* Type: `optional`
* Default value: `false`

### `SINK_PROM_PROTO_EVENT_TIMESTAMP_INDEX`

Defines the proto index of a field that can be used as the timestamp.

* Example value: `2`
* Type: `required (if SINK_PROM_WITH_EVENT_TIMESTAMP=true)`

\`\`

## MongoDB Sink

A MongoDB sink Firehose \(`SINK_TYPE`= `mongodb` \) requires the following variables to be set along with Generic ones

### `SINK_MONGO_CONNECTION_URLS`

MongoDB connection URL/URLs to connect. Multiple URLs could be given in a comma separated format.

* Example value: `localhost:27017`
* Type: `required`

### `SINK_MONGO_DB_NAME`

The name of the Mongo database to which you want to write the documents. If it does not exist, it will be created.

* Example value: `sample_DB`
* Type: `required`

### `SINK_MONGO_AUTH_ENABLE`

This field should be set to `true` if login authentication is enabled for the MongoDB Server.

* Example value: `true`
* Type: `optional`
* Default value: `false`

### `SINK_MONGO_AUTH_USERNAME`

The login username for session authentication to the MongoDB Server. This is a required field is `SINK_MONGO_AUTH_ENABLE` is set to `true`

* Example value: `bruce_wills`
* Type: `optional`

### `SINK_MONGO_AUTH_PASSWORD`

The login password for session authentication to the MongoDB Server. This is a required field is `SINK_MONGO_AUTH_ENABLE` is set to `true`

* Example value: `password@123`
* Type: `optional`

### `SINK_MONGO_AUTH_DB`

The name of the Mongo authentication database in which the user credentials are stored. This is a required field is `SINK_MONGO_AUTH_ENABLE` is set to `true`

* Example value: `sample_auth_DB`
* Type: `optional`

### `SINK_MONGO_COLLECTION_NAME`

Defines the name of the Mongo Collection

* Example value: `customerCollection`
* Type: `required`

### `SINK_MONGO_PRIMARY_KEY`

The identifier field of the document in MongoDB. This should be the key of a field present in the message \(JSON or Protobuf\) and it has to be a unique, non-null field. So the value of this field in the message will be copied to the `_id` field of the document in MongoDB while writing the document.

Note - If this parameter is not specified in Upsert mode  \( i.e. when the variable`SINK_MONGO_MODE_UPDATE_ONLY_ENABLE=false`\), then Mongo server will assign the default UUID to the `_id` field, and only insert operations can be performed.

Note - this variable is a required field in the case of Update-Only mode \( i.e. when the variable`SINK_MONGO_MODE_UPDATE_ONLY_ENABLE=true`\). Also, all externally-fed documents must have this key copied to the `_id` field, for update operations to execute normally.

* Example value: `customer_id`
* Type: `optional`

### `SINK_MONGO_MODE_UPDATE_ONLY_ENABLE`

MongoDB sink can be created in 2 modes: `Upsert mode` or `UpdateOnly mode`. If this config is set:

* `TRUE`: Firehose will run on UpdateOnly mode which will only UPDATE the already existing documents in the MongoDB collection.
* `FALSE`: Firehose will run on Upsert mode, UPDATING the existing documents and also INSERTING any new ones.
  * Example value: `TRUE`
  * Type: `required`
  * Default value: `FALSE`

### `SINK_MONGO_INPUT_MESSAGE_TYPE`

Indicates if the Kafka topic contains JSON or Protocol Buffer messages.

* Example value: `PROTOBUF`
* Type: `required`
* Default value: `JSON`

### `SINK_MONGO_CONNECT_TIMEOUT_MS`

Defines the connect timeout of the MongoDB endpoint. The value specified is in milliseconds.

* Example value: `60000`
* Type: `required`
* Default value: `60000`

### `SINK_MONGO_RETRY_STATUS_CODE_BLACKLIST`

List of comma-separated status codes for which Firehose should not retry in case of UPDATE ONLY mode is TRUE

* Example value: `16608,11000`
* Type: `optional`


### `SINK_MONGO_PRESERVE_PROTO_FIELD_NAMES_ENABLE`

Whether or not the protobuf field names should be preserved in the MongoDB document. If false the fields will be converted to camel case.
* Example value: `false`
* Type: `optional`
* Default: `true`



### `SINK_MONGO_SERVER_SELECT_TIMEOUT_MS`

Sets the server selection timeout in milliseconds, which defines how long the driver will wait for server selection to succeed before throwing an exception.
A value of 0 means that it will timeout immediately if no server is available. A negative value means to wait indefinitely.

* Example value: `4000`
* Type: `optional`
* Default: `30000`


## Blob Sink

A Blob sink Firehose \(`SINK_TYPE`=`blob`\) requires the following variables to be set along with Generic ones

### `SINK_BLOB_STORAGE_TYPE`

Defines the types of blob storage the destination remote file system the file will be uploaded. Currently, the only supported blob storage is `GCS` (google cloud storage).

* Example value: `GCS`
* Type: `required`

### `SINK_BLOB_LOCAL_FILE_WRITER_TYPE`

Defines the name of the writer of a file format. Currently, only `PARQUET` file format is supported.

* Example value: `PARQUET`
* Type: `required`

### `SINK_BLOB_OUTPUT_INCLUDE_KAFKA_METADATA_ENABLE`

Define configuration to enable or disable adding kafka metadata field to the output. Depends on the stucture of the output, in parquet format some metadata columns will be added.

* Example value: `true`
* Type: `required`

### `SINK_BLOB_LOCAL_DIRECTORY`

Defines directory temporary files will be created before uploaded to remote destination.

* Example value: `/tmp/firehose/objectstorage`
* Type: `optional`
* Default value: `/tmp/firehose`

### `SINK_BLOB_OUTPUT_KAFKA_METADATA_COLUMN_NAME`

Defines the kafka metadata column name. This config determines the schema changes column or field that will be added on the parquet format. When the metadata column name is not configured all metadata column or field will be added on top level.
When metadata column name is configured, all metadata column/field will be added as child field under the configured column name.

* Example value: `kafka_metadata`
* Type: `optional`

### `SINK_BLOB_LOCAL_FILE_WRITER_PARQUET_BLOCK_SIZE`

Defines the storage parquet writer block size, this config only applies on parquet writer. This configuration is only needed to be set manually when user need to control the block size for optimal file read.

* Example value: `134217728`
* Type: `optional`

### `SINK_BLOB_LOCAL_FILE_WRITER_PARQUET_PAGE_SIZE`

Define the storage parquet writer page size, this config only applies on parquet writer. This configuration is only needed to be set manually when user need to control the block size for optimal file read.

* Example value: `1048576`
* Type: `optional`

### `SINK_BLOB_LOCAL_FILE_ROTATION_DURATION_MS`

Define the maximum duration of record to be added to a single parquet file in milliseconds, after the elapsed time exceeded the configured duration, current file will be closed, a new file will be created and incoming records will be written to the new file.

* Example value: `1800000`
* Type: `optional`
* Default value: `3600000`

### `SINK_BLOB_LOCAL_FILE_ROTATION_MAX_SIZE_BYTES`

Defines the maximum size of record to be written on a single parquet file in bytes, new record will be written to new a file.

* Example value: `3600000`
* Type: `required`
* Default value: `268435456`

### `SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_FIELD_NAME`

Defines the field used as file partitioning.

* Example value: `event_timestamp`
* Type: `required`

### `SINK_BLOB_FILE_PARTITION_TIME_GRANULARITY_TYPE`

Defines time partitioning file type. Currently, the supported partitioning type are `hour`, `day`. This also affect the partitioning path of the files.

* Example value: `hour`
* Type: `required`
* Default value: `hour`

### `SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_TIMEZONE`

Defines time partitioning time zone. The date time partitioning uses local date and time value that calculated using the configured timezone.

* Example value: `UTC`
* Type: `optional`
* Default value: `UTC`

### `SINK_BLOB_FILE_PARTITION_TIME_HOUR_PREFIX`

Defines time partitioning path format for hour segment for example `${date_segment}/hr=10/${filename}`.

* Example value: `hr=`
* Type: `optional`
* Default value: `hr=`

### `SINK_BLOB_FILE_PARTITION_TIME_DATE_PREFIX`

Defines time partitioning path format for date segment for example `dt=2021-01-01/${hour_segment}/${filename}`.

* Example value: `dt=`
* Type: `optional`
* Default value: `dt=`

### `SINK_BLOB_GCS_GOOGLE_CLOUD_PROJECT_ID`

The identifier of google project ID where the google cloud storage bucket is located. Further documentation on google cloud [project id](https://cloud.google.com/resource-manager/docs/creating-managing-projects).

* Example value: `project-007`
* Type: `required`

### `SINK_BLOB_GCS_BUCKET_NAME`

The name of google cloud storage bucket. Here is further documentation of google cloud storage [bucket name](https://cloud.google.com/storage/docs/naming-buckets).

* Example value: `pricing`
* Type: `required`

### `SINK_BLOB_GCS_CREDENTIAL_PATH`

Full path of google cloud credentials file. Here is further documentation of google cloud authentication and [credentials](https://cloud.google.com/docs/authentication/getting-started).

* Example value: `/.secret/google-cloud-credentials.json`
* Type: `required`

### `SINK_BLOB_GCS_RETRY_MAX_ATTEMPTS`

Number of retry of the google cloud storage upload request when the request failed.

* Example value: `10`
* Type: `optional`
* Default value: `10`

### `SINK_BLOB_GCS_RETRY_TOTAL_TIMEOUT_MS`

Duration of retry of the google cloud storage upload in milliseconds. Google cloud storage client will keep retry the upload operation until the elapsed time since first retry exceed the configured duration.
Both of the config `SINK_BLOB_GCS_RETRY_MAX_ATTEMPTS` and `SINK_BLOB_GCS_RETRY_TOTAL_TIMEOUT_MS` can works at the same time, exception will be triggered when one of the limit is exceeded, user also need to aware of the default values.

* Example value: `60000`
* Type: `optional`
* Default value: `120000`

### `SINK_BLOB_GCS_RETRY_INITIAL_DELAY_MS"`

Initial delay for first retry in milliseconds. It is recommended to set this config at default values.

* Example value: `500`
* Type: `optional`
* Default value: `1000`

### `SINK_BLOB_GCS_RETRY_MAX_DELAY_MS"`

Maximum delay for each retry in milliseconds when delay being multiplied because of increase in retry attempts. It is recommended to set this config at default values.

* Example value: `15000`
* Type: `optional`
* Default value: `30000`

### `SINK_BLOB_GCS_RETRY_DELAY_MULTIPLIER"`

Multiplier of retry delay. The new retry delay for the subsequent operation will be recalculated for each retry. This config will cause increase of retry delay.
When this config is set to `1` means the delay will be constant. It is recommended to set this config at default values.

* Example value: `1.5`
* Type: `optional`
* Default value: `2`

### `SINK_BLOB_GCS_RETRY_INITIAL_RPC_TIMEOUT_MS"`

Initial timeout in milliseconds of RPC call for google cloud storage client. It is recommended to set this config at default values.

* Example value: `3000`
* Type: `optional`
* Default value: `5000`

### `SINK_BLOB_GCS_RETRY_RPC_MAX_TIMEOUT_MS"`

Maximum timeout in milliseconds of RPC call for google cloud storage client. It is recommended to set this config at default values.

* Example value: `10000`
* Type: `optional`
* Default value: `5000`

### `SINK_BLOB_GCS_RETRY_RPC_TIMEOUT_MULTIPLIER"`

Multiplier of google cloud storage client RPC call timeout. When this config is set to `1` means the config is multiplied. It is recommended to set this config at default values.

* Example value: `1`
* Type: `optional`
* Default value: `1`


## Bigquery Sink

A Bigquery sink Firehose \(`SINK_TYPE`=`bigquery`\) requires the following variables to be set along with Generic ones

### `SINK_BIGQUERY_GOOGLE_CLOUD_PROJECT_ID`

Contains information of google cloud project id location of the bigquery table where the records need to be inserted. Further documentation on google cloud [project id](https://cloud.google.com/resource-manager/docs/creating-managing-projects).

* Example value: `gcp-project-id`
* Type: `required`

### `SINK_BIGQUERY_TABLE_NAME`

The name of bigquery table. Here is further documentation of bigquery [table naming](https://cloud.google.com/bigquery/docs/tables).

* Example value: `user_profile`
* Type: `required`

### `SINK_BIGQUERY_DATASET_NAME`

The name of dataset that contains the bigquery table. Here is further documentation of bigquery [dataset naming](https://cloud.google.com/bigquery/docs/datasets).

* Example value: `customer`
* Type: `required`


### `SINK_BIGQUERY_DATASET_LABELS`

Labels of a bigquery dataset, key-value information separated by comma attached to the bigquery dataset. This configuration define labels that will be set to the bigquery dataset. Here is further documentation of bigquery [labels](https://cloud.google.com/bigquery/docs/labels-intro).

* Example value: `owner=data-engineering,granurality=daily`
* Type: `optional`

### `SINK_BIGQUERY_TABLE_LABELS`

Labels of a bigquery table, key-value information separated by comma attached to the bigquery table. This configuration define labels that will be set to the bigquery dataset. Here is further documentation of bigquery [labels](https://cloud.google.com/bigquery/docs/labels-intro).

* Example value: `owner=data-engineering,granurality=daily`
* Type: `optional`

### `SINK_BIGQUERY_TABLE_PARTITIONING_ENABLE`

Configuration for enable table partitioning. This config will be used for provide partitioning config when creating the bigquery table.
Bigquery table partitioning config can only be set once, on the table creation and the partitioning cannot be disabled once created. Changing this value of this config later will cause error when firehose trying to update the bigquery table.
Here is further documentation of bigquery [table partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables).

* Example value: `true`
* Type: `required`
* Default value: `false`

### `SINK_BIGQUERY_TABLE_PARTITION_KEY`

Define protobuf/bigquery field name that will be used for bigquery table partitioning. only protobuf `Timestamp` field, that later converted into bigquery `Timestamp` column that is supported as partitioning key.
Currently, this sink only support `DAY` time partitioning type.
Here is further documentation of bigquery [column time partitioning](https://cloud.google.com/bigquery/docs/creating-partitioned-tables#console).

* Example value: `event_timestamp`
* Type: `required`

### `SINK_BIGQUERY_ROW_INSERT_ID_ENABLE`

This config enables adding of ID row intended for deduplication when inserting new records into bigquery.
Here is further documentation of bigquery streaming insert [deduplication](https://cloud.google.com/bigquery/streaming-data-into-bigquery).

* Example value: `false`
* Type: `required`
* Default value: `true`

### `SINK_BIGQUERY_CREDENTIAL_PATH`

Full path of google cloud credentials file. Here is further documentation of google cloud authentication and [credentials](https://cloud.google.com/docs/authentication/getting-started).

* Example value: `/.secret/google-cloud-credentials.json`
* Type: `required`

### `SINK_BIGQUERY_METADATA_NAMESPACE`

The name of column that will be added alongside of the existing bigquery column that generated from protobuf, that column contains struct of kafka metadata of the inserted record.
When this config is not configured the metadata column will not be added to the table.

* Example value: `kafka_metadata`
* Type: `optional`

### `SINK_BIGQUERY_DATASET_LOCATION`

The geographic region name of location of bigquery dataset. Further documentation on bigquery dataset [location](https://cloud.google.com/bigquery/docs/locations#dataset_location).

* Example value: `us-central1`
* Type: `optional`
* Default value: `asia-southeast1`

### `SINK_BIGQUERY_TABLE_PARTITION_EXPIRY_MS`

The duration of bigquery table partitioning expiration in milliseconds. Fill this config with `-1` will disable the table partition expiration. Further documentation on bigquery table partition [expiration](https://cloud.google.com/bigquery/docs/managing-partitioned-tables#partition-expiration).

* Example value: `2592000000`
* Type: `optional`
* Default value: `-1`

### `SINK_BIGQUERY_CLIENT_READ_TIMEOUT_MS`

The duration of bigquery client http read timeout in milliseconds, 0 for an infinite timeout, a negative number for the default value (20000).

* Example value: `20000`
* Type: `optional`
* Default value: `-1`

### `SINK_BIGQUERY_CLIENT_CONNECT_TIMEOUT_MS`

The duration of bigquery client http connection timeout in milliseconds, 0 for an infinite timeout, a negative number for the default value (20000).

* Example value: `20000`
* Type: `optional`
* Default value: `-1`


## Retries

### `RETRY_EXPONENTIAL_BACKOFF_INITIAL_MS`

Initial expiry time in milliseconds for exponential backoff policy.

* Example value: `10`
* Type: `required`
* Default value: `10`

### `RETRY_EXPONENTIAL_BACKOFF_RATE`

Backoff rate for exponential backoff policy.

* Example value: `2`
* Type: `required`
* Default value: `2`

### `RETRY_EXPONENTIAL_BACKOFF_MAX_MS`

Maximum expiry time in milliseconds for exponential backoff policy.

* Example value: `60000`
* Type: `required`
* Default value: `60000`

