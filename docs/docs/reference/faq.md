# Frequently Asked Questions

This section is an attempt to gather some commonly asked queries about Firehose and related components and provide some
answers.

# Table of contents

- [Frequently Asked Questions](#frequently-asked-questions)
    - [Firehose Sinks](#firehose-sinks)
        - [Blob Sink](#blob-sink)
            - [What file formats are the records written into ?](#what-file-formats-are-the-records-written-into-)
            - [Does the sink support json/Avro/Parquet/CSV ?](#does-the-sink-support-jsonavroparquetcsv-)
            - [Does the sink support file compression GZIP/Snappy/lz4 ?](#does-the-sink-support-file-compression-gzipsnappylz4-)
            - [Does the sink support partitioning ?](#does-the-sink-support-partitioning-)
            - [How to configure partitioning ?](#how-to-configure-partitioning-)
            - [Does the sink support S3 or hdfs ?](#does-the-sink-support-s3-or-hdfs-)
            - [How the schema being generated ? How the data types being converted to Parquet types ?](#how-the-schema-being-generated--how-the-data-types-being-converted-to-parquet-types-)
            - [How is the folder path for the file that being created ?](#how-is-the-folder-path-for-the-file-that-being-created-)
            - [How to configure object storage bucket ?](#how-to-configure-object-storage-bucket-)
            - [What will happen when file upload failed ?](#what-will-happen-when-file-upload-failed-)
            - [What will happen when disk is full ?](#what-will-happen-when-disk-is-full-)
            - [How to implement new Object Storage provider, for example S3 ?](#how-to-implement-new-object-storage-provider-for-example-s3-)
            - [How to implement new file format ?](#how-to-implement-new-file-format-)
            - [How to commit being handled ?](#how-to-commit-being-handled-)
            - [How much Disk size recommended for the sink ?](#how-much-disk-size-recommended-for-the-sink-)
        - [BigQuery Sink](#bigquery-sink)
            - [What is the method that is being used to insert the bq rows ?](#what-is-the-method-that-is-being-used-to-insert-the-bq-rows-)
            - [Does the sink support partitioned table ?](#does-the-sink-support-partitioned-table-)
            - [How to configure partitioning ?](#how-to-configure-partitioning-)
            - [How the schema being generated ? How the data types being converted to BQ ?](#how-the-schema-being-generated--how-the-data-types-being-converted-to-bq-)
            - [Does the sink support ingestion time/ integer range partitioning?](#does-the-sink-support-ingestion-time-integer-range-partitioning)
            - [How to configure table destination ?](#how-to-configure-table-destination-)
            - [What will happen when on insertion of a record the timestamp is out of range more than 5 year in the past or 1 year in the future ?](#what-will-happen-when-on-insertion-of-a-record-the-timestamp-is-out-of-range-more-than-5-year-in-the-past-or-1-year-in-the-future-)
            - [How many records are inserted/ batched each time ?](#how-many-records-are-inserted-batched-each-time-)
            - [When is the BigQuery table schema updated ?](#when-is-the-bigquery-table-schema-updated-)
            - [Is the table automatically created ?](#is-the-table-automatically-created-)
            - [Does this sink support BigQuery table clustering configuration ?](#does-this-sink-support-bigquery-table-clustering-configuration-)
            - [Does this sink support BigQuery table labeling ?](#does-this-sink-support-bigquery-table-labeling-)
        - [Redis Sink](#redis-sink)
            - [What is the Redis version supported ?](#what-is-the-redis-version-supported-)
            - [What Data types are supported in Redis sink?](#what-data-types-are-supported-in-redis-sink)
            - [What all deployments types of Redis is supported ?](#what-all-deployments-types-of-redis-is-supported-)
            - [How to use Redis cluster for Redis sink?](#how-to-use-redis-cluster-for-redis-sink)
            - [How to specify a template for the keys ?](#how-to-specify-a-template-for-the-keys-)
            - [How to select nested fields?](#how-to-select-nested-fields)
            - [What is the behaviour on connection failures?](#what-is-the-behaviour-on-connection-failures)
            - [How can TTL be configured for the Redis keys?](#how-can-ttl-be-configured-for-the-redis-keys)
            - [Does it support deleting the keys?](#does-it-support-deleting-the-keys)
            - [What are some of the use cases of this sink?](#what-are-some-of-the-use-cases-of-this-sink)
            - [What happens if the Redis goes down?](#what-happens-if-the-redis-goes-down)
        - [JDBC Sink](#jdbc-sink)
            - [What are some of the use cases of this sink?](#what-are-some-of-the-use-cases-of-this-sink)
            - [What monitoring metrics are available for this sink?](#what-monitoring-metrics-are-available-for-this-sink)
            - [Do we need to create table/schema before pushing data via JDBC sink ?](#do-we-need-to-create-tableschema-before-pushing-data-via-jdbc-sink-)
            - [Any restriction of version of supported database?](#any-restriction-of-version-of-supported-database)
            - [How messages get mapped to the queries?](#how-messages-get-mapped-to-the-queries)
            - [How data types are handled?](#how-data-types-are-handled)
            - [How are the database connections are formed?](#how-are-the-database-connections-are-formed)
            - [How does JDBC sink handles connection pooling?](#how-does-jdbc-sink-handles-connection-pooling)
            - [When and how do the DB connections gets closed?](#when-and-how-do-the-db-connections-gets-closed)
            - [How to support a new data base which supports JDBC, e:g MySQL ?](#how-to-support-a-new-data-base-which-supports-jdbc-eg-mysql-)
            - [Any transaction, locking provision?](#any-transaction-locking-provision)
            - [Are there any chances of race conditions?](#are-there-any-chances-of-race-conditions)
        - [HTTP Sink](#http-sink)
            - [How does the payload look like?](#how-does-the-payload-look-like)
            - [Does it support DELETE calls?](#does-it-support-delete-calls)
            - [How many messages are pushed in one call?](#how-many-messages-are-pushed-in-one-call)
            - [How can I configure the number of connections?](#how-can-i-configure-the-number-of-connections)
            - [What data types of request body are supported?](#what-data-types-of-request-body-are-supported)
            - [Does it support client side load balancing?](#does-it-support-client-side-load-balancing)
            - [Which authentication methods are supported?](#which-authentication-methods-are-supported)
            - [How can I pass a particular input field as a header in request?](#how-can-i-pass-a-particular-input-field-as-a-header-in-request)
            - [How can I pass a particular input field as a query param in request?](#how-can-i-pass-a-particular-input-field-as-a-query-param-in-request)
            - [What happens if my services fails ?](#what-happens-if-my-services-fails-)
            - [How are HTTP connections handled, long lived?](#how-are-http-connections-handled-long-lived)
            - [What is logRequest config?](#what-is-logrequest-config)
            - [What is shouldRetry config?](#what-is-shouldretry-config)
            - [What happens in case of null response?](#what-happens-in-case-of-null-response)
            - [What happens in case of null status code in a non-null response?](#what-happens-in-case-of-null-status-code-in-a-non-null-response)
            - [When is a message dropped?](#when-is-a-message-dropped)
            - [What is the difference between Parameterised vs Dynamic Url ?](#what-is-the-difference-between-parameterised-vs-dynamic-url-)
            - [For parameterised header, how is the data added?](#for-parameterised-header-how-is-the-data-added)
        - [General Queries](#general-queries)
            - [Which Java versions firehose work with?](#which-java-versions-firehose-work-with)
            - [How does the execution work? Is the entire process sync or async ?](#how-does-the-execution-work-is-the-entire-process-sync-or-async-)
            - [Is there any code snippet which shows how I can produce sample message in supported data format?](#is-there-any-code-snippet-which-shows-how-i-can-produce-sample-message-in-supported-data-format)
            - [Why Protobuf ? Can it support other formats like JSON, AVRO, Thrift ?](#why-protobuf--can-it-support-other-formats-like-json-avro-thrift-)
            - [Does firehose have support for SSL on Kafka?](#does-firehose-have-support-for-ssl-on-kafka)
            - [How does  Firehose create the Kafka consumer?](#how-does--firehose-create-the-kafka-consumer)
            - [Can I change the consumer group name or reset it ? Overall, how do I handle my consumer group operations?](#can-i-change-the-consumer-group-name-or-reset-it--overall-how-do-i-handle-my-consumer-group-operations)
            - [What are the advantages of Firehose over Kafka connect ?](#what-are-the-advantages-of-firehose-over-kafka-connect-)
            - [What problems does Firehose solve?](#what-problems-does-firehose-solve)
            - [Can I do any transformations before sending data to sink, for example filtering ?](#can-i-do-any-transformations-before-sending-data-to-sink-for-example-filtering-)
            - [How to optimise parallelism based on input rate of Kafka messages? Does it depend on sink ?](#how-to-optimise-parallelism-based-on-input-rate-of-kafka-messages-does-it-depend-on-sink-)
            - [What are the retry mechanisms in firehose?](#what-are-the-retry-mechanisms-in-firehose)
            - [Which Kafka client configs are available ?](#which-kafka-client-configs-are-available-)
            - [What all data formats are supported?](#what-all-data-formats-are-supported)
            - [Can we select particular fields from the input message?](#can-we-select-particular-fields-from-the-input-message)
            - [How do I configure the Protobuf schema for the topic Firehose needs to consume?](#how-do-i-configure-the-protobuf-schema-for-the-topic-firehose-needs-to-consume)
            - [What is Stencil in context of firehose ?](#what-is-stencil-in-context-of-firehose-)
            - [Will I have any data loss if my firehose is failed ?](#will-i-have-any-data-loss-if-my-firehose-is-failed-)
            - [How does firehose handle failed messages?](#how-does-firehose-handle-failed-messages)
            - [What all metrics are produced for me to monitor ?](#what-all-metrics-are-produced-for-me-to-monitor-)
            - [What kind of delivery guarantees does Firehose provide? Is it different from what Kafka is tuned for?](#what-kind-of-delivery-guarantees-does-firehose-provide-is-it-different-from-what-kafka-is-tuned-for)
            - [What happens if my firehose is stopped and Kafka retention is for few days?](#what-happens-if-my-firehose-is-stopped-and-kafka-retention-is-for-few-days)

## Firehose Sinks

### Blob Sink

#### What file formats are the records written into ?

Files are written as Parquet file format. For more details, please
look [here](https://parquet.apache.org/documentation/latest/)

#### Does the sink support json/Avro/Parquet/CSV ?

Though the sink only supports writing data as parquet files as of the latest release, other file formats can be added as
well.

#### Does the sink support file compression GZIP/Snappy/lz4 ?

Firehose Blob Sink uses GZIP codec for data compression during the generation of Parquet files.

#### Does the sink support partitioning ?

Yes, Firehose Blob Sink supports hourly and date wise partitioning.

#### How to configure partitioning ?

Partitioning can be configured by setting the config **SINK_BLOB_FILE_PARTITION_TIME_GRANULARITY_TYPE** to HOUR for
hourly partitions or to DATE for date based partitioning. The timestamp field of the message which needs to be used for
partitioning can be configured via **SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_FIELD_NAME** config.

#### Does the sink support S3 or hdfs ?

Though the sink only supports uploading files to Google Cloud Storage(GCS) as of the latest release, other storage
providers can be added as well.

#### How the schema being generated ? How the data types being converted to Parquet types ?

Schema of the Kafka message is read from the proto class specified via configs. Stencil is used to serialize the message
byte array into a Protobuf Dynamic Message using this proto. It is then finally written to a parquet file. In case a
stencil registry is configured, the schema changes will be dynamically updated.

#### How is the folder path for the file that being created ?

The filename is generated as a random UUID string. For the local file, base directory path is taken from the config  
**SINK_BLOB_LOCAL_DIRECTORY** and by default it is set to `/tmp/firehose`. Within this directory, subdirectories  
are generated with names based on whether partitioning is enabled or not:

1. If **SINK_BLOB_FILE_PARTITION_TIME_GRANULARITY_TYPE** is set to NONE meaning partitioning is disabled, the parquet
   files created are placed in directories with name set to the topic name of the kafka messages being processed.
2. If **SINK_BLOB_FILE_PARTITION_TIME_GRANULARITY_TYPE** is set to DAY, the parquet files generated are placed in
   directories  
   with names in the format [topic-name]/[date-prefix][date], where,  
   i. [topic-name] is the topic name of the Kafka message  
   ii. [date-prefix] is a config fetched from **SINK_BLOB_FILE_PARTITION_TIME_DATE_PREFIX** and is set to `dt=` as
   default,  
   iii. [date] is the date in `yyyy-MM-dd` format extracted from a timestamp column of the Kafka message as specified
   via  
   **SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_FIELD_NAME** and converted to the timezone of  
   **SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_TIMEZONE** (default UTC)
3. If **SINK_OBJECT_STORAGE_TIME_PARTITIONING_TYPE** is set to HOUR, the parquet files generated are placed in
   directories  
   with names in the format [topic-name]/[date-prefix][date]/[hour-prefix][hour], where,  
   i. [hour-prefix] is a config fetched from **SINK_BLOB_FILE_PARTITION_TIME_HOUR_PREFIX** and is set to `hr=` as
   default,  
   ii. [hour] is the hour in `HH` format extracted from a timestamp column of the Kafka message as specified via  
   **SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_FIELD_NAME** and converted to the timezone of  
   **SINK_BLOB_FILE_PARTITION_PROTO_TIMESTAMP_TIMEZONE**(default UTC).

#### How to configure object storage bucket ?

Firehose will automatically configure the GCS bucket upon starting by reading the credentials, project id, bucket name
and other settings as configured via environment variables. Please look at the Blob Sink configs for more details.

#### What will happen when file upload failed ?

In case the file upload fails, the thread processing the file upload throws an exception which ultimately bubbles up and
causes the Firehose JVM to get terminated with an abnormal status code.

#### What will happen when disk is full ?

In case the file upload fails, an IOException is thrown which ultimately bubbles up and causes the Firehose JVM to get
terminated with an abnormal status code.

#### How to implement new Object Storage provider, for example S3 ?

Firehose exposes an interface class called *BlobStorage.java* which can be used to create new blob storage
implementations. Subsequently, the *BlobStorageFactory.java* can be modified to produce instance of this new blob
storage provider, while injecting the necessary configs. Firehose also exposes a config called as **
SINK_BLOB_STORAGE_TYPE** which is used to control which storage provider to use, with its default currently set to GCS.

#### How to implement new file format ?

Firehose exposes an interface called *LocalFileWriter.java* which can be used to create new file writer implementations.
Subsequently, the *LocalStorage.java* class can be modified to produce instance of this new file writer, while injecting
the necessary configs. Firehose also exposes a config called as **SINK_BLOB_LOCAL_FILE_WRITER_TYPE** which is used to
control which file writer type to use, with its default currently set to parquet.

#### How to commit being handled ?

ObjectStorage sink does its own offset management. After the polled kafka messages are processed by the sink and the
parquet files are uploaded to object storage and cleaned from local disk as per the policy, committable offsets are then
recorded by the sink. The offsets are then committed as per the Offset Manager. Please look at the Offset Manager class
for more details.

#### How much Disk size recommended for the sink ?

A safe disk size will depend on and can be tuned based on configs set such as the time duration since creation after
which files are pushed to the Blob Storage (**SINK_BLOB_LOCAL_FILE_ROTATION_DURATION_MS**), max parquet file size after
which it is pushed to the Blob Storage(**SINK_BLOB_LOCAL_FILE_ROTATION_MAX_SIZE_BYTES**) as well as the number of topics
that Firehose is consuming from.

### BigQuery Sink

#### What is the method that is being used to insert the bq rows ?

Rows are inserted using streaming insert.

#### Does the sink support partitioned table ?

Yes, BigQuery sink supports writing to partitioned tables as well.

#### How to configure partitioning ?

Partitioning can be enabled by setting the config **SINK_BIGQUERY_ENABLE_TABLE_PARTITIONING_ENABLE** to true. A
partitioning key can be specified via the config **SINK_BIGQUERY_TABLE_PARTITION_KEY**. The time for expiry for date in
a partition can also be configured using **SINK_BIGQUERY_TABLE_PARTITION_EXPIRY_MS**.

#### How the schema being generated ? How the data types being converted to BQ ?

The schema of the BQ table is generated as per the schema of the input Kafka message proto and is specified via **
INPUT_SCHEMA_PROTO_CLASS**. The Protobuf types are mapped to Legacy SQL types ( BigQuery types). Nested fields and lists
are mapped as type RECORD and REPEATED fields respectively. If Stencil is configured to dynamically fetch the updated
proto, the schema of the BQ table is modified accordingly as and when the input message's proto changes in the Stencil
cache. As per constraints set by BigQuery, a maximum of 15 levels of nesting can be de-serialised and mapped into
corresponding BQ data types beyond which the field values are simply serialised into strings. During the serialisation,
the top level field names in the input Kafka message are mapped to column names in the Big Query table.

#### Does the sink support ingestion time/ integer range partitioning?

No. As of now, Firehose BQ Sink only supports TimePartitioning. The only supported partition keys can be of type DATE or
TIMESTAMP.

#### How to configure table destination ?

The geographic location of the dataset can be set using **SINK_BIGQUERY_DATASET_LOCATION**. Please note that this is a
one-time only change and becomes immutable once set. Changing the config later will cause an exception to be thrown. The
name of the table can be configured by specifying the config **SINK_BIGQUERY_TABLE_NAME**.

#### What will happen when on insertion of a record the timestamp is out of range more than 5 year in the past or 1 year in the future ?

Firehose uses the
legacy [BQ Streaming API](https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/insertAll) and only allows
setting a partition key of type DATE or TIMESTAMP column. When Firehose streams data to the table, BigQuery
automatically puts the data into the correct partition, based on the values in the partition column. Records with
partition column value that is between 5 years in the past and 1 year in the future can be inserted, however data
outside this range is rejected. If **SINK_BIGQUERY_TABLE_PARTITION_EXPIRY_MS** is set, then BigQuery will delete the
data in the partition once this much time has elapsed since the creation of the partition.

#### How many records are inserted/ batched each time ?

All the records fetched in one poll from the Kafka broker are pushed in one batch to BigQuery. This can be controlled by
the config **SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS**.

#### When is the BigQuery table schema updated ?

If Stencil is configured to dynamically fetch the updated proto, the schema of the BigQuery table is modified
accordingly as and when the input message's proto changes in the Stencil cache.

#### Is the table automatically created ?

Yes, Firehose will follow an upsert based model whereby if the BQ table or the dataset doesn't exist, then it will
create them based on the set configs prior to pushing the records.

#### Does this sink support BigQuery table clustering configuration ?

No, table clustering is not yet supported as of the latest release.

#### Does this sink support BigQuery table labeling ?

Yes, both table labels and dataset labels can be specified by setting them in configs **SINK_BIGQUERY_TABLE_LABELS**
and **SINK_BIGQUERY_DATASET_LABELS** respectively. For example, **SINK_BIGQUERY_TABLE_LABELS** can be set
to `label1=value1,label2=value2`.

### Redis Sink

#### What is the Redis version supported ?

Firehose uses Jedis v3.0.1 (as of the latest release) as the redis client. At the time of writing this documentation, 
Jedis is [fully compatible](https://github.com/redis/jedis#jedis) with redis 2.8.x, 3.x.x and above.

#### What Data types are supported in Redis sink?

Redis Sink supports persisting the input Kafka messages as LIST and HASHSET data types into Redis.

#### What all deployments types of Redis is supported ?

Redis Sink currently supports Standalone and Cluster deployments of Redis.

#### How to use Redis cluster for Redis sink?

To use Redis cluster as the sink, set the following configs as follows:

1. **SINK_REDIS_URLS** to be set to the cluster node URLs separated by comma as a delimiter. For
   example, `127.0.0.1:30004,127.0.0.1:30002,127.0.0.1:30003`
2. **SINK_REDIS_DEPLOYMENT_TYPE** to be set as CLUSTER

#### How to specify a template for the keys ?

The template key can be specified using the config **SINK_REDIS_KEY_TEMPLATE**. This can be set to either :

1. A constant
2. As a template string: for example, `Service\_%%s,1`. Here, Firehose will extract the value of the incoming message
   field having protobuf index 1 and create the Redis key as per the template.

#### How to select nested fields?

Nested fields can only be selected from the incoming Kafka message when the data type to be pushed to Redis is set as
HashSet. To do so, ensure **SINK_REDIS_DATA_TYPE** config is set to HASHSET and **INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING**
config is set as a JSON indicating the mapping of input message proto field numbers to the desired field names in the
redis hash set entry. For example, consider a proto as follows:-

    message Driver {
        int32 driver_id = 1;
        PersonName driver_name = 2;
    }
    
    message PersonName {
        string fname = 1;
        string lname = 2;
    }

Now consider that each entry in the Redis is required to be a HASHSET consisting of driver id as the key and the
driver's first name and last name as the value fields with names as `first_name` and `last_name` respectively.
Accordingly, the **INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING** config can be set as follows:-

    {""2"":""{\""1\"":\""first_name\"", \""2\"":\""last_name\""}""}"

#### What is the behaviour on connection failures?

If messages failed to get pushed to Redis because of some error( either due to connection timeout or Redis server
failure), Firehose will retry sending the messages for a fixed number of attempts depending on whether a retryable
exception was thrown. If the messages still failed to get published after retries, Firehose will push these messages to
the DLQ topic if DLQ is enabled via configs. If not enabled, it will drop the messages.

#### How can TTL be configured for the Redis keys?

The TTL can be set in 2 ways :-

1. Expiring keys after some duration: Set **SINK_REDIS_TTL_TYPE** to `DURATION` and **SINK_REDIS_TTL_VALUE** to the
   duration in seconds.
2. Expiring keys at some fixed time: Set **SINK_REDIS_TTL_TYPE** to `EXACT_TIME` and **SINK_REDIS_TTL_VALUE** to
   the `UNIX` timestamp when the key needs to expire.

#### Does it support deleting the keys?

Keys can be deleted by configuring a TTL. To know how to set the TTL, please
look [here](#how-can-ttl-be-configured-for-the-redis-keys).

#### What are some of the use cases of this sink?

Some possible use cases could be:-

1. High throughput data generated by one application needs to be streamed back to another application for fast lookup
   purposes. For example, recommendations based on customer interactions.
2. Constant schema but fast changing value of an external entity as published by one application needs to be quickly
   communicated to multiple services to maintain an accurate representation of the entity in internal systems. For
   example, caching of GCM keys in push notification systems.

#### What happens if the Redis goes down?

If messages failed to get pushed to Redis because of some error( either due to connection timeout or Redis server
failure), Firehose will retry sending the messages for a fixed number of attempts depending on whether a retryable
exception was thrown. If the messages still failed to get published after retries, Firehose will push these messages to
the DLQ topic if DLQ is enabled via configs. If not enabled, it will drop the messages.

### JDBC Sink

#### What are some of the use cases of this sink?

This sink can be used for cases when one would want to run customised and frequently changing queries on the streaming
data, for joining with other data to do complex analytics or for doing transactional processing.

#### What monitoring metrics are available for this sink?

Along with the generic Firehose metrics, this sink also exposes metrics to monitor the following:-

1. Connection pool size for the JDBC connection
2. Total active and idle connections in the pool

#### Do we need to create table/schema before pushing data via JDBC sink ?

Yes. The table needs to created before Firehose can stream data to it. The table name can be specified via the config **
SINK_JDBC_TABLE_NAME**.

#### Any restriction of version of supported database?

Firehose JDBC sink works with PostgresSQL ver. 9 and above.

#### How messages get mapped to the queries?

Firehose maps an incoming Kafka message to a query based on 2 configs: **INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING** and **
SINK_JDBC_UNIQUE_KEYS**. For example, consider a proto as follows:-

    message Driver {
        int32 driver_id = 1;
        PersonName driver_name = 2;
    }
    
    message PersonName {
        string fname = 1;
        string lname = 2;
    }

Now consider that the Driver table in the database has 3 columns: `id`, `first_name` and `last_name`. Accordingly, one
can set the **INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING** config as follows:-

    {""1"":""id"", ""2"":""{\""1\"":\""first_name\"", \""2\"":\""last_name\""}""}

This will output a row in the Driver table as follows:-

| id   | first_name | last_name |
|------|------------|-----------|
| 1234 | Chuck      | Norris    |

Unique keys as per the table schema can be configured by setting the config **SINK_JDBC_UNIQUE_KEYS**. For
example: `order_number,driver_id`. When trying to push a message from Kafka to the database table, Firehose will
construct a query which first tries to do an INSERT. If there is a conflict, such as a unique key constraint violation,
the query tries to do an update of all the non-unique columns of the row with appropriate values from the message. If
still there is a conflict, the query won't do anything.

#### How data types are handled?

Firehose can insert/update rows into the DB table with columns of data type string only. For a field of type as a nested
Protobuf message, Firehose will serialise this message into a JSON string. While serialising, it will trim insignificant
whitespaces, retain the default field names as specified in the message descriptor and use the default values for the
field if no value is set. For a field of type collection of messages, the individual messages are converted into JSON
strings and then serialised into a JSON array. The array itself is then serialised into a string. For a field of type
Timestamp, Firehose will parse it into an Instant type and then serialise it as a string. For fields of all other data
types in the Kafka message, the field values are simply serialised to string.

#### How are the database connections are formed?

DB connections are created using a Hikari Connection Pool.

#### How does JDBC sink handles connection pooling?

JDBC Sink creates a Hikari ConnectionPool (based on max pool size). The connections are used and released back to the
pool while in use. When firehose shuts down, it closes the pool.

#### When and how do the DB connections gets closed?

When the main thread is interrupted, Firehose consumer is closed which in turn closes the JDBC sink which in turn closes
the pool.

#### How to support a new data base which supports JDBC, e:g MySQL ?

Adding of new DB sinks can be supported by making the necessary code changes, such as including the necessary JDBC
driver dependencies, changing the `JDBCSinkFactory.java` class to create instance of the new sink, extending and
overriding the `AbstractSink.java` class lifecycle methods to handle the processing of messages, etc.

#### Any transaction, locking provision?

As the inserts are independent of each other and depends on events, there is no locking/transaction provision.

#### Are there any chances of race conditions?

In cases where the unique key in the db is different from Kafka key, there can be cases where data from one partition
can override the data from other partition.

### HTTP Sink

#### How does the payload look like?

The payload format differs based on the configs **SINK_HTTP_DATA_FORMAT**, **SINK_HTTP_JSON_BODY_TEMPLATE**, 
**SINK_HTTP_PARAMETER_SOURCE**, **SINK_HTTP_SERVICE_URL** and **SINK_HTTP_PARAMETER_PLACEMENT**. For details on what these
configs mean, please have a look at the config section.

#### Does it support DELETE calls?

Yes, the HTTP Sink supports DELETE calls since Firehose version 0.7.3.

#### How many messages are pushed in one call?

Firehose can support batching of messages in one HTTP call based on special configurations. In such cases, the total
number of messages which are pulled from Kafka in a single fetch can be serialised into one request. This is controlled
by the config **SOURCE_KAFKA_CONSUMER_CONFIG_MAX_POLL_RECORDS**.

#### How can I configure the number of connections?

The max number of HTTP connections can be configured by specifying the config **SINK_HTTP_MAX_CONNECTIONS**.

#### What data types of request body are supported?

The request body can be sent as a JSON string in which the message proto is parsed into their respective fields in the
request body. It can also be sent in a standard JSON format as below, with each of the fields being set to the
respective encoded serialised string from the Kafka message:

        {
         "topic":"sample-topic",
         "log_key":"CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM=",
         "log_message":"CgYIyOm+xgUSBgiE6r7GBRgNIICAgIDA9/y0LigCMAM="
      }

#### Does it support client side load balancing?

Yes. The IP/hostname of the Load Balancer fronting the HTTP Sink Servers can be provided to Firehose via 
**SINK_HTTP_SERVICE_URL**. Firehose will call this endpoint when pushing messages and then the LB can take care of
distributing the load among multiple sink servers.

#### Which authentication methods are supported?

Firehose supports OAuth authentication for the HTTP Sink.

#### How can I pass a particular input field as a header in request?

1. set **SINK_HTTP_PARAMETER_SOURCE** to either `key` or `message` 
(based on whether the field one requires in the http request is part of the key or the message in the Kafka record)
2. set **SINK_HTTP_PARAMETER_PLACEMENT** to `header`, and
3. set **SINK_HTTP_PARAMETER_SCHEMA_PROTO_CLASS** to the proto class for the input Kafka message and set 
**INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING** to a json value indicating the proto field number from the input message to be 
mapped to the header name.

#### How can I pass a particular input field as a query param in request?

1. set **SINK_HTTP_PARAMETER_SOURCE** to either `key` or `message` 
(based on whether the field one requires in the http request is part of the key or the message in the Kafka record),
2. set **SINK_HTTP_PARAMETER_PLACEMENT** to `query`, and
3. set **SINK_HTTP_PARAMETER_SCHEMA_PROTO_CLASS** to the proto class for the input Kafka message and set 
**INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING** to a json value indicating the proto field number from the input message to be 
mapped to the query parameter name.

#### What happens if my services fails ?

If messages failed to get pushed to the HTTP endpoint with a retryable status code as specified via 
**SINK_HTTP_RETRY_STATUS_CODE_RANGES** config, Firehose will retry sending the messages for a fixed number of attempts. 
If the messages still failed to get published after retries, Firehose will push these messages to the DLQ topic if DLQ is
enabled via configs. If not enabled, it will drop the messages.

#### How are HTTP connections handled, long lived?

A connection pool is created with max number of connections set to **SINK_HTTP_MAX_CONNECTIONS** configuration. Firehose
doesn't support maintaining keep-alive connections and hence connections are renewed every-time the default TTL for
PoolingHttpClientConnectionManager expires.For more details,
see [here](https://hc.apache.org/httpcomponents-client-4.5.x/current/httpclient/apidocs/org/apache/http/impl/conn/PoolingHttpClientConnectionManager.html)

#### What is logRequest config?

If the response received from the http sink endpoint is null or if the status code of the response is within the range
of **SINK_HTTP_REQUEST_LOG_STATUS_CODE_RANGES** config, then the request payload is logged. e;g, `200-500`

#### What is shouldRetry config?

If the response received from the http sink endpoint is null or if the status code of the response is within the range
of **SINK_HTTP_RETRY_STATUS_CODE_RANGES** config, then the request is again retried for a configured number of attempts
with backoff. e;g, `400-500`

#### What happens in case of null response?

If the request to the sink fails with a null status code, Firehose will attempt to retry calling the endpoint for a
configured number of attempts with backoff. After that, if DLQ is enabled, the messages are pushed to DLQ queue with
backoff. If not enabled, no more attempts are made.

#### What happens in case of null status code in a non-null response?

If the request to the sink fails with a null status code, Firehose will drop the messages and not retry sending them
again.

#### When is a message dropped?

If the request to the sink fails with a retryable status code as specified via **SINK_HTTP_RETRY_STATUS_CODE_RANGES**
config, Firehose will attempt to retry calling the endpoint for a configured number of attempts with backoff. After
that, if DLQ is enabled, the messages are pushed to DLQ queue with backoff. If DLQ is disabled, messages are dropped.

#### What is the difference between Parameterised vs Dynamic Url ?

In parameterised query and parameterised header HTTP sink mode, the incoming Kafka message is converted to query
parameters and header parameters in the HTTP request respectively. In case of dynamic url HTTP sink mode, the input
Kafka message is parsed into the request body.

#### For parameterised header, how is the data added?

1. set **SINK_HTTP_PARAMETER_SOURCE** to either `key` or `message` (based on whether the field one requires in the http 
request is part of the key or the message in the Kafka record),
2. set **SINK_HTTP_PARAMETER_PLACEMENT** to `header`, and
3. set **SINK_HTTP_PARAMETER_SCHEMA_PROTO_CLASS** to the Protobuf class for the input Kafka message and 
**INPUT_SCHEMA_PROTO_TO_COLUMN_MAPPING** to a json value indicating the proto field number from the input message to be 
mapped to the header name.

### General Queries

#### Which Java versions firehose work with?

Firehose has been developed and tested to work with Java 8. Compatibility with lower or higher versions is not known and
hence, needs to be tested prior to use.

#### How does the execution work? Is the entire process sync or async ?

The execution works as follows:
* Get messages from Kafka
* Filter the messages (optional)
* Push these messages to sink: All the existing sink types follow the same contract/lifecycle defined
  in `AbstractSink.java`. It consists of two stages:
    * **Prepare**: Transformation over-filtered messages’ list to prepare the sink-specific insert/update client
      requests.
    * **Execute**: Requests created in the prepare stage are executed at this step and a list of failed
      messages is returned (if any) for retry.
* If the push to the sink fails with a retryable exception, Firehose will attempt to retry pushing the messages for a
  configured number of attempts with backoff. After that, if DLQ is enabled, the messages are pushed to DLQ queue with
  backoff. If DLQ is disabled, messages are dropped.
* Captures telemetry and success/failure events and send them to Telegraf
* Repeat the process above again

Firehose can configure its Kafka consumer to work in either sync or async mode. For more details, you can look 
[here](/docs/concepts/consumer.md)

#### Is there any code snippet which shows how I can produce sample message in supported data format?

Following is an example to demonstrate how to create a Protobuf message and then produce it to a Kafka cluster. Firstly,
create a  `.proto`  file containing all the required field names and their corresponding integer tags. Save it in a new
file named  `person.proto`

```
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

Next, compile your  `.proto`  file using Protobuf compiler i.e.  `protoc`.This will generate Person, PersonOrBuilder and
PersonProtos Java source files. Specify the source directory (where your application's source code lives – the current
directory is used if you don't provide a value), the destination directory (where you want the generated code to go;
often the same as  `$SRC_DIR`), and the path to your  `.proto`

```
protoc -I=$SRC_DIR --java_out=$DST_DIR $SRC_DIR/person.proto

```

Lastly, add the following lines in your Java code to generate a POJO (Plain Old Java Object) of the Person proto class
and serialize it to a byte array, using the  `toByteArray()`  method of
the  [com.google.protobuf.GeneratedMessageV3](https://www.javadoc.io/static/com.google.protobuf/protobuf-java/3.5.1/com/google/protobuf/GeneratedMessageV3.html)
class. The byte array is then sent to the Kafka cluster by the producer.

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

Refer  [https://developers.google.com/protocol-buffers](https://developers.google.com/protocol-buffers)  for more info
on how to create protobuf files.

#### Why Protobuf ? Can it support other formats like JSON, AVRO, Thrift ?

Protocol buffers are Google's language-neutral, platform-neutral, extensible mechanism for serialising structured data.
Data streams on Kafka topics are bound to a Protobuf schema. Protobuf is much more lightweight that other schema formats
like JSON, since it encodes the keys in the message to integers.

ElasticSearch and MongoDB sink support both JSON and Protobuf as the input schema. For other sinks, Firehose currently
supports only Protobuf. Support for JSON and Avro is planned and incorporated in the future roadmap.

#### Does firehose have support for SSL on Kafka?

No, the Kafka consumer used in Firehose doesn't support SSL.

#### How does  Firehose create the Kafka consumer?

When Firehose starts, it creates as many Kafka consumers as specified by the config **APPLICATION_THREAD_COUNT** (
default set to 1). Each consumer runs on a separate thread. Please
look [here](https://github.com/goto/firehose/blob/main/docs/reference/configuration.md#kafka-consumer) for more details
on how to configure the Kafka consumer.

#### Can I change the consumer group name or reset it ? Overall, how do I handle my consumer group operations?

Yes, the Kafka consumer group name can be changed by specifying the following configuration **SOURCE_KAFKA_CONSUMER_GROUP_ID**. The Kafka consumer in Firehose can also be reset. However, the default behaviour is
currently set to read from the latest offset. To know further details on how to tune the Kafka consumer group, you can
have a look at the Kafka consumer configurations already exposed by
Firehose [here](https://github.com/goto/firehose/blob/main/docs/reference/configuration/kafka-consumer-1.md#kafka-consumer).

#### What are the advantages of Firehose over Kafka connect ?

- **Ease of use:**  Firehose is easier to install, and using different sinks only requires changing a few
  configurations. However, Kafka Connect requires connectors to be installed across all the worker nodes within the
  cluster when used in distributed mode.
- **Filtering:**  Value-based filtering is much easier to implement as compared to Kafka Connect. Requires no additional
  plugins/schema-registry to be installed.
- **Extensible:**  Provides a comprehensible abstract sink contract making it easier to add a new sink in Firehose.
  Firehose also comes with an inbuilt serialisation/deserialisation and doesn't require any converters and serialisers
  when implementing a new sink.
- **Easy monitoring:**  Firehose provides a detailed health dashboard (Grafana) for effortless monitoring.
- **Connectors:**  Some of the Kafka connect available connectors usually have limitations. It's usually rare to find
  all the required features in a single connector and so is to find documentation for the same.
- **Fully open-source:**  Firehose is completely open-source while separation of commercial and open-source features is
  not very structured in Kafka Connect and for monitoring and advanced features, confluent control center requires an
  enterprise subscription.

#### What problems does Firehose solve?

Every micro-service needs its own sink to be developed for common operations such as streaming data from Kafka to data
lakes or other endpoints, along with real-time filtering, parsing, and monitoring of the sink.

With Firehose, you don't need to write sink code for every such microservice, or manage resources to sink data from
Kafka server to your database/service endpoint. Having provided all the configuration parameters of the sink, Firehose
will create, manage and monitor one for you. It also automatically scales to match the throughput of your data and
requires no ongoing administration.

#### Can I do any transformations before sending data to sink, for example filtering ?

Yes, Firehose provides JEXL based filters based on the fields in key or message of the Kafka record. Read
the [Filters](https://github.com/goto/firehose/blob/main/docs/concepts/filters.md) section for further details.

#### How to optimise parallelism based on input rate of Kafka messages? Does it depend on sink ?

You can increase the workers in the Firehose which will effectively multiply the number of records being processed by
Firehose. Adding some sort of filter condition in the Firehose to ignore unnecessary messages in the topic would help 
you bring down the volume of data being processed by the sink.Firehose can also be configured for its Kafka consumer to 
work in [async mode](/docs/concepts/consumer.md), thereby allowing it to do offset management and commit asynchronously 
improving performance.

#### What are the retry mechanisms in firehose?

If the push to the sink fails with a retryable error, Firehose will attempt to retry pushing the messages for a
configured number of attempts with backoff. If the messages still fail to get pushed, and if DLQ is enabled, the
messages are pushed to DLQ queue with backoff. If push fails and DLQ is disabled, messages are dropped.

Starting with version 0.2, Firehose also provides the ability to tag different error types under a specific scope: `DLQ`
, `RETRY` or `FAIL`. This enables Firehose to effectively determine at runtime as to what should be the strategy when a
particular error is encountered. For more details, please look at `ErrorConfig.java` class and classes in
the `come.gotocompany.firehose.error` package.

#### Which Kafka client configs are available ?

Firehose provides various Kafka client configurations.
Refer [Kafka Consumer Configurations](https://github.com/goto/firehose/blob/main/docs/reference/configuration/kafka-consumer-1.md)
section.

#### What all data formats are supported?

ElasticSearch, Bigquery and MongoDB sink support both JSON and Protobuf as the input schema. For other sinks, we currently support
only Protobuf. Support for JSON and Avro is planned and incorporated in the roadmap.

Protocol buffers are Google's language-neutral, platform-neutral, extensible mechanism for serialising structured data.
When `INPUT_SCHEMA_DATA_TYPE=protobuf` Data streams on Kafka topics are bound to a Protobuf schema. Follow the instructions
in  [this article](https://developers.google.com/protocol-buffers/docs/javatutorial)  on how to create, compile and
serialize a Protobuf object to send it to a binary OutputStream.
Refer  [this guide](https://developers.google.com/protocol-buffers/docs/proto3)  for detailed Protobuf syntax and rules
to create a  `.proto`  file.
When `INPUT_SCHEMA_DATA_TYPE=json` data streams on kafka topics are bound to having a valid json message.

#### Can we select particular fields from the input message?

Firehose will send all the fields of the incoming messages to the specified sink. But you can configure your sink
destination/ database to consume only the required fields.

#### How do I configure the Protobuf schema for the topic Firehose needs to consume?

Generated Protobuf Descriptors are hosted behind a Stencil server artifactory/HTTP endpoint. This endpoint URL and the
ProtoDescriptor class that the Firehose deployment should use to deserialise raw data with is configured in Firehose in
the environment variables **SCHEMA_REGISTRY_STENCIL_URLS** and **INPUT_SCHEMA_PROTO_CLASS**  respectively.

The Proto Descriptor set of the Kafka messages must be uploaded to the Stencil server.
Refer  [this guide](https://github.com/goto/stencil/blob/master/docs/guides/quick_start.md)  on how to setup and
configure the Stencil server.

#### What is Stencil in context of firehose ?

Stencil API is a dynamic schema registry for hosting and managing versions of Protobuf descriptors. The schema
handling i.e., find the mapped schema for the topic, downloading the descriptors, and dynamically being notified
of/updating with the latest schema is abstracted through the Stencil library.

The Stencil Client is a proprietary library that provides an abstraction layer, for schema handling. Schema caching,
dynamic schema updates are features of the stencil client library.

Refer  [this article](https://goto.gitbook.io/stencil/)  for further information of the features, configuration and
deployment instructions of the Stencil API. Source code of Stencil Server and Client API can be found in
its  [Github repository](https://github.com/goto/stencil).

#### Will I have any data loss if my firehose is failed ?

Firehose follows an *at-least* once policy. After a batch of messages is pushed to the sink successfully, Firehose
commits the offset before the consumer polls another batch from Kafka. Thus, failed messages are not committed.

So, when Firehose is restarted, the Kafka Consumer automatically starts pulling messages from the last committed offset
of the consumer group. No data loss occurs.

#### How does firehose handle failed messages?

If the push to the sink fails with a retryable error, Firehose will attempt to retry pushing the messages for a
configured number of attempts with backoff. If the messages still fail to get pushed, and if DLQ is enabled, the
messages are pushed to DLQ queue with backoff. If push fails and DLQ is disabled, messages are dropped.

Starting with version 0.2, Firehose also provides the ability to tag different error types under a specific scope: `DLQ`
, `RETRY` or `FAIL`. This enables Firehose to effectively determine at runtime as to what should be the strategy when a
particular error is encountered. For more details, please look at `ErrorConfig.java` class and classes in
the `com.gotocompany.firehose.error` package.

#### What all metrics are produced for me to monitor ?

Firehose exposes critical metrics to monitor the health of your delivery streams and take any necessary actions. Refer
the [Metrics](https://github.com/goto/firehose/blob/main/docs/reference/metrics.md) section for further details on each
metric.

#### What kind of delivery guarantees does Firehose provide? Is it different from what Kafka is tuned for?

Firehose provides at-least once delivery guarantee. In case of a failure or restart, the Kafka Consumer in Firehose will
automatically starts pulling messages from the last committed offset of the consumer group. Hence, no data loss occurs.

If the push to the sink fails with a retryable error, Firehose will attempt to retry pushing the messages for a
configured number of attempts with backoff. If the messages still fail to get pushed, and if DLQ is enabled, the
messages are pushed to DLQ queue with backoff. If push fails and DLQ is disabled, messages are dropped.

Starting with version 0.2, Firehose also provides the ability to tag different error types under a specific scope: `DLQ`
, `RETRY` or `FAIL`. This enables Firehose to effectively determine at runtime as to what should be the strategy when a
particular error is encountered. For more details, please look at `ErrorConfig.java` class and classes in
the `com.gotocompany.firehose.error` package.

#### What happens if my firehose is stopped and Kafka retention is for few days?

When firehose restarts, it will start reading from the last committed offset. If the message at the last committed
offset for the partition was deleted due to retention period, the following 3 cases can happen:-

1. If the config **SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET** is set to `latest`, then Firehose will start
   consuming from the latest offset for the partition.
2. If the config **SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET** is set to `earliest`, then Firehose will start
   consuming from the earliest available offset for the partition. For example, if the committed offset was 10, but the
   earliest available offset is now 21, Firehose will start reading from offset 21 for the partition. This is also the
   default behaviour in case this config is not specified at all.
3. If the config **SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET** is set to `none` or anything else, then Firehose
   will terminate with an abnormal status code.
