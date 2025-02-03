# DLQ

DLQ storage can be configured for certain errors thrown by sink.

## `DLQ_SINK_ENABLE`

* Example value: `true`
* Type: `optional`
* Default value: `false`

## `DLQ_WRITER_TYPE`

DLQ Writer to be configured. The possible values are, `KAFKA,BLOB_STORAGE,LOG`

* Example value: `BLOB_STORAGE`
* Type: `optional`
* Default value: `LOG`

## `DLQ_RETRY_MAX_ATTEMPTS`

Max attempts to retry for dlq.

* Example value: `3`
* Type: `optional`
* Default value: `2147483647`

## `DLQ_RETRY_FAIL_AFTER_MAX_ATTEMPT_ENABLE`

* Example value: `true`
* Type: `optional`
* Default value: `false`

## `DLQ_BLOB_STORAGE_TYPE`

If the writer type is set to BLOB_STORAGE, we can choose any blob storage. Currently, GCS, S3 and OSS are supported.

* Example value: `GCS`
* Type: `optional`
* Default value: `GCS`

## `DLQ_GCS_GOOGLE_CLOUD_PROJECT_ID`

* Example value: `my-project-id`
* Type: `Required if BLOB storage type is GCS`

## `DLQ_GCS_BUCKET_NAME`

* Example value: `dlq-bucket`
* Type: `Required if BLOB storage type is GCS`

## `DLQ_GCS_CREDENTIAL_PATH`

* Example value: `/path/for/json/credential`
* Type: `Required if BLOB storage type is GCS`

## `DLQ_GCS_RETRY_MAX_ATTEMPTS`

* Example value: `3`
* Type: `optional`
* Default value: `10`

## `DLQ_GCS_RETRY_TOTAL_TIMEOUT_MS`

* Example value: `120000`
* Type: `optional`
* Default value: `120000`

## `DLQ_GCS_RETRY_INITIAL_DELAY_MS`

* Example value: `1000`
* Type: `optional`
* Default value: `1000`

## `DLQ_GCS_RETRY_MAX_DELAY_MS`

* Example value: `30000`
* Type: `optional`
* Default value: `30000`

## `DLQ_GCS_RETRY_DELAY_MULTIPLIER`

* Example value: `2`
* Type: `optional`
* Default value: `2`

## `DLQ_GCS_RETRY_INITIAL_RPC_TIMEOUT_MS`

* Example value: `5000`
* Type: `optional`
* Default value: `5000`

## `DLQ_GCS_RETRY_RPC_TIMEOUT_MULTIPLIER`

* Example value: `1`
* Type: `optional`
* Default value: `1`

## `DLQ_GCS_RETRY_RPC_MAX_TIMEOUT_MS`

* Example value: `5000`
* Type: `optional`
* Default value: `5000`

## `DLQ_KAFKA_ACKS`

* Example value: `all`
* Type: `optional`
* Default value: `all`

## `DLQ_KAFKA_RETRIES`

* Example value: `3`
* Type: `optional`
* Default value: `2147483647`

## `DLQ_KAFKA_BATCH_SIZE`

* Example value: `100`
* Type: `optional`
* Default value: `16384`

## `DLQ_KAFKA_LINGER_MS`

* Example value: `5`
* Type: `optional`
* Default value: `0`

## `DLQ_KAFKA_BUFFER_MEMORY`

* Example value: `33554432`
* Type: `optional`
* Default value: `33554432`

## `DLQ_KAFKA_KEY_SERIALIZER`

* Example value: `your.own.class`
* Type: `optional`
* Default value: `org.apache.kafka.common.serialization.ByteArraySerializer`

## `DLQ_KAFKA_VALUE_SERIALIZER`

* Example value: `your.own.class`
* Type: `optional`
* Default value: `org.apache.kafka.common.serialization.ByteArraySerializer`

## `DLQ_KAFKA_BROKERS`

* Example value: `127.0.0.1:1234`
* Type: `required if writer type is kafka`

## `DLQ_KAFKA_TOPIC`

* Example value: `your-own-topic`
* Type: `optional`
* Default value: `firehose-retry-topic`

## `DLQ_KAFKA_(.*)`

* Example property: `DLQ_KAFKA_SASL_JAAS_CONFIG`, `DLQ_KAFKA_SECURITY_PROTOCOL`, `DLQ_KAFKA_SSL_TRUSTSTORE_PASSWORD`, `DLQ_KAFKA_SASL_MECHANISM`
* Type: `optional`
* Default value: `null`
* Description: Any property starting with `DLQ_KAFKA_` will be passed to the kafka producer. This one is useful for setting any kafka producer property that is not available in the configuration.

## `DLQ_S3_REGION"`

Amazon S3 creates buckets in a Region that you specify.

* Example value: `ap-south-1`
* Type: `required`

## `DLQ_S3_BUCKET_NAME"`

The Name of  Amazon S3 bucket .Here is further documentation of s3 [bucket name](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingBucket.html).

* Example value: `sink_bucket`
* Type: `required`

## `DLQ_S3_ACCESS_KEY"`

Access Key to access the bucket. This key can also be set through env using `AWS_ACCESS_KEY_ID` key or by creating credentials file in `${HOME}/.aws/credentials` folder . Here is further documentation on how to set through [credentials file](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) or [environment varialbes](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html)

* Example value: `AKIAIOSFODNN7EXAMPLE`
* Type: `required`

## `DLQ_S3_SECRET_KEY"`

Secret Key to access the bucket. This key can also be set through env using `AWS_SECRET_ACCESS_KEY` key or by creating credentials file in `${HOME}/.aws/credentials` folder . Here is further documentation on how to set through [credentials file](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) or [environment varialbes](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html)

* Example value: `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`
* Type: `required`

## `DLQ_S3_RETRY_MAX_ATTEMPTS`

Number of retry of the s3 upload request when the request failed.

* Example value: `10`
* Type: `optional`
* Default value : `10`

## `DLQ_S3_BASE_DELAY_MS"`

Initial delay for first retry in milliseconds.

* Example value: `1000`
* Type: `optional`
* Default value : `1000`

## `DLQ_S3_MAX_BACKOFF_MS"`

Max backoff time for retry in milliseconds

* Example value: `30000`
* Type: `optional`
* Default value : `30000`

## `DLQ_S3_API_ATTEMPT_TIMEOUT_MS"`

The amount of time to wait for the http request to complete before giving up and timing out in milliseconds.

* Example value: `10000`
* Type: `optional`
* Default value : `10000`

## `DLQ_S3_API_TIMEOUT_MS"`

The amount of time to allow the client to complete the execution of an API call. This timeout covers the entire client execution except for marshalling. Unit is in milliseconds.

* Example value: `40000`
* Type: `optional`
* Default value : `40000`

## `DLQ_OSS_ENDPOINT`

The endpoint of the oss service. For more information, please refer to the [oss documentation](https://www.alibabacloud.com/help/en/oss/user-guide/regions-and-endpoints?spm=a2c63.p38356.0.0.65ad7fdf6qkcoQ).
Mandatory if DLQ_BLOB_STORAGE_TYPE is OSS.

* Example value: `oss-cn-hangzhou.aliyuncs.com`
* Type: `Required if DLQ_BLOB_STORAGE_TYPE is OSS`
* Default value : `null`

## `DLQ_OSS_ACCESS_ID`

The access key id of the oss service. For more information, please refer to the [oss documentation](https://www.alibabacloud.com/help/en/oss/developer-reference/oss-java-configure-access-credentials#dd657ea839xv1).

* Example value: `youraccessid`
* Type: `Required if DLQ_BLOB_STORAGE_TYPE is OSS`

## `DLQ_OSS_ACCESS_KEY`

The access key secret of the oss service. For more information, please refer to the [oss documentation](https://www.alibabacloud.com/help/en/oss/developer-reference/oss-java-configure-access-credentials#dd657ea839xv1).

* Example value: `youraccesskey`
* Type: `Required if DLQ_BLOB_STORAGE_TYPE is OSS`

## `DLQ_OSS_BUCKET_NAME`

The name of the oss bucket. Must adhere to the naming rules of oss. For more information, please refer to the [oss documentation](https://www.alibabacloud.com/help/en/oss/user-guide/bucket-naming-conventions?spm=a2c63.p38356.0.0.4cdb3962K5f3io).

* Example value: `oss_bucket`
* Type: `Required if DLQ_BLOB_STORAGE_TYPE is OSS`

## `DLQ_OSS_DIRECTORY_PREFIX`

The prefix of the directory in the oss bucket. For more information, please refer to the [oss documentation](https://www.alibabacloud.com/help/en/oss/user-guide/object-naming-conventions).

* Example value: `oss_prefix`
* Type: `optional`

## `DLQ_OSS_SOCKET_TIMEOUT_MS`

The socket timeout in milliseconds.

* Example value: `10000`
* Type: `required`
* Default value : `50000`

## `DLQ_OSS_CONNECTION_TIMEOUT_MS`

The connection timeout in milliseconds.

* Example value: `50000`
* Type: `required`
* Default value : `50000`

## `DLQ_OSS_CONNECTION_REQUEST_TIMEOUT_MS`

The connection request timeout in milliseconds. Negative value indicates no timeout.

* Example value: `100`
* Type: `required`
* Default value : `-1`

## `DLQ_OSS_REQUEST_TIMEOUT_MS`

The request timeout in milliseconds.

* Example value: `50000`
* Type: `required`
* Default value : `300000`

## `DLQ_OSS_RETRY_ENABLED`

The flag to enable retry mechanism for OSS client when transient failure occurred.

* Example value: `true`
* Type: `required`
* Default value : `true`

## `DLQ_OSS_MAX_RETRY_ATTEMPTS`

The maximum number of retry attempts. To be used in conjunction when `DLQ_OSS_RETRY_ENABLED` is set to `true`.

* Example value: `3`
* Type: `required`
* Default value : `3`
