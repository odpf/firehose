# Redis Sink

Redis Sink is implemented in Firehose using the Redis sink connector implementation in Depot. You can check out Depot Github repository [here](https://github.com/goto/depot).

### Data Types
Redis sink can be created in 3 different modes based on the value of [`SINK_REDIS_DATA_TYPE`](https://github.com/goto/depot/blob/main/docs/reference/configuration/redis.md#sink_redis_data_type): HashSet, KeyValue or List
- `Hashset`: For each message, an entry of the format `key : field : value` is generated and pushed to Redis. Field and value are generated on the basis of the config [`SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING`](https://github.com/goto/depot/blob/main/docs/reference/configuration/redis.md#sink_redis_hashset_field_to_column_mapping)
- `List`: For each message entry of the format `key : value` is generated and pushed to Redis. Value is fetched for the Proto field name provided in the config [`SINK_REDIS_LIST_DATA_FIELD_NAME`](https://github.com/goto/depot/blob/main/docs/reference/configuration/redis.md#sink_redis_list_data_field_name)
- `KeyValue`: For each message entry of the format `key : value` is generated and pushed to Redis. Value is fetched for the proto field name provided in the config [`SINK_REDIS_KEY_VALUE_DATA_FIELD_NAME`](https://github.com/goto/depot/blob/main/docs/reference/configuration/redis.md#sink_redis_key_value_data_field_name)

The `key` is picked up from a field in the message itself.

Limitation: Depot Redis sink only supports Key-Value, HashSet and List entries as of now.

### Configuration

For Redis sink in Firehose we need to set first (`SINK_TYPE`=`redis`). There are some generic configs which are common across different sink types which need to be set which are mentioned in [generic.md](../advance/generic.md). Redis sink specific configs are mentioned in Depot repository. You can check out the Redis Sink configs [here](https://github.com/goto/depot/blob/main/docs/reference/configuration/redis.md)


### Deployment Types
Redis sink, as of now, supports two different Deployment Types `Standalone` and `Cluster`. This can be configured in the Depot environment variable `SINK_REDIS_DEPLOYMENT_TYPE`.
