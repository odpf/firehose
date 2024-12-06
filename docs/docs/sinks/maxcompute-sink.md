# MaxCompute sink

### Datatype Protobuf

MaxCompute sink has several responsibilities, including :

1. Creation of MaxCompute table if it does not exist.
2. Updating the MaxCompute table schema based on the latest protobuf schema.
3. Translating protobuf messages into MaxCompute compatible records and inserting them into MaxCompute tables.

## MaxCompute Table Schema Update

### Protobuf

MaxCompute Sink update the MaxCompute table schema on separate table update operation. MaxCompute
utilise [Stencil](https://github.com/goto/stencil) to parse protobuf messages generate schema and update MaxCompute
tables with the latest schema.
The stencil client periodically reload the descriptor cache. Table schema update happened after the descriptor caches
uploaded.

#### Supported Protobuf - MaxCompute Table Type Mapping

| Protobuf Type                            | MaxCompute Type               |
|------------------------------------------|-------------------------------|
| bytes                                    | BINARY                        |
| string                                   | STRING                        |
| enum                                     | STRING                        |
| float                                    | FLOAT                         |
| double                                   | DOUBLE                        |
| bool                                     | BOOLEAN                       |
| int64, uint64, fixed64, sfixed64, sint64 | BIGINT                        |
| int32, uint32, fixed32, sfixed32, sint32 | INT                           |
| message                                  | STRUCT                        |
| .google.protobuf.Timestamp               | TIMESTAMP_NTZ                 |
| .google.protobuf.Struct                  | STRING (Json Serialised)      |
| .google.protobuf.Duration                | STRUCT                        |
| map<k,v>                                 | ARRAY<STRUCT<key:k, value:v>> |

## Partitioning

MaxCompute Sink supports creation of table with partition configuration. Currently, MaxCompute Sink supports primitive field(STRING, TINYINT, SMALLINT, BIGINT)
and timestamp field based partitioning. Timestamp based partitioning strategy introduces a pseudo-partition column with the value of the timestamp field truncated to the nearest start of day.

## Clustering

MaxCompute Sink currently does not support clustering.

## Metadata

For data quality checking purposes, sometimes some metadata need to be added on the record.
if `SINK_MAXCOMPUTE_ADD_METADATA_ENABLED` is true then the metadata will be added.
`SINK_MAXCOMPUTE_METADATA_NAMESPACE` is used for another namespace to add columns
if namespace is empty, the metadata columns will be added in the root level.
`SINK_MAXCOMPUTE_METADATA_COLUMNS_TYPES` is set with kafka metadata column and their type,
An example of metadata columns that can be added for kafka records.
