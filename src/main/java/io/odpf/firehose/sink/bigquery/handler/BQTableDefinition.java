package io.odpf.firehose.sink.bigquery.handler;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TimePartitioning;
import io.odpf.firehose.config.BigQuerySinkConfig;
import lombok.AllArgsConstructor;

import java.util.Optional;

@AllArgsConstructor
public class BQTableDefinition {
    private final BigQuerySinkConfig bqConfig;

    public StandardTableDefinition getTableDefinition(Schema schema) {
        StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder()
                .setSchema(schema)
                .build();
        if (!bqConfig.isTablePartitioningEnabled()) {
            return tableDefinition;
        }
        return getPartitionedTableDefinition(schema);
    }

    private StandardTableDefinition getPartitionedTableDefinition(Schema schema) {
        StandardTableDefinition.Builder tableDefinition = StandardTableDefinition.newBuilder();
        Optional<Field> partitionFieldOptional = schema.getFields().stream().filter(obj -> obj.getName().equals(bqConfig.getTablePartitionKey())).findFirst();
        if (!partitionFieldOptional.isPresent()) {
            throw new RuntimeException(String.format("Partition key %s is not present in the schema", bqConfig.getTablePartitionKey()));
        }

        Field partitionField = partitionFieldOptional.get();
        if (isTimePartitionedField(partitionField)) {
            return createTimePartitionBuilder(tableDefinition)
                    .setSchema(schema)
                    .build();
        } else {
            throw new UnsupportedOperationException("Range Bigquery partitioning is not supported, supported paritition fields have to be of DATE or TIMESTAMP type");
        }
    }

    private StandardTableDefinition.Builder createTimePartitionBuilder(StandardTableDefinition.Builder tableBuilder) {
        TimePartitioning.Builder timePartitioningBuilder = TimePartitioning.newBuilder(TimePartitioning.Type.DAY);
        if (bqConfig.getTablePartitionKey() == null) {
            throw new RuntimeException(String.format("Partition key not specified for the table: %s", bqConfig.getTableName()));
        }
        timePartitioningBuilder.setField(bqConfig.getTablePartitionKey())
                .setRequirePartitionFilter(true);

        Long neverExpireMillis = null;
        Long partitionExpiry = bqConfig.getBigQueryTablePartitionExpiryMS() > 0 ? bqConfig.getBigQueryTablePartitionExpiryMS() : neverExpireMillis;
        timePartitioningBuilder.setExpirationMs(partitionExpiry);

        return tableBuilder
                .setTimePartitioning(timePartitioningBuilder.build());
    }

    private boolean isTimePartitionedField(Field partitionField) {
        return partitionField.getType() == LegacySQLTypeName.TIMESTAMP || partitionField.getType() == LegacySQLTypeName.DATE;
    }
}
