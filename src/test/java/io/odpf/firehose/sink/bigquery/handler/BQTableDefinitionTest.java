package io.odpf.firehose.sink.bigquery.handler;


import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import io.odpf.firehose.config.BigQuerySinkConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BQTableDefinitionTest {
    @Mock
    private BigQuerySinkConfig bqConfig;

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowUnsupportedExceptionForRangePartition() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("int_field");

        Schema bqSchema = Schema.of(
                Field.newBuilder("int_field", LegacySQLTypeName.INTEGER).build()
        );

        BQTableDefinition bqTableDefinition = new BQTableDefinition(bqConfig);
        bqTableDefinition.getTableDefinition(bqSchema);
    }

    @Test
    public void shouldReturnTableDefinitionIfPartitionDisabled() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        Schema bqSchema = Schema.of(
                Field.newBuilder("int_field", LegacySQLTypeName.INTEGER).build()
        );

        BQTableDefinition bqTableDefinition = new BQTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bqTableDefinition.getTableDefinition(bqSchema);
        Schema returnedSchema = tableDefinition.getSchema();
        assertEquals(returnedSchema.getFields().size(), bqSchema.getFields().size());
        assertEquals(returnedSchema.getFields().get(0).getName(), bqSchema.getFields().get(0).getName());
        assertEquals(returnedSchema.getFields().get(0).getMode(), bqSchema.getFields().get(0).getMode());
        assertEquals(returnedSchema.getFields().get(0).getType(), bqSchema.getFields().get(0).getType());
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowErrorIfPartitionFieldNotSet() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        Schema bqSchema = Schema.of(
                Field.newBuilder("int_field", LegacySQLTypeName.INTEGER).build()
        );

        BQTableDefinition bqTableDefinition = new BQTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bqTableDefinition.getTableDefinition(bqSchema);
        tableDefinition.getSchema();
    }

    @Test
    public void shouldCreatePartitionedTable() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        Schema bqSchema = Schema.of(
                Field.newBuilder("timestamp_field", LegacySQLTypeName.TIMESTAMP).build()
        );

        BQTableDefinition bqTableDefinition = new BQTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bqTableDefinition.getTableDefinition(bqSchema);

        Schema returnedSchema = tableDefinition.getSchema();
        assertEquals(returnedSchema.getFields().size(), bqSchema.getFields().size());
        assertEquals(returnedSchema.getFields().get(0).getName(), bqSchema.getFields().get(0).getName());
        assertEquals(returnedSchema.getFields().get(0).getMode(), bqSchema.getFields().get(0).getMode());
        assertEquals(returnedSchema.getFields().get(0).getType(), bqSchema.getFields().get(0).getType());
        assertEquals("timestamp_field", tableDefinition.getTimePartitioning().getField());
    }

    @Test
    public void shouldCreateTableWithPartitionExpiry() {
        long partitionExpiry = 5184000000L;
        when(bqConfig.getBigQueryTablePartitionExpiryMS()).thenReturn(partitionExpiry);
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        Schema bqSchema = Schema.of(
                Field.newBuilder("timestamp_field", LegacySQLTypeName.TIMESTAMP).build()
        );

        BQTableDefinition bqTableDefinition = new BQTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bqTableDefinition.getTableDefinition(bqSchema);

        assertEquals("timestamp_field", tableDefinition.getTimePartitioning().getField());
        assertEquals(partitionExpiry, tableDefinition.getTimePartitioning().getExpirationMs().longValue());
    }

    @Test
    public void shouldReturnTableWithNullPartitionExpiryIfLessThanZero() {
        long partitionExpiry = -1L;
        when(bqConfig.getBigQueryTablePartitionExpiryMS()).thenReturn(partitionExpiry);
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        Schema bqSchema = Schema.of(
                Field.newBuilder("timestamp_field", LegacySQLTypeName.TIMESTAMP).build()
        );

        BQTableDefinition bqTableDefinition = new BQTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bqTableDefinition.getTableDefinition(bqSchema);

        assertEquals("timestamp_field", tableDefinition.getTimePartitioning().getField());
        assertEquals(null, tableDefinition.getTimePartitioning().getExpirationMs());
    }

    @Test
    public void shouldReturnTableWithNullPartitionExpiryIfEqualsZero() {
        long partitionExpiry = 0L;
        when(bqConfig.getBigQueryTablePartitionExpiryMS()).thenReturn(partitionExpiry);
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("timestamp_field");
        Schema bqSchema = Schema.of(
                Field.newBuilder("timestamp_field", LegacySQLTypeName.TIMESTAMP).build()
        );

        BQTableDefinition bqTableDefinition = new BQTableDefinition(bqConfig);
        StandardTableDefinition tableDefinition = bqTableDefinition.getTableDefinition(bqSchema);

        assertEquals("timestamp_field", tableDefinition.getTimePartitioning().getField());
        assertEquals(null, tableDefinition.getTimePartitioning().getExpirationMs());
    }
}
