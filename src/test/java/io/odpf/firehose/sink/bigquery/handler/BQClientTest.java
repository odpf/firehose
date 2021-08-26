package io.odpf.firehose.sink.bigquery.handler;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import io.odpf.firehose.config.BigQuerySinkConfig;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.bigquery.models.Constants;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BQClientTest {
    @Mock
    private BigQuery bigquery;
    @Mock
    private BigQuerySinkConfig bqConfig;
    @Mock
    private Dataset dataset;
    @Mock
    private Table table;
    @Mock
    private StandardTableDefinition mockTableDefinition;
    @Mock
    private TimePartitioning mockTimePartitioning;
    private BigQueryClient bqClient;

    @Mock
    private Instrumentation instrumentation;

    @Test
    public void shouldIgnoreExceptionIfDatasetAlreadyExists() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("partition_column");
        when(bqConfig.getBigQueryTablePartitionExpiryMS()).thenReturn(-1L);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition_column", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.OFFSET_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TOPIC_COLUMN_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.LOAD_TIME_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TIMESTAMP_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.PARTITION_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        TableDefinition tableDefinition = getPartitionedTableDefinition(bqSchemaFields);
        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(false);
        when(table.exists()).thenReturn(false);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(bigquery.create(tableInfo)).thenReturn(table);

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery).create(DatasetInfo.newBuilder(tableId.getDataset()).setLocation("US").build());
        verify(bigquery).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
    }

    @Test
    public void shouldCreateBigqueryTableWithPartition() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTablePartitionKey()).thenReturn("partition_column");
        when(bqConfig.getBigQueryTablePartitionExpiryMS()).thenReturn(-1L);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition_column", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.OFFSET_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TOPIC_COLUMN_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.LOAD_TIME_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TIMESTAMP_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.PARTITION_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};
        TableDefinition tableDefinition = getPartitionedTableDefinition(bqSchemaFields);
        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(false);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(bigquery.create(tableInfo)).thenReturn(table);

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
    }

    @Test
    public void shouldCreateBigqueryTableWithoutPartition() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.OFFSET_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TOPIC_COLUMN_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.LOAD_TIME_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TIMESTAMP_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.PARTITION_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        TableDefinition tableDefinition = getNonPartitionedTableDefinition(bqSchemaFields);
        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(false);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.exists()).thenReturn(false);
        when(bigquery.create(tableInfo)).thenReturn(table);

        bqClient.upsertTable(bqSchemaFields);

        verify(bigquery).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
    }

    @Test
    public void shouldNotUpdateTableIfTableAlreadyExistsWithSameSchema() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.OFFSET_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TOPIC_COLUMN_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.LOAD_TIME_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TIMESTAMP_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.PARTITION_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        TableDefinition tableDefinition = getNonPartitionedTableDefinition(bqSchemaFields);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getType()).thenReturn(TableDefinition.Type.TABLE);
        when(mockTableDefinition.getSchema()).thenReturn(tableDefinition.getSchema());
        when(table.exists()).thenReturn(true);

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
    }

    @Test
    public void shouldUpdateTableIfTableAlreadyExistsAndSchemaChanges() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.OFFSET_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TOPIC_COLUMN_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.LOAD_TIME_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TIMESTAMP_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.PARTITION_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        TableDefinition tableDefinition = getNonPartitionedTableDefinition(bqSchemaFields);
        ArrayList<Field> updatedBQSchemaFields = new ArrayList<>(bqSchemaFields);
        updatedBQSchemaFields.add(Field.newBuilder("new-field", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        TableDefinition updatedBQTableDefinition = getNonPartitionedTableDefinition(updatedBQSchemaFields);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, updatedBQTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getSchema()).thenReturn(tableDefinition.getSchema());
        when(bigquery.update(tableInfo)).thenReturn(table);

        bqClient.upsertTable(updatedBQSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery).update(tableInfo);
    }

    @Test
    public void shouldUpdateTableIfTableNeedsToSetPartitionExpiry() {
        long partitionExpiry = 5184000000L;
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(true);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryTablePartitionExpiryMS()).thenReturn(partitionExpiry);
        when(bqConfig.getTablePartitionKey()).thenReturn("partition_column");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");
        bqClient = new BigQueryClient(bigquery, bqConfig, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("partition_column", LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.OFFSET_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TOPIC_COLUMN_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.LOAD_TIME_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TIMESTAMP_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.PARTITION_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        TableDefinition tableDefinition = getPartitionedTableDefinition(bqSchemaFields);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getType()).thenReturn(TableDefinition.Type.TABLE);
        when(mockTableDefinition.getTimePartitioning()).thenReturn(mockTimePartitioning);
        when(mockTimePartitioning.getExpirationMs()).thenReturn(null);
        when(mockTableDefinition.getSchema()).thenReturn(tableDefinition.getSchema());
        when(table.exists()).thenReturn(true);

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery).update(tableInfo);
    }

    @Test(expected = BigQueryException.class)
    public void shouldThrowExceptionIfUpdateTableFails() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("US");

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.OFFSET_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TOPIC_COLUMN_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.LOAD_TIME_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TIMESTAMP_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.PARTITION_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        TableDefinition tableDefinition = getNonPartitionedTableDefinition(bqSchemaFields);
        ArrayList<Field> updatedBQSchemaFields = new ArrayList<>(bqSchemaFields);
        updatedBQSchemaFields.add(Field.newBuilder("new-field", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        TableDefinition updatedBQTableDefinition = getNonPartitionedTableDefinition(updatedBQSchemaFields);

        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, updatedBQTableDefinition).build();
        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");
        when(table.exists()).thenReturn(true);
        when(bigquery.getTable(tableId)).thenReturn(table);
        when(table.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getSchema()).thenReturn(tableDefinition.getSchema());
        when(bigquery.update(tableInfo)).thenThrow(new BigQueryException(404, "Failed to update"));

        bqClient = new BigQueryClient(bigquery, bqConfig, instrumentation);
        bqClient.upsertTable(updatedBQSchemaFields);
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionIfDatasetLocationIsChanged() {
        when(bqConfig.isTablePartitioningEnabled()).thenReturn(false);
        when(bqConfig.getBigQueryTablePartitionExpiryMS()).thenReturn(-1L);
        when(bqConfig.getTableName()).thenReturn("bq-table");
        when(bqConfig.getDatasetName()).thenReturn("bq-proto");
        when(bqConfig.getBigQueryDatasetLocation()).thenReturn("new-location");
        bqClient = new BigQueryClient(bigquery, bqConfig, instrumentation);

        ArrayList<Field> bqSchemaFields = new ArrayList<Field>() {{
            add(Field.newBuilder("test-1", LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder("test-2", LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.OFFSET_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TOPIC_COLUMN_NAME, LegacySQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.LOAD_TIME_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.TIMESTAMP_COLUMN_NAME, LegacySQLTypeName.TIMESTAMP).setMode(Field.Mode.NULLABLE).build());
            add(Field.newBuilder(Constants.PARTITION_COLUMN_NAME, LegacySQLTypeName.INTEGER).setMode(Field.Mode.NULLABLE).build());
        }};

        TableDefinition tableDefinition = getPartitionedTableDefinition(bqSchemaFields);
        TableId tableId = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

        when(bigquery.getDataset(tableId.getDataset())).thenReturn(dataset);
        when(dataset.exists()).thenReturn(true);
        when(dataset.getLocation()).thenReturn("US");

        bqClient.upsertTable(bqSchemaFields);
        verify(bigquery, never()).create(tableInfo);
        verify(bigquery, never()).update(tableInfo);
    }

    private TableDefinition getPartitionedTableDefinition(ArrayList<Field> bqSchemaFields) {
        TimePartitioning.Builder timePartitioningBuilder = TimePartitioning.newBuilder(TimePartitioning.Type.DAY);
        timePartitioningBuilder.setField(bqConfig.getTablePartitionKey())
                .setRequirePartitionFilter(true);

        if (bqConfig.getBigQueryTablePartitionExpiryMS() > 0) {
            timePartitioningBuilder.setExpirationMs(bqConfig.getBigQueryTablePartitionExpiryMS());
        }

        Schema schema = Schema.of(bqSchemaFields);

        return StandardTableDefinition.newBuilder()
                .setSchema(schema)
                .setTimePartitioning(timePartitioningBuilder.build())
                .build();
    }

    private TableDefinition getNonPartitionedTableDefinition(ArrayList<Field> bqSchemaFields) {
        Schema schema = Schema.of(bqSchemaFields);

        return StandardTableDefinition.newBuilder()
                .setSchema(schema)
                .build();
    }
}
