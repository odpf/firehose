package io.odpf.firehose.sink.bigquery.handler;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.TransportOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import io.odpf.firehose.config.BigQuerySinkConfig;
import io.odpf.firehose.metrics.BigQueryMetrics;
import io.odpf.firehose.metrics.Instrumentation;
import lombok.Getter;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

public class BigQueryClient {
    private final BigQuery bigquery;
    @Getter
    private final TableId tableID;
    private final BigQuerySinkConfig bqConfig;
    private final BQTableDefinition bqTableDefinition;
    private final Instrumentation instrumentation;

    public BigQueryClient(BigQuerySinkConfig bqConfig, Instrumentation instrumentation) throws IOException {
        this(getBigQueryInstance(bqConfig), bqConfig, instrumentation);
    }

    public BigQueryClient(BigQuery bq, BigQuerySinkConfig bqConfig, Instrumentation instrumentation) {
        this.bigquery = bq;
        this.bqConfig = bqConfig;
        this.tableID = TableId.of(bqConfig.getDatasetName(), bqConfig.getTableName());
        this.bqTableDefinition = new BQTableDefinition(bqConfig);
        this.instrumentation = instrumentation;
    }

    private static BigQuery getBigQueryInstance(BigQuerySinkConfig sinkConfig) throws IOException {
        TransportOptions transportOptions = BigQueryOptions.getDefaultHttpTransportOptions().toBuilder()
                .setConnectTimeout(Integer.parseInt(sinkConfig.getBqClientConnectTimeoutMS()))
                .setReadTimeout(Integer.parseInt(sinkConfig.getBqClientReadTimeoutMS()))
                .build();
        return BigQueryOptions.newBuilder()
                .setTransportOptions(transportOptions)
                .setCredentials(GoogleCredentials.fromStream(new FileInputStream(sinkConfig.getBigQueryCredentialPath())))
                .setProjectId(sinkConfig.getGCloudProjectID())
                .build().getService();
    }

    public InsertAllResponse insertAll(InsertAllRequest rows) {
        Instant start = Instant.now();
        InsertAllResponse response = bigquery.insertAll(rows);
        instrument(start, BigQueryMetrics.BigQueryAPIType.TABLE_INSERT_ALL);
        return response;
    }

    public void upsertTable(List<Field> bqSchemaFields) throws BigQueryException {
        Schema schema = Schema.of(bqSchemaFields);
        TableDefinition tableDefinition = getTableDefinition(schema);
        TableInfo tableInfo = TableInfo.newBuilder(tableID, tableDefinition)
                .setLabels(bqConfig.getTableLabels())
                .build();
        upsertDatasetAndTable(tableInfo);
    }

    private void upsertDatasetAndTable(TableInfo tableInfo) {
        Dataset dataSet = bigquery.getDataset(tableID.getDataset());
        if (dataSet == null || !bigquery.getDataset(tableID.getDataset()).exists()) {
            Instant start = Instant.now();
            bigquery.create(
                    Dataset.newBuilder(tableID.getDataset())
                            .setLocation(bqConfig.getBigQueryDatasetLocation())
                            .setLabels(bqConfig.getDatasetLabels())
                            .build()
            );
            instrumentation.logInfo("Successfully CREATED bigquery DATASET: {}", tableID.getDataset());
            instrument(start, BigQueryMetrics.BigQueryAPIType.DATASET_CREATE);
        } else if (shouldUpdateDataset(dataSet)) {
            Instant start = Instant.now();
            bigquery.update(
                    Dataset.newBuilder(tableID.getDataset())
                            .setLabels(bqConfig.getDatasetLabels())
                            .build()
            );
            instrumentation.logInfo("Successfully UPDATED bigquery DATASET: {} with labels", tableID.getDataset());
            instrument(start, BigQueryMetrics.BigQueryAPIType.DATASET_UPDATE);
        }

        Table table = bigquery.getTable(tableID);
        if (table == null || !table.exists()) {
            Instant start = Instant.now();
            bigquery.create(tableInfo);
            instrumentation.logInfo("Successfully CREATED bigquery TABLE: {}", tableID.getTable());
            instrument(start, BigQueryMetrics.BigQueryAPIType.TABLE_CREATE);
        } else {
            Schema existingSchema = table.getDefinition().getSchema();
            Schema updatedSchema = tableInfo.getDefinition().getSchema();

            if (shouldUpdateTable(tableInfo, table, existingSchema, updatedSchema)) {
                Instant start = Instant.now();
                bigquery.update(tableInfo);
                instrumentation.logInfo("Successfully UPDATED bigquery TABLE: {}", tableID.getTable());
                instrument(start, BigQueryMetrics.BigQueryAPIType.TABLE_UPDATE);
            } else {
                instrumentation.logInfo("Skipping bigquery table update, since proto schema hasn't changed");
            }
        }
    }

    private void instrument(Instant startTime, BigQueryMetrics.BigQueryAPIType type) {
        instrumentation.incrementCounter(
                BigQueryMetrics.SINK_BIGQUERY_OPERATION_TOTAL,
                String.format(BigQueryMetrics.BIGQUERY_TABLE_TAG, tableID.getTable()),
                String.format(BigQueryMetrics.BIGQUERY_DATASET_TAG, tableID.getDataset()),
                String.format(BigQueryMetrics.BIGQUERY_API_TAG, type));
        instrumentation.captureDurationSince(
                BigQueryMetrics.SINK_BIGQUERY_OPERATION_LATENCY_MILLISECONDS,
                startTime,
                String.format(BigQueryMetrics.BIGQUERY_TABLE_TAG, tableID.getTable()),
                String.format(BigQueryMetrics.BIGQUERY_DATASET_TAG, tableID.getDataset()),
                String.format(BigQueryMetrics.BIGQUERY_API_TAG, type));
    }

    private boolean shouldUpdateTable(TableInfo tableInfo, Table table, Schema existingSchema, Schema updatedSchema) {
        return !table.getLabels().equals(tableInfo.getLabels())
                || !existingSchema.equals(updatedSchema)
                || shouldChangePartitionExpiryForStandardTable(table);
    }

    private boolean shouldUpdateDataset(Dataset dataSet) {
        if (!dataSet.getLocation().equals(bqConfig.getBigQueryDatasetLocation())) {
            throw new RuntimeException("Dataset location cannot be changed from "
                    + dataSet.getLocation() + " to " + bqConfig.getBigQueryDatasetLocation());
        }

        return !dataSet.getLabels().equals(bqConfig.getDatasetLabels());
    }

    private boolean shouldChangePartitionExpiryForStandardTable(Table table) {
        if (!table.getDefinition().getType().equals(TableDefinition.Type.TABLE)) {
            return false;
        }
        TimePartitioning timePartitioning = ((StandardTableDefinition) (table.getDefinition())).getTimePartitioning();
        if (timePartitioning == null) {
            // If the table is not partitioned already, no need to update the table
            return false;
        }
        long neverExpireMs = 0L;
        Long currentExpirationMs = timePartitioning.getExpirationMs() == null ? neverExpireMs : timePartitioning.getExpirationMs();
        Long newExpirationMs = bqConfig.getBigQueryTablePartitionExpiryMS() > 0 ? bqConfig.getBigQueryTablePartitionExpiryMS() : neverExpireMs;
        return !currentExpirationMs.equals(newExpirationMs);
    }

    private TableDefinition getTableDefinition(Schema schema) throws RuntimeException {
        return bqTableDefinition.getTableDefinition(schema);
    }
}
