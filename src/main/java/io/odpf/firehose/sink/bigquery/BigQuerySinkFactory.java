package io.odpf.firehose.sink.bigquery;

import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.client.StencilClient;
import com.gojek.de.stencil.parser.Parser;
import com.gojek.de.stencil.parser.ProtoParser;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.TransportOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import io.odpf.firehose.config.BigQuerySinkConfig;
import io.odpf.firehose.config.enums.SinkType;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.sink.Sink;
import io.odpf.firehose.sink.SinkFactory;
import io.odpf.firehose.sink.bigquery.converter.MessageRecordConverterCache;
import io.odpf.firehose.sink.bigquery.handler.BigQueryClient;
import io.odpf.firehose.sink.bigquery.handler.BigQueryRow;
import io.odpf.firehose.sink.bigquery.handler.BigQueryRowWithInsertId;
import io.odpf.firehose.sink.bigquery.handler.BigQueryRowWithoutInsertId;
import io.odpf.firehose.sink.bigquery.proto.ProtoUpdateListener;
import org.aeonbits.owner.ConfigFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

public class BigQuerySinkFactory implements SinkFactory {

    @Override
    public Sink create(Map<String, String> env, StatsDReporter statsDReporter, StencilClient defaultClient) {
        BigQuerySinkConfig sinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, env);
        try {
            BigQueryClient bigQueryClient = new BigQueryClient(sinkConfig, new Instrumentation(statsDReporter, BigQueryClient.class));
            MessageRecordConverterCache recordConverterWrapper = new MessageRecordConverterCache();
            ProtoUpdateListener protoUpdateListener = new ProtoUpdateListener(sinkConfig, bigQueryClient, recordConverterWrapper);
            StencilClient client = sinkConfig.isSchemaRegistryStencilEnable()
                    ? StencilClientFactory.getClient(sinkConfig.getSchemaRegistryStencilUrls(), env, statsDReporter.getClient(), protoUpdateListener)
                    : StencilClientFactory.getClient();

            Parser parser = new ProtoParser(client, sinkConfig.getInputSchemaProtoClass());
            protoUpdateListener.setStencilParser(parser);
            protoUpdateListener.update(client.getAllDescriptorAndTypeName());
            BigQueryRow rowCreator;
            if (sinkConfig.isRowInsertIdEnabled()) {
                rowCreator = new BigQueryRowWithInsertId();
            } else {
                rowCreator = new BigQueryRowWithoutInsertId();
            }
            return new BigQuerySink(
                    new Instrumentation(statsDReporter, BigQuerySink.class),
                    SinkType.BIGQUERY.name(),
                    bigQueryClient,
                    recordConverterWrapper,
                    rowCreator);
        } catch (IOException e) {
            throw new IllegalArgumentException("Exception occurred while creating sink", e);
        }
    }

    private BigQuery getBigQueryInstance(BigQuerySinkConfig sinkConfig) throws IOException {
        final TransportOptions transportOptions = BigQueryOptions.getDefaultHttpTransportOptions().toBuilder()
                .setConnectTimeout(Integer.parseInt(sinkConfig.getBqClientConnectTimeoutMS()))
                .setReadTimeout(Integer.parseInt(sinkConfig.getBqClientReadTimeoutMS()))
                .build();
        return BigQueryOptions.newBuilder()
                .setTransportOptions(transportOptions)
                .setCredentials(GoogleCredentials.fromStream(new FileInputStream(sinkConfig.getBigQueryCredentialPath())))
                .setProjectId(sinkConfig.getGCloudProjectID())
                .build().getService();
    }
}
