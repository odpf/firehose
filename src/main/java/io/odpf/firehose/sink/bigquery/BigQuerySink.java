package io.odpf.firehose.sink.bigquery;

import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.AbstractSink;
import io.odpf.firehose.sink.bigquery.converter.MessageRecordConverterCache;
import io.odpf.firehose.sink.bigquery.handler.BigQueryClient;
import io.odpf.firehose.sink.bigquery.handler.BigQueryResponseParser;
import io.odpf.firehose.sink.bigquery.handler.BigQueryRow;
import io.odpf.firehose.sink.bigquery.models.Record;
import io.odpf.firehose.sink.bigquery.models.Records;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

public class BigQuerySink extends AbstractSink {

    private final BigQueryClient bigQueryClient;
    private final BigQueryRow rowCreator;
    private final Instrumentation instrumentation;
    private final MessageRecordConverterCache converterCache;
    private List<Message> messageList;

    public BigQuerySink(Instrumentation instrumentation,
                        String sinkType,
                        BigQueryClient client,
                        MessageRecordConverterCache converterCache,
                        BigQueryRow rowCreator) {
        super(instrumentation, sinkType);
        this.instrumentation = instrumentation;
        this.bigQueryClient = client;
        this.converterCache = converterCache;
        this.rowCreator = rowCreator;
    }

    @Override
    protected List<Message> execute() throws Exception {
        Instant now = Instant.now();
        Records records = converterCache.getMessageRecordConverter().convert(messageList, now);
        List<Message> invalidMessages = records.getInvalidRecords().stream().map(Record::getMessage).collect(Collectors.toList());
        if (records.getValidRecords().size() > 0) {
            InsertAllResponse response = insertIntoBQ(records.getValidRecords());
            if (response.hasErrors()) {
                invalidMessages.addAll(BigQueryResponseParser.parseResponse(records.getValidRecords(), response));
            }
        }
        return invalidMessages;
    }

    @Override
    protected void prepare(List<Message> messages) throws DeserializerException, IOException, SQLException {
        this.messageList = messages;
    }

    @Override
    public void close() throws IOException {

    }

    private InsertAllResponse insertIntoBQ(List<Record> records) {
        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(bigQueryClient.getTableID());
        records.forEach((Record m) -> builder.addRow(rowCreator.of(m)));
        InsertAllRequest rows = builder.build();
        InsertAllResponse response = bigQueryClient.insertAll(rows);
        instrumentation.logInfo("Pushed a batch of {} records to BQ. Insert success?: {}", records.size(), !response.hasErrors());
        return response;
    }
}
