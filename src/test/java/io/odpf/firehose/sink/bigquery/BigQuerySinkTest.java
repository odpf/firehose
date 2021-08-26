package io.odpf.firehose.sink.bigquery;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.TableId;
import io.odpf.firehose.TestMessageBQ;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.error.ErrorType;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.sink.bigquery.converter.MessageRecordConverter;
import io.odpf.firehose.sink.bigquery.converter.MessageRecordConverterCache;
import io.odpf.firehose.sink.bigquery.exception.BigQuerySinkException;
import io.odpf.firehose.sink.bigquery.handler.BigQueryClient;
import io.odpf.firehose.sink.bigquery.handler.BigQueryRow;
import io.odpf.firehose.sink.bigquery.handler.BigQueryRowWithInsertId;
import io.odpf.firehose.sink.bigquery.models.Record;
import io.odpf.firehose.sink.bigquery.models.Records;
import org.aeonbits.owner.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class BigQuerySinkTest {

    private final MessageUtils util = new MessageUtils();
    private final TableId tableId = TableId.of("test_dataset", "test_table");
    private final MessageRecordConverterCache converterCache = new MessageRecordConverterCache();
    private final BigQueryRow rowCreator = new BigQueryRowWithInsertId();
    @Mock
    private BigQueryClient client;
    @Mock
    private Instrumentation instrumentation;
    @Mock
    private MessageRecordConverter converter;
    private BigQuerySink sink;
    @Mock
    private InsertAllResponse response;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        this.converterCache.setMessageRecordConverter(converter);
        this.sink = new BigQuerySink(instrumentation, "BIGQUERY", client, converterCache, rowCreator);
        Mockito.when(client.getTableID()).thenReturn(tableId);
    }

    @Test
    public void shouldPushToBigQuerySink() throws Exception {
        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        OffsetInfo record2Offset = new OffsetInfo("topic1", 2, 102, Instant.now().toEpochMilli());
        OffsetInfo record3Offset = new OffsetInfo("topic1", 3, 103, Instant.now().toEpochMilli());
        OffsetInfo record4Offset = new OffsetInfo("topic1", 4, 104, Instant.now().toEpochMilli());
        OffsetInfo record5Offset = new OffsetInfo("topic1", 5, 104, Instant.now().toEpochMilli());
        OffsetInfo record6Offset = new OffsetInfo("topic1", 6, 104, Instant.now().toEpochMilli());
        Message message1 = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        Message message2 = util.withOffsetInfo(record2Offset).createConsumerRecord("order-2", "order-url-2", "order-details-2");
        Message message3 = util.withOffsetInfo(record3Offset).createConsumerRecord("order-3", "order-url-3", "order-details-3");
        Message message4 = util.withOffsetInfo(record4Offset).createConsumerRecord("order-4", "order-url-4", "order-details-4");
        Message message5 = util.withOffsetInfo(record5Offset).createConsumerRecord("order-5", "order-url-5", "order-details-5");
        Message message6 = util.withOffsetInfo(record6Offset).createConsumerRecord("order-6", "order-url-6", "order-details-6");
        List<Message> messages = Collections.list(message1, message2, message3, message4, message5, message6);
        sink.prepare(messages);
        Record record1 = new Record(message1, new HashMap<>());
        Record record2 = new Record(message2, new HashMap<>());
        Record record3 = new Record(message3, new HashMap<>());
        Record record4 = new Record(message4, new HashMap<>());
        Record record5 = new Record(message5, new HashMap<>());
        Record record6 = new Record(message6, new HashMap<>());
        Records records = new Records(Collections.list(record1, record2, record3, record4, record5, record6), java.util.Collections.emptyList());

        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(client.getTableID());
        records.getValidRecords().forEach((Record m) -> builder.addRow(rowCreator.of(m)));
        InsertAllRequest rows = builder.build();
        Mockito.when(converter.convert(Mockito.eq(messages), Mockito.any(Instant.class))).thenReturn(records);
        Mockito.when(client.insertAll(rows)).thenReturn(response);
        Mockito.when(response.hasErrors()).thenReturn(false);
        List<Message> invalidMessages = sink.execute();
        Assert.assertEquals(0, invalidMessages.size());
        Mockito.verify(client, Mockito.times(1)).insertAll(rows);
    }

    @Test
    public void shouldReturnInvalidMessages() throws Exception {
        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        OffsetInfo record2Offset = new OffsetInfo("topic1", 2, 102, Instant.now().toEpochMilli());
        OffsetInfo record3Offset = new OffsetInfo("topic1", 3, 103, Instant.now().toEpochMilli());
        OffsetInfo record4Offset = new OffsetInfo("topic1", 4, 104, Instant.now().toEpochMilli());
        OffsetInfo record5Offset = new OffsetInfo("topic1", 5, 104, Instant.now().toEpochMilli());
        OffsetInfo record6Offset = new OffsetInfo("topic1", 6, 104, Instant.now().toEpochMilli());
        Message message1 = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        Message message2 = util.withOffsetInfo(record2Offset).createConsumerRecord("order-2", "order-url-2", "order-details-2");
        Message message3 = util.withOffsetInfo(record3Offset).createConsumerRecord("order-3", "order-url-3", "order-details-3");
        Message message4 = util.withOffsetInfo(record4Offset).createConsumerRecord("order-4", "order-url-4", "order-details-4");
        Message message5 = util.withOffsetInfo(record5Offset).createConsumerRecord("order-5", "order-url-5", "order-details-5");
        Message message6 = util.withOffsetInfo(record6Offset).createConsumerRecord("order-6", "order-url-6", "order-details-6");
        List<Message> messages = Collections.list(message1, message2, message3, message4, message5, message6);
        sink.prepare(messages);
        Record record1 = new Record(message1, new HashMap<>());
        Record record2 = new Record(message2, new HashMap<>());
        Record record3 = new Record(message3, new HashMap<>());
        Record record4 = new Record(message4, new HashMap<>());
        Record record5 = new Record(message5, new HashMap<>());
        Record record6 = new Record(message6, new HashMap<>());
        Records records = new Records(Collections.list(record1, record3, record5, record6), Collections.list(record2, record4));

        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(client.getTableID());
        records.getValidRecords().forEach((Record m) -> builder.addRow(rowCreator.of(m)));
        InsertAllRequest rows = builder.build();
        Mockito.when(converter.convert(Mockito.eq(messages), Mockito.any(Instant.class))).thenReturn(records);
        Mockito.when(client.insertAll(rows)).thenReturn(response);
        Mockito.when(response.hasErrors()).thenReturn(false);
        List<Message> invalidMessages = sink.execute();
        Assert.assertEquals(2, invalidMessages.size());
        Mockito.verify(client, Mockito.times(1)).insertAll(rows);

        Assert.assertEquals(TestMessageBQ.newBuilder()
                .setOrderNumber("order-2")
                .setOrderUrl("order-url-2")
                .setOrderDetails("order-details-2")
                .build(), TestMessageBQ.parseFrom(invalidMessages.get(0).getLogMessage()));
        Assert.assertEquals(TestMessageBQ.newBuilder()
                .setOrderNumber("order-4")
                .setOrderUrl("order-url-4")
                .setOrderDetails("order-details-4")
                .build(), TestMessageBQ.parseFrom(invalidMessages.get(1).getLogMessage()));
        Assert.assertNull(invalidMessages.get(0).getErrorInfo());
        Assert.assertNull(invalidMessages.get(1).getErrorInfo());
    }

    @Test
    public void shouldReturnInvalidMessagesWithFailedInsertMessages() throws Exception {
        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        OffsetInfo record2Offset = new OffsetInfo("topic1", 2, 102, Instant.now().toEpochMilli());
        OffsetInfo record3Offset = new OffsetInfo("topic1", 3, 103, Instant.now().toEpochMilli());
        OffsetInfo record4Offset = new OffsetInfo("topic1", 4, 104, Instant.now().toEpochMilli());
        OffsetInfo record5Offset = new OffsetInfo("topic1", 5, 104, Instant.now().toEpochMilli());
        OffsetInfo record6Offset = new OffsetInfo("topic1", 6, 104, Instant.now().toEpochMilli());
        Message message1 = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        Message message2 = util.withOffsetInfo(record2Offset).createConsumerRecord("order-2", "order-url-2", "order-details-2");
        Message message3 = util.withOffsetInfo(record3Offset).createConsumerRecord("order-3", "order-url-3", "order-details-3");
        Message message4 = util.withOffsetInfo(record4Offset).createConsumerRecord("order-4", "order-url-4", "order-details-4");
        Message message5 = util.withOffsetInfo(record5Offset).createConsumerRecord("order-5", "order-url-5", "order-details-5");
        Message message6 = util.withOffsetInfo(record6Offset).createConsumerRecord("order-6", "order-url-6", "order-details-6");
        List<Message> messages = Collections.list(message1, message2, message3, message4, message5, message6);
        sink.prepare(messages);
        Record record1 = new Record(message1, new HashMap<>());
        Record record2 = new Record(message2, new HashMap<>());
        Record record3 = new Record(message3, new HashMap<>());
        Record record4 = new Record(message4, new HashMap<>());
        Record record5 = new Record(message5, new HashMap<>());
        Record record6 = new Record(message6, new HashMap<>());
        Records records = new Records(Collections.list(record1, record3, record5, record6), Collections.list(record2, record4));

        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(client.getTableID());
        records.getValidRecords().forEach((Record m) -> builder.addRow(rowCreator.of(m)));
        InsertAllRequest rows = builder.build();
        Mockito.when(converter.convert(Mockito.eq(messages), Mockito.any(Instant.class))).thenReturn(records);
        Mockito.when(client.insertAll(rows)).thenReturn(response);
        Mockito.when(response.hasErrors()).thenReturn(true);

        BigQueryError error1 = new BigQueryError("", "US", "");
        BigQueryError error3 = new BigQueryError("invalid", "", "The destination table's partition tmp$20160101 is outside the allowed bounds. You can only stream to partitions within 1825 days in the past and 366 days in the future relative to the current date");

        Map<Long, List<BigQueryError>> insertErrorsMap = new HashMap<Long, List<BigQueryError>>() {{
            put(0L, Collections.list(error1));
            put(2L, Collections.list(error3));
        }};
        Mockito.when(response.getInsertErrors()).thenReturn(insertErrorsMap);

        List<Message> invalidMessages = sink.execute();
        Mockito.verify(client, Mockito.times(1)).insertAll(rows);

        Assert.assertEquals(4, invalidMessages.size());
        Assert.assertEquals(TestMessageBQ.newBuilder()
                .setOrderNumber("order-2")
                .setOrderUrl("order-url-2")
                .setOrderDetails("order-details-2")
                .build(), TestMessageBQ.parseFrom(invalidMessages.get(0).getLogMessage()));
        Assert.assertEquals(TestMessageBQ.newBuilder()
                .setOrderNumber("order-4")
                .setOrderUrl("order-url-4")
                .setOrderDetails("order-details-4")
                .build(), TestMessageBQ.parseFrom(invalidMessages.get(1).getLogMessage()));
        Assert.assertEquals(TestMessageBQ.newBuilder()
                .setOrderNumber("order-1")
                .setOrderUrl("order-url-1")
                .setOrderDetails("order-details-1")
                .build(), TestMessageBQ.parseFrom(invalidMessages.get(2).getLogMessage()));
        Assert.assertEquals(TestMessageBQ.newBuilder()
                .setOrderNumber("order-5")
                .setOrderUrl("order-url-5")
                .setOrderDetails("order-details-5")
                .build(), TestMessageBQ.parseFrom(invalidMessages.get(3).getLogMessage()));
        Assert.assertNull(invalidMessages.get(0).getErrorInfo());
        Assert.assertNull(invalidMessages.get(1).getErrorInfo());
        Assert.assertEquals(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_UNKNOWN_ERROR), invalidMessages.get(2).getErrorInfo());
        Assert.assertEquals(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_4XX_ERROR), invalidMessages.get(3).getErrorInfo());
    }
}
