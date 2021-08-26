package io.odpf.firehose.sink.bigquery.handler;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.firehose.TestMessageBQ;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.error.ErrorType;
import io.odpf.firehose.sink.bigquery.MessageUtils;
import io.odpf.firehose.sink.bigquery.OffsetInfo;
import io.odpf.firehose.sink.bigquery.exception.BigQuerySinkException;
import io.odpf.firehose.sink.bigquery.models.Record;
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

public class BigQueryResponseParserTest {

    private final MessageUtils util = new MessageUtils();
    @Mock
    private InsertAllResponse response;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldParseResponse() throws InvalidProtocolBufferException {
        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        OffsetInfo record2Offset = new OffsetInfo("topic1", 2, 102, Instant.now().toEpochMilli());
        OffsetInfo record3Offset = new OffsetInfo("topic1", 3, 103, Instant.now().toEpochMilli());
        OffsetInfo record4Offset = new OffsetInfo("topic1", 4, 104, Instant.now().toEpochMilli());
        OffsetInfo record5Offset = new OffsetInfo("topic1", 5, 104, Instant.now().toEpochMilli());
        OffsetInfo record6Offset = new OffsetInfo("topic1", 6, 104, Instant.now().toEpochMilli());
        Record record1 = new Record(util.withOffsetInfo(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1"), new HashMap<>());
        Record record2 = new Record(util.withOffsetInfo(record2Offset).createConsumerRecord("order-2", "order-url-2", "order-details-2"), new HashMap<>());
        Record record3 = new Record(util.withOffsetInfo(record3Offset).createConsumerRecord("order-3", "order-url-3", "order-details-3"), new HashMap<>());
        Record record4 = new Record(util.withOffsetInfo(record4Offset).createConsumerRecord("order-4", "order-url-4", "order-details-4"), new HashMap<>());
        Record record5 = new Record(util.withOffsetInfo(record5Offset).createConsumerRecord("order-5", "order-url-5", "order-details-5"), new HashMap<>());
        Record record6 = new Record(util.withOffsetInfo(record6Offset).createConsumerRecord("order-6", "order-url-6", "order-details-6"), new HashMap<>());
        List<Record> records = Collections.list(record1, record2, record3, record4, record5, record6);
        BigQueryError error1 = new BigQueryError("", "US", "");
        BigQueryError error2 = new BigQueryError("invalid", "US", "no such field");
        BigQueryError error3 = new BigQueryError("invalid", "", "The destination table's partition tmp$20160101 is outside the allowed bounds. You can only stream to partitions within 1825 days in the past and 366 days in the future relative to the current date");
        BigQueryError error4 = new BigQueryError("stopped", "", "");

        Map<Long, List<BigQueryError>> insertErrorsMap = new HashMap<Long, List<BigQueryError>>() {{
            put(0L, Collections.list(error1));
            put(1L, Collections.list(error2));
            put(2L, Collections.list(error3));
            put(3L, Collections.list(error4));
        }};
        Mockito.when(response.hasErrors()).thenReturn(true);
        Mockito.when(response.getInsertErrors()).thenReturn(insertErrorsMap);
        List<Message> messages = BigQueryResponseParser.parseResponse(records, response);

        Assert.assertEquals(4, messages.size());
        Assert.assertEquals(TestMessageBQ.newBuilder()
                .setOrderNumber("order-1")
                .setOrderUrl("order-url-1")
                .setOrderDetails("order-details-1")
                .build(), TestMessageBQ.parseFrom(messages.get(0).getLogMessage()));
        Assert.assertEquals(TestMessageBQ.newBuilder()
                .setOrderNumber("order-2")
                .setOrderUrl("order-url-2")
                .setOrderDetails("order-details-2")
                .build(), TestMessageBQ.parseFrom(messages.get(1).getLogMessage()));
        Assert.assertEquals(TestMessageBQ.newBuilder()
                .setOrderNumber("order-3")
                .setOrderUrl("order-url-3")
                .setOrderDetails("order-details-3")
                .build(), TestMessageBQ.parseFrom(messages.get(2).getLogMessage()));
        Assert.assertEquals(TestMessageBQ.newBuilder()
                .setOrderNumber("order-4")
                .setOrderUrl("order-url-4")
                .setOrderDetails("order-details-4")
                .build(), TestMessageBQ.parseFrom(messages.get(3).getLogMessage()));
        Assert.assertEquals(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_UNKNOWN_ERROR), messages.get(0).getErrorInfo());
        Assert.assertEquals(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_4XX_ERROR), messages.get(1).getErrorInfo());
        Assert.assertEquals(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_4XX_ERROR), messages.get(2).getErrorInfo());
        Assert.assertEquals(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_5XX_ERROR), messages.get(3).getErrorInfo());
    }
}
