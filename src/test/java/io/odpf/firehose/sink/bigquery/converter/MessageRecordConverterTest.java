package io.odpf.firehose.sink.bigquery.converter;

import com.gojek.de.stencil.StencilClientFactory;
import com.gojek.de.stencil.parser.Parser;
import com.gojek.de.stencil.parser.ProtoParser;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnknownFieldSet;
import io.odpf.firehose.TestMessageBQ;
import io.odpf.firehose.config.BigQuerySinkConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.error.ErrorType;
import io.odpf.firehose.sink.bigquery.MessageUtils;
import io.odpf.firehose.sink.bigquery.OffsetInfo;
import io.odpf.firehose.sink.bigquery.models.Records;
import io.odpf.firehose.sink.exception.UnknownFieldsException;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MessageRecordConverterTest {
    private final MessageUtils util = new MessageUtils();
    private MessageRecordConverter recordConverter;
    private RowMapper rowMapper;
    private Parser parser;
    private Instant now;

    @Before
    public void setUp() {
        parser = new ProtoParser(StencilClientFactory.getClient(), TestMessageBQ.class.getName());
        Properties columnMapping = new Properties();
        columnMapping.put(1, "bq_order_number");
        columnMapping.put(2, "bq_order_url");
        columnMapping.put(3, "bq_order_details");
        rowMapper = new RowMapper(columnMapping);

        recordConverter = new MessageRecordConverter(rowMapper, parser,
                ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties()));

        now = Instant.now();
    }

    @Test
    public void shouldGetRecordForBQFromConsumerRecords() {
        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        OffsetInfo record2Offset = new OffsetInfo("topic1", 2, 102, Instant.now().toEpochMilli());
        Message record1 = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        Message record2 = util.withOffsetInfo(record2Offset).createConsumerRecord("order-2", "order-url-2", "order-details-2");


        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
        record1ExpectedColumns.putAll(util.metadataColumns(record1Offset, now));


        Map<String, Object> record2ExpectedColumns = new HashMap<>();
        record2ExpectedColumns.put("bq_order_number", "order-2");
        record2ExpectedColumns.put("bq_order_url", "order-url-2");
        record2ExpectedColumns.put("bq_order_details", "order-details-2");
        record2ExpectedColumns.putAll(util.metadataColumns(record2Offset, now));
        List<Message> messages = Arrays.asList(record1, record2);

        Records records = recordConverter.convert(messages, now);

        assertEquals(messages.size(), records.getValidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        Map<String, Object> record2Columns = records.getValidRecords().get(1).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record2ExpectedColumns.size(), record2Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
        assertEquals(record2ExpectedColumns, record2Columns);
    }

    @Test
    public void shouldIgnoreNullRecords() {
        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        OffsetInfo record2Offset = new OffsetInfo("topic1", 2, 102, Instant.now().toEpochMilli());
        Message record1 = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        Message record2 = util.withOffsetInfo(record2Offset).createEmptyValueConsumerRecord("order-2", "order-url-2");


        Map<Object, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
        record1ExpectedColumns.putAll(util.metadataColumns(record1Offset, now));

        List<Message> messages = Arrays.asList(record1, record2);
        Records records = recordConverter.convert(messages, now);

        assertEquals(1, records.getValidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
    }

    public void shouldReturnInvalidRecordsWhenGivenNullRecords() {
        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        OffsetInfo record2Offset = new OffsetInfo("topic1", 2, 102, Instant.now().toEpochMilli());
        Message record1 = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");
        Message record2 = util.withOffsetInfo(record2Offset).createEmptyValueConsumerRecord("order-2", "order-url-2");

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
        record1ExpectedColumns.putAll(util.metadataColumns(record1Offset, now));

        List<Message> messages = Arrays.asList(record1, record2);
        Records records = recordConverter.convert(messages, now);

        assertEquals(1, records.getValidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
    }

    @Test
    public void shouldNotNamespaceMetadataFieldWhenNamespaceIsNotProvided() {
        BigQuerySinkConfig sinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        MessageRecordConverter recordConverterTest = new MessageRecordConverter(rowMapper, parser, sinkConfig);

        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        Message record1 = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
        record1ExpectedColumns.putAll(util.metadataColumns(record1Offset, now));

        List<Message> messages = Collections.singletonList(record1);
        Records records = recordConverterTest.convert(messages, now);

        assertEquals(messages.size(), records.getValidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
        assertEquals(sinkConfig.getBqMetadataNamespace(), "");
    }

    @Test
    public void shouldNamespaceMetadataFieldWhenNamespaceIsProvided() {
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "metadata_ns");
        BigQuerySinkConfig sinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        MessageRecordConverter recordConverterTest = new MessageRecordConverter(rowMapper, parser, sinkConfig);

        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        Message record1 = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1", "order-url-1", "order-details-1");

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
        record1ExpectedColumns.put(sinkConfig.getBqMetadataNamespace(), util.metadataColumns(record1Offset, now));

        List<Message> messages = Collections.singletonList(record1);
        Records records = recordConverterTest.convert(messages, now);

        assertEquals(messages.size(), records.getValidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
        System.setProperty("SINK_BIGQUERY_METADATA_NAMESPACE", "");
    }


    public void shouldReturnInvalidRecordsGivenInvalidProtobufMessage() {
        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        OffsetInfo record2Offset = new OffsetInfo("topic1", 2, 102, Instant.now().toEpochMilli());
        Message record1 = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");
        Message record2 = new Message("invalid-key".getBytes(), "invalid-value".getBytes(),
                record2Offset.getTopic(), record2Offset.getPartition(),
                record2Offset.getOffset(), null, record2Offset.getTimestamp(), 0);

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
        record1ExpectedColumns.putAll(util.metadataColumns(record1Offset, now));

        List<Message> messages = Arrays.asList(record1, record2);
        Records records = recordConverter.convert(messages, now);
        assertEquals(1, records.getInvalidRecords());
    }

    @Test
    public void shouldWriteToErrorWriterInvalidRecords() {
        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        OffsetInfo record2Offset = new OffsetInfo("topic1", 2, 102, Instant.now().toEpochMilli());
        Message record1 = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");

        Message record2 = new Message("invalid-key".getBytes(), "invalid-value".getBytes(),
                record2Offset.getTopic(), record2Offset.getPartition(),
                record2Offset.getOffset(), null, record2Offset.getTimestamp(), 0);

        Map<String, Object> record1ExpectedColumns = new HashMap<>();
        record1ExpectedColumns.put("bq_order_number", "order-1");
        record1ExpectedColumns.put("bq_order_url", "order-url-1");
        record1ExpectedColumns.put("bq_order_details", "order-details-1");
        record1ExpectedColumns.putAll(util.metadataColumns(record1Offset, now));

        List<Message> messages = Arrays.asList(record1, record2);
        Records records = recordConverter.convert(messages, now);

        assertEquals(1, records.getValidRecords().size());
        assertEquals(1, records.getInvalidRecords().size());
        Map<String, Object> record1Columns = records.getValidRecords().get(0).getColumns();
        assertEquals(record1ExpectedColumns.size(), record1Columns.size());
        assertEquals(record1ExpectedColumns, record1Columns);
    }

    @Test
    public void shouldReturnInvalidRecordsWhenUnknownFieldsFound() throws InvalidProtocolBufferException {
        System.setProperty("INPUT_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE", "false");
        Parser mockParser = mock(Parser.class);

        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        Message consumerRecord = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(TestMessageBQ.getDescriptor())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                        .build())
                .build();
        when(mockParser.parse(consumerRecord.getLogMessage())).thenReturn(dynamicMessage);

        recordConverter = new MessageRecordConverter(rowMapper, mockParser,
                ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties()));

        List<Message> messages = Collections.singletonList(consumerRecord);
        Records records = recordConverter.convert(messages, now);
        consumerRecord.setErrorInfo(new ErrorInfo(new UnknownFieldsException(dynamicMessage), ErrorType.UNKNOWN_FIELDS_ERROR));
        assertEquals(consumerRecord, records.getInvalidRecords().get(0).getMessage());
    }

    @Test
    public void shouldIgnoreUnknownFieldsIfTheConfigIsSet() throws InvalidProtocolBufferException {
        System.setProperty("INPUT_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE", "true");
        Parser mockParser = mock(Parser.class);

        OffsetInfo record1Offset = new OffsetInfo("topic1", 1, 101, Instant.now().toEpochMilli());
        Message consumerRecord = util.withOffsetInfo(record1Offset).createConsumerRecord("order-1",
                "order-url-1", "order-details-1");

        DynamicMessage dynamicMessage = DynamicMessage.newBuilder(TestMessageBQ.getDescriptor())
                .setUnknownFields(UnknownFieldSet.newBuilder()
                        .addField(1, UnknownFieldSet.Field.getDefaultInstance())
                        .build())
                .build();
        when(mockParser.parse(consumerRecord.getLogMessage())).thenReturn(dynamicMessage);

        recordConverter = new MessageRecordConverter(rowMapper, mockParser,
                ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties()));

        List<Message> messages = Collections.singletonList(consumerRecord);
        Records records = recordConverter.convert(messages, now);
        assertEquals(1, records.getValidRecords().size());
        assertEquals(0, records.getInvalidRecords().size());
        assertEquals(consumerRecord, records.getValidRecords().get(0).getMessage());
    }
}
