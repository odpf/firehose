package io.odpf.firehose.sink.bigquery.converter;

import com.gojek.de.stencil.parser.Parser;
import com.google.api.client.util.DateTime;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.firehose.config.BigQuerySinkConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.error.ErrorType;
import io.odpf.firehose.exception.DeserializerException;
import io.odpf.firehose.sink.exception.EmptyMessageException;
import io.odpf.firehose.sink.exception.UnknownFieldsException;
import io.odpf.firehose.sink.bigquery.models.Constants;
import io.odpf.firehose.sink.bigquery.models.Record;
import io.odpf.firehose.sink.bigquery.models.Records;
import io.odpf.firehose.sink.bigquery.proto.UnknownProtoFields;
import io.odpf.firehose.sink.common.ProtoUtils;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
@Slf4j
public class MessageRecordConverter {
    private final RowMapper rowMapper;
    private final Parser parser;
    private final BigQuerySinkConfig config;

    public Records convert(List<Message> messages, Instant now) {
        ArrayList<Record> validRecords = new ArrayList<>();
        ArrayList<Record> invalidRecords = new ArrayList<>();
        for (Message message : messages) {
            try {
                Record record = createRecord(now, message);
                validRecords.add(record);
            } catch (UnknownFieldsException e) {
                message.setErrorInfo(new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR));
                invalidRecords.add(new Record(message, Collections.emptyMap()));
            } catch (EmptyMessageException e) {
                message.setErrorInfo(new ErrorInfo(e, ErrorType.INVALID_MESSAGE_ERROR));
                invalidRecords.add(new Record(message, Collections.emptyMap()));
            } catch (DeserializerException e) {
                message.setErrorInfo(new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR));
                invalidRecords.add(new Record(message, Collections.emptyMap()));
            }
        }
        return new Records(validRecords, invalidRecords);
    }

    private Record createRecord(Instant now, Message message) throws DeserializerException {
        if (message.getLogMessage() == null || message.getLogMessage().length == 0) {
            log.info("empty message found at offset: {}, partition: {}", message.getOffset(), message.getPartition());
            throw new EmptyMessageException();
        }

        try {
            DynamicMessage dynamicMessage = parser.parse(message.getLogMessage());

            if (!config.getInputSchemaProtoAllowUnknownFieldsEnable() && ProtoUtils.hasUnknownField(dynamicMessage)) {
                log.info("unknown fields found at offset: {}, partition: {}, message: {}", message.getOffset(), message.getPartition(), message);
                throw new UnknownFieldsException(dynamicMessage);
            }

            Map<String, Object> columns = rowMapper.map(dynamicMessage);
            addMetadata(columns, message, now);
            return new Record(message, columns);
        } catch (InvalidProtocolBufferException e) {
            log.info("failed to deserialize message: {} at offset: {}, partition: {}", UnknownProtoFields.toString(message.getLogMessage()),
                    message.getOffset(), message.getPartition());
            throw new DeserializerException("failed to deserialize ", e);
        }
    }

    private void addMetadata(Map<String, Object> columns, Message message, Instant now) {
        Map<String, Object> offsetMetadata = new HashMap<>();
        offsetMetadata.put(Constants.PARTITION_COLUMN_NAME, message.getPartition());
        offsetMetadata.put(Constants.OFFSET_COLUMN_NAME, message.getOffset());
        offsetMetadata.put(Constants.TOPIC_COLUMN_NAME, message.getTopic());
        offsetMetadata.put(Constants.TIMESTAMP_COLUMN_NAME, new DateTime(message.getTimestamp()));
        offsetMetadata.put(Constants.LOAD_TIME_COLUMN_NAME, new DateTime(Date.from(now)));

        if (config.getBqMetadataNamespace().isEmpty()) {
            columns.putAll(offsetMetadata);
        } else {
            columns.put(config.getBqMetadataNamespace(), offsetMetadata);
        }
    }
}
