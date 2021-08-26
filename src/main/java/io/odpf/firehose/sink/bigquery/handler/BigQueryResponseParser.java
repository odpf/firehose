package io.odpf.firehose.sink.bigquery.handler;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllResponse;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.error.ErrorInfo;
import io.odpf.firehose.error.ErrorType;
import io.odpf.firehose.sink.bigquery.error.ErrorDescriptor;
import io.odpf.firehose.sink.bigquery.error.ErrorParser;
import io.odpf.firehose.sink.bigquery.error.InvalidSchemaError;
import io.odpf.firehose.sink.bigquery.error.OOBError;
import io.odpf.firehose.sink.bigquery.error.StoppedError;
import io.odpf.firehose.sink.bigquery.exception.BigQuerySinkException;
import io.odpf.firehose.sink.bigquery.models.Record;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
public class BigQueryResponseParser {
    /**
     * Parses the {@link InsertAllResponse} object and returns {@link Message} that were
     * tried to sink in BQ and the error type {@link ErrorDescriptor}.
     * {@link InsertAllResponse} in bqResponse are 1 to 1 indexed based on the records that are requested to be inserted.
     *
     * @param records    - list of records that were tried with BQ insertion
     * @param bqResponse - the status of insertion for all records as returned by BQ
     * @return list of messages with error.
     */
    public static List<Message> parseResponse(final List<Record> records, final InsertAllResponse bqResponse) {
        if (!bqResponse.hasErrors()) {
            return Collections.emptyList();
        }
        List<Message> messages = new ArrayList<>();
        Map<Long, List<BigQueryError>> insertErrorsMap = bqResponse.getInsertErrors();
        for (final Map.Entry<Long, List<BigQueryError>> errorEntry : insertErrorsMap.entrySet()) {
            final Message message = records.get(errorEntry.getKey().intValue()).getMessage();
            List<ErrorDescriptor> errors = ErrorParser.parseError(errorEntry.getValue());
            log.error("Error while bigquery insert for message. Record: {}, Error: {}, Topic: {}, Partition: {}, Offset: {}",
                    records.get(errorEntry.getKey().intValue()).getColumns(),
                    errors,
                    message.getTopic(),
                    message.getPartition(),
                    message.getOffset());
            if (errorMatch(errors, io.odpf.firehose.sink.bigquery.error.UnknownError.class)) {
                message.setErrorInfo(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_UNKNOWN_ERROR));
            } else if (errorMatch(errors, InvalidSchemaError.class) || errorMatch(errors, OOBError.class)) {
                message.setErrorInfo(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_4XX_ERROR));
            } else if (errorMatch(errors, StoppedError.class)) {
                message.setErrorInfo(new ErrorInfo(new BigQuerySinkException(), ErrorType.SINK_5XX_ERROR));
            }
            messages.add(message);
        }

        return messages;
    }

    public static boolean errorMatch(List<ErrorDescriptor> errors, Class c) {
        return errors.stream()
                .anyMatch(errorDescriptor -> errorDescriptor.getClass().equals(c));
    }
}
