package io.odpf.firehose.sink.bigquery.error;


import com.google.cloud.bigquery.BigQueryError;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ErrorParser determines the {@link ErrorDescriptor} classes error based on the
 * error string supplied.
 */
public class ErrorParser {

    public static ErrorDescriptor getError(String reasonText, String msgText) {
        List<ErrorDescriptor> errDescList = Arrays.asList(
                new InvalidSchemaError(reasonText, msgText),
                new OOBError(reasonText, msgText),
                new StoppedError(reasonText));

        ErrorDescriptor errorDescriptor = errDescList
                .stream()
                .filter(ErrorDescriptor::matches)
                .findFirst()
                .orElse(new UnknownError());

        return errorDescriptor;
    }

    public static List<ErrorDescriptor> parseError(List<BigQueryError> bqErrors) {
        return bqErrors.stream()
                .map(err -> getError(err.getReason(), err.getMessage()))
                .collect(Collectors.toList());
    }

}
