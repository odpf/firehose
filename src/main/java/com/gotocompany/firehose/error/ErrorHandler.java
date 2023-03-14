package com.gotocompany.firehose.error;

import com.gotocompany.firehose.config.ErrorConfig;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.depot.error.ErrorType;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Error handler for messages.
 */
@AllArgsConstructor
public class ErrorHandler {

    private final ErrorConfig config;

    /**
     * @param message Message to filter
     * @param scope   scope of the error
     * @return true
     * if no error info is present and
     * if error type matches the scope
     */
    public boolean filter(Message message, ErrorScope scope) {
        if (message.getErrorInfo() == null) {
            return scope.equals(ErrorScope.RETRY);
        }
        ErrorType type = message.getErrorInfo().getErrorType();
        switch (scope) {
            case DLQ:
                return config.getErrorTypesForDLQ().contains(type);
            case FAIL:
                return config.getErrorTypesForFailing().contains(type);
            case RETRY:
                return config.getErrorTypesForRetry().contains(type);
            default:
                throw new IllegalArgumentException("Unknown Error Scope");
        }
    }

    public Map<Boolean, List<Message>> split(List<Message> messages, ErrorScope scope) {
        return messages.stream().collect(Collectors.partitioningBy(m -> filter(m, scope)));
    }
}
