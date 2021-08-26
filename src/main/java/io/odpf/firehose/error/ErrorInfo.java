package io.odpf.firehose.error;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class ErrorInfo {
    private Exception exception;
    private ErrorType errorType;
}
