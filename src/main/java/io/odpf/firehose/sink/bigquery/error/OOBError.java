package io.odpf.firehose.sink.bigquery.error;

import lombok.AllArgsConstructor;

@AllArgsConstructor
/**
 * Out of bounds are caused when the partitioned column has a date value less than
 * 5 years and more than 1 year in future
 * */
public class OOBError implements ErrorDescriptor {

    private final String reason;
    private final String message;

    @Override
    public boolean matches() {
        return reason.equals("invalid")
               && ((message.contains("is outside the allowed bounds") && message.contains("days in the past and") && message.contains("days in the future"))
                   || message.contains("out of range"));
    }
    @Override
    public String toString() {
        return "OOBError";
    }

}
