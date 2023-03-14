package com.gotocompany.firehose.exception;


import com.google.protobuf.DynamicMessage;

/**
 * UnknownFieldsException is thrown when unknown fields is detected on the log message although the proto message was succesfuly parsed.
 * Unknown fields error can happen because multiple causes, and can be handled differently depends on the use case.
 * Unknown fields error by default should be handled by retry the processing because there is a probability that, message deserializer is not updated to the latest schema
 * When consumer is deliberately process message using different schema and intentionally ignore extra fields that missing from descriptor the error handling can be disabled.
 * On some use case that need zero data loss, for example data warehousing unknown fields error should be handled properly to prevent missing fields.
 */
public class UnknownFieldsException extends DeserializerException {

    public UnknownFieldsException(DynamicMessage dynamicMessage) {
        super(String.format("unknown fields found, message : %s", dynamicMessage));
    }
}
