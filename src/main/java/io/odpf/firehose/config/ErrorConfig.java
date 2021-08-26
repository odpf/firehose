package io.odpf.firehose.config;

import io.odpf.firehose.config.converter.SetErrorTypeConverter;
import io.odpf.firehose.error.ErrorType;
import org.aeonbits.owner.Config;
import org.aeonbits.owner.Mutable;

import java.util.Set;

public interface ErrorConfig extends Config, Mutable {

    @ConverterClass(SetErrorTypeConverter.class)
    @Key("ERROR_TYPES_FOR_DLQ")
    @Separator(",")
    @DefaultValue("")
    Set<ErrorType> getErrorTypesForDLQ();

    @ConverterClass(SetErrorTypeConverter.class)
    @Key("ERROR_TYPES_FOR_RETRY")
    @Separator(",")
    @DefaultValue("")
    Set<ErrorType> getErrorTypesForRetry();

    @ConverterClass(SetErrorTypeConverter.class)
    @Key("ERROR_TYPES_FOR_FAILING")
    @Separator(",")
    @DefaultValue("DESERIALIZATION_ERROR,INVALID_MESSAGE_ERROR,UNKNOWN_FIELDS_ERROR")
    Set<ErrorType> getErrorTypesForFailing();

}
