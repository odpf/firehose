package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.SetErrorTypeConverter;
import com.gotocompany.depot.error.ErrorType;
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
    @DefaultValue("DEFAULT_ERROR,SINK_RETRYABLE_ERROR")
    Set<ErrorType> getErrorTypesForRetry();

    @ConverterClass(SetErrorTypeConverter.class)
    @Key("ERROR_TYPES_FOR_FAILING")
    @Separator(",")
    @DefaultValue("DESERIALIZATION_ERROR,INVALID_MESSAGE_ERROR,UNKNOWN_FIELDS_ERROR")
    Set<ErrorType> getErrorTypesForFailing();

}
