package com.gotocompany.firehose.config.converter;

import com.gotocompany.depot.error.ErrorType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Locale;

public class GrpcSinkRetryErrorTypeConverter implements Converter<ErrorType> {
    @Override
    public ErrorType convert(Method method, String s) {
        return ErrorType.valueOf(s.trim().toUpperCase(Locale.ROOT));
    }
}
