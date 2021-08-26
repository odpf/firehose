package io.odpf.firehose.config.converter;

import io.odpf.firehose.error.ErrorType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class SetErrorTypeConverter implements Converter<ErrorType> {
    @Override
    public ErrorType convert(Method method, String input) {
        return ErrorType.valueOf(input);
    }
}
