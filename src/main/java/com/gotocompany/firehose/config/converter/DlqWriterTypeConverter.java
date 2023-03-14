package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.sink.dlq.DLQWriterType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class DlqWriterTypeConverter implements Converter<DLQWriterType> {
    @Override
    public DLQWriterType convert(Method method, String input) {
        return DLQWriterType.valueOf(input.toUpperCase());
    }
}
