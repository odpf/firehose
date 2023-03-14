package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.FilterMessageFormatType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class FilterMessageFormatTypeConverter implements Converter<FilterMessageFormatType> {
    @Override
    public FilterMessageFormatType convert(Method method, String input) {
        try {
            return FilterMessageFormatType.valueOf(input.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("FILTER_INPUT_MESSAGE_TYPE must be JSON or PROTOBUF");
        }
    }
}
