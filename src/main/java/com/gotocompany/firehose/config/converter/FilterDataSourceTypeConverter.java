package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.config.enums.FilterDataSourceType;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class FilterDataSourceTypeConverter implements Converter<FilterDataSourceType> {
    @Override
    public FilterDataSourceType convert(Method method, String input) {
        try {
            return FilterDataSourceType.valueOf(input.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("FILTER_DATA_SOURCE must be or KEY or MESSAGE", e);
        }
    }
}
