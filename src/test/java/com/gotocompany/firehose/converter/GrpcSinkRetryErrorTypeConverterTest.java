package com.gotocompany.firehose.converter;

import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.firehose.config.converter.GrpcSinkRetryErrorTypeConverter;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


public class GrpcSinkRetryErrorTypeConverterTest {
    @Test
    public void shouldConvertToAppropriateEnumType() {
        Map<String, ErrorType> stringToExpectedValue = Arrays.stream(ErrorType.values())
                .collect(Collectors.toMap(ErrorType::toString, Function.identity()));
        GrpcSinkRetryErrorTypeConverter grpcSinkRetryErrorTypeConverter = new GrpcSinkRetryErrorTypeConverter();

        stringToExpectedValue.keySet().stream()
                .forEach(key -> {
                    ErrorType expectedValue = stringToExpectedValue.get(key);
                    ErrorType actualValue = grpcSinkRetryErrorTypeConverter.convert(null, key);
                    Assertions.assertEquals(expectedValue, actualValue);
                });
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionForInvalidValue() {
        GrpcSinkRetryErrorTypeConverter grpcSinkRetryErrorTypeConverter = new GrpcSinkRetryErrorTypeConverter();
        grpcSinkRetryErrorTypeConverter.convert(null, "ErrorType.UNREGISTERED");
    }
}
