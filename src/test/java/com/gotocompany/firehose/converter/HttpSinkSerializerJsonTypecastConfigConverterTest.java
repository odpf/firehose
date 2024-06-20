package com.gotocompany.firehose.converter;

import com.gotocompany.firehose.config.converter.HttpSinkSerializerJsonTypecastConfigConverter;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Map;
import java.util.function.Function;

public class HttpSinkSerializerJsonTypecastConfigConverterTest {

    private final HttpSinkSerializerJsonTypecastConfigConverter httpSinkSerializerJsonTypecastConfigConverter = new HttpSinkSerializerJsonTypecastConfigConverter();

    @Test
    public void convertShouldConvertToPropertyMapWhenValidJsonConfig() {
        String configJson = "[{\"jsonPath\": \"$.root.field\", \"type\": \"LONG\"}]";
        String expectedPropertyMapKey = "$.root.field";

        Map<String, Function<String, Object>> result = httpSinkSerializerJsonTypecastConfigConverter.convert(null, configJson);
        Function<String, Object> mapper = result.get(expectedPropertyMapKey);
        Object mapperResult = mapper.apply("4");

        Assertions.assertNotNull(mapper);
        Assertions.assertTrue(mapperResult instanceof Long);
        Assertions.assertEquals(4L, mapperResult);
    }

    @Test
    public void convertShouldThrowJsonParseExceptionWhenInvalidJsonFormatProvided() {
        String malformedConfigJson = "[{\"jsonPath\": \"$.root.field\" \"type\": \"LONG\"";

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> httpSinkSerializerJsonTypecastConfigConverter.convert(null, malformedConfigJson));
    }

    @Test
    public void convertShouldThrowJsonParseExceptionWhenUnregisteredTypecastingProvided() {
        String malformedConfigJson = "[{\"jsonPath\": \"$.root.field\", \"type\": \"BIG_INTEGER\"}]";

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> httpSinkSerializerJsonTypecastConfigConverter.convert(null, malformedConfigJson));
    }

    @Test
    public void convertShouldHandleEmptyJsonConfig() {
        String emptyConfigJson = "[]";

        Map<String, Function<String, Object>> result = httpSinkSerializerJsonTypecastConfigConverter.convert(null, emptyConfigJson);

        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void convertShouldHandleNullJsonConfig() {
        String nullConfigJson = null;

        Map<String, Function<String, Object>> result = httpSinkSerializerJsonTypecastConfigConverter.convert(null, nullConfigJson);

        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void convertShouldThrowExceptionForUnsupportedDataType() {
        String unsupportedTypeConfigJson = "[{\"jsonPath\": \"$.root.field\", \"type\": \"UNSUPPORTED_TYPE\"}]";

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> httpSinkSerializerJsonTypecastConfigConverter.convert(null, unsupportedTypeConfigJson));
    }

    @Test
    public void convertShouldHandleMultipleValidConfigs() {
        String multipleConfigJson = "[{\"jsonPath\": \"$.root.field1\", \"type\": \"LONG\"}, {\"jsonPath\": \"$.root.field2\", \"type\": \"STRING\"}]";

        Map<String, Function<String, Object>> result = httpSinkSerializerJsonTypecastConfigConverter.convert(null, multipleConfigJson);
        Function<String, Object> mapper1 = result.get("$.root.field1");
        Function<String, Object> mapper2 = result.get("$.root.field2");

        Assertions.assertNotNull(mapper1);
        Assertions.assertNotNull(mapper2);
        Assertions.assertTrue(mapper1.apply("4") instanceof Long);
        Assertions.assertTrue(mapper2.apply("test") instanceof String);
    }

    @Test
    public void convertShouldThrowExceptionForMissingFieldsInConfig() {
        String missingFieldsConfigJson = "[{\"jsonPath\": \"$.root.field\"}]";

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> httpSinkSerializerJsonTypecastConfigConverter.convert(null, missingFieldsConfigJson));
    }
}
