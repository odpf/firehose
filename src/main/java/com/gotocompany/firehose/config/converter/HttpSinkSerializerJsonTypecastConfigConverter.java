package com.gotocompany.firehose.config.converter;

import com.gotocompany.firehose.serializer.constant.TypecastTarget;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.aeonbits.owner.Converter;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HttpSinkSerializerJsonTypecastConfigConverter implements Converter<Map<String, Function<String, Object>>> {

    private final ObjectMapper objectMapper;

    public HttpSinkSerializerJsonTypecastConfigConverter() {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public Map<String, Function<String, Object>> convert(Method method, String input) {
        if (StringUtils.isBlank(input)) {
            return Collections.emptyMap();
        }
        try {
            List<JsonTypecast> jsonTypecasts = objectMapper.readValue(input, new TypeReference<List<JsonTypecast>>() {
                    });
            validate(jsonTypecasts);
            return jsonTypecasts.stream()
                    .collect(Collectors.toMap(JsonTypecast::getJsonPath, jsonTypecast -> jsonTypecast.getType()::cast));
        } catch (IOException e) {
            throw new IllegalArgumentException("Error when parsing serializer json config: " + e.getMessage(), e);
        }
    }

    private void validate(List<JsonTypecast> jsonTypecasts) {
        boolean invalidConfigurationExist = jsonTypecasts.stream()
                .anyMatch(jt -> Objects.isNull(jt.getJsonPath()) || Objects.isNull(jt.getType()));
        if (invalidConfigurationExist) {
            throw new IllegalArgumentException("Invalid configuration: jsonPath or type should not be null");
        }
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    @Builder
    private static class JsonTypecast {
        private String jsonPath;
        private TypecastTarget type;
    }

}
