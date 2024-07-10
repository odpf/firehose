package com.gotocompany.firehose.config.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Map;

public class GrpcDynamicMetadataConverter implements Converter<Map<String, Object>> {

    @Override
    public Map<String, Object> convert(Method method, String s) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(s, new TypeReference<Map<String, Object>>() {});
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Invalid JSON string: " + s, e);
        }
    }

}
