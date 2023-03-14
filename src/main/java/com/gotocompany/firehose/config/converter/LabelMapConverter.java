package com.gotocompany.firehose.config.converter;

import org.aeonbits.owner.Converter;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;

public class LabelMapConverter implements Converter<Map<String, String>> {
    public static final String ELEMENT_SEPARATOR = ",";
    private static final String VALUE_SEPARATOR = "=";
    private static final int MAX_LENGTH = 63;

    public Map<String, String> convert(Method method, String input) {
        Map<String, String> result = new LinkedHashMap<>();
        String[] chunks = input.split(ELEMENT_SEPARATOR, -1);
        for (String chunk : chunks) {
            String[] entry = chunk.split(VALUE_SEPARATOR, -1);
            if (entry.length <= 1) {
                continue;
            }
            String key = entry[0].trim();
            if (key.isEmpty()) {
                continue;
            }

            String value = entry[1].trim();
            value = value.length() > MAX_LENGTH ? value.substring(0, MAX_LENGTH) : value;
            result.put(key, value);
        }
        return result;
    }
}

