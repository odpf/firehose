package io.odpf.firehose.sink.bigquery.converter;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.firehose.sink.bigquery.converter.fields.NestedField;
import io.odpf.firehose.sink.bigquery.converter.fields.ProtoField;
import io.odpf.firehose.sink.bigquery.models.Constants;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
@AllArgsConstructor
public class RowMapper {

    private final Properties mapping;

    public Map<String, Object> map(DynamicMessage message) {
        if (mapping == null) {
            throw new RuntimeException("BQ_PROTO_COLUMN_MAPPING is not configured");
        }
        return getMappings(message, mapping);
    }

    private Map<String, Object> getMappings(DynamicMessage message, Properties columnMapping) {
        if (message == null || columnMapping == null || columnMapping.isEmpty()) {
            return new HashMap<>();
        }
        Descriptors.Descriptor descriptorForType = message.getDescriptorForType();

        Map<String, Object> row = new HashMap<>(columnMapping.size());
        columnMapping.forEach((key, value) -> {
            String columnName = value.toString();
            String column = key.toString();
            if (column.equals(Constants.Config.RECORD_NAME)) {
                return;
            }
            int protoIndex = Integer.parseInt(column);
            Descriptors.FieldDescriptor fieldDesc = descriptorForType.findFieldByNumber(protoIndex);
            if (fieldDesc != null && !message.getField(fieldDesc).toString().isEmpty()) {
                Object field = message.getField(fieldDesc);
                ProtoField protoField = FieldFactory.getField(fieldDesc, field);
                Object fieldValue = protoField.getValue();

                if (fieldValue instanceof List) {
                    addRepeatedFields(row, (String) key, value, (List<Object>) fieldValue);
                    return;
                }

                if (protoField.getClass().getName().equals(NestedField.class.getName())) {
                    try {
                        columnName = getNestedColumnName((Properties) value);
                        fieldValue = getMappings((DynamicMessage) field, (Properties) value);
                    } catch (Exception e) {
                        log.error("Exception::Handling nested field failure: {}", e.getMessage());
                        throw e;
                    }
                }
                row.put(columnName, fieldValue);
            }
        });
        return row;
    }

    private String getNestedColumnName(Properties value) {
        return value.get(Constants.Config.RECORD_NAME).toString();
    }

    private void addRepeatedFields(Map<String, Object> row, String key, Object value, List<Object> fieldValue) {
        if (fieldValue.isEmpty()) {
            return;
        }
        List<Object> repeatedNestedFields = new ArrayList<>();
        String columnName = null;
        for (Object f : fieldValue) {
            if (f instanceof DynamicMessage) {
                Properties nestedMappings = (Properties) value;
                repeatedNestedFields.add(getMappings((DynamicMessage) f, nestedMappings));
                columnName = getNestedColumnName(nestedMappings);
            } else {
                repeatedNestedFields.add(f);
                columnName = (String) value;
            }
        }
        row.put(columnName, repeatedNestedFields);
    }
}
