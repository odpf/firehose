package com.gotocompany.firehose.sink.jdbc.field;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.List;

/**
 * Jdbc map field.
 */
public class JdbcMapField implements JdbcField {
    private Object columnValue;
    private Descriptors.FieldDescriptor fieldDescriptor;


    public JdbcMapField(Object columnValue, Descriptors.FieldDescriptor fieldDescriptor) {
        this.columnValue = columnValue;
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public Object getColumn() throws RuntimeException {
        HashMap<String, Object> columnFields = new HashMap<>();
        List<DynamicMessage> values = (List<DynamicMessage>) this.columnValue;
        for (DynamicMessage dynamicMessage : values) {
            Object[] data = dynamicMessage.getAllFields().values().toArray();
            Object mapValue = data.length > 1 ? data[1] : "";
            columnFields.put((String) data[0], mapValue);
        }
        String columnEntry = JSONObject.toJSONString(columnFields);
        return columnEntry;
    }

    @Override
    public boolean canProcess() {
        return fieldDescriptor.isMapField();
    }
}
