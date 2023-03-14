package com.gotocompany.firehose.sink.jdbc;

import com.gotocompany.firehose.sink.jdbc.field.JdbcFieldFactory;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import lombok.Getter;

import java.util.Map;
import java.util.Properties;

/**
 * JdbcMapper transform field values.
 */
public class JdbcMapper {
    private String key;
    private Message message;
    private Properties protoToDbMapping;
    @Getter
    private Object columnValue;
    @Getter
    private Object column;
    private Descriptors.FieldDescriptor fieldDescriptor;

    /**
     * Instantiates a new Jdbc mapper.
     *
     * @param key              the key
     * @param message          the message
     * @param protoToDbMapping the proto to db mapping
     */
    public JdbcMapper(String key, Message message, Properties protoToDbMapping) {
        this.key = key;
        this.message = message;
        this.protoToDbMapping = protoToDbMapping;
    }

    /**
     * Initialize jdbc mapper.
     *
     * @return the jdbc mapper
     */
    public JdbcMapper initialize() {
        Integer protoIndex = Integer.valueOf(key);
        columnValue = getField(protoIndex);
        column = protoToDbMapping.get(key);
        fieldDescriptor = message.getDescriptorForType().findFieldByNumber(protoIndex);
        return this;
    }

    private Object getField(Integer protoIndex) {
        return message.getField(message.getDescriptorForType().findFieldByNumber(protoIndex));
    }

    /**
     * Add column to the input map.
     *
     * @param columnToValueMap the column to value map
     * @return the map
     */
    public Map<String, Object> add(Map<String, Object> columnToValueMap) {
        Object columnValueResult = JdbcFieldFactory
                .getField(this.columnValue, this.fieldDescriptor)
                .getColumn();
        columnToValueMap.put((String) column, columnValueResult);
        return columnToValueMap;
    }
}
