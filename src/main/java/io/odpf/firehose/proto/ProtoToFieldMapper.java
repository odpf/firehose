package io.odpf.firehose.proto;


import io.odpf.firehose.sink.jdbc.JdbcMapper;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.odpf.stencil.parser.ProtoParser;
import org.apache.http.util.Asserts;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Utility class to map fields in protobuf format to corresponding fields of a table in database.
 */
public class ProtoToFieldMapper {

    private ProtoParser protoParser;
    private Properties protoIndexToFieldMapping;

    /**
     * Instantiates a new Proto to field mapper.
     *
     * @param protoParser              the proto parser
     * @param protoIndexToFieldMapping the proto index to field mapping
     */
    public ProtoToFieldMapper(ProtoParser protoParser, Properties protoIndexToFieldMapping) {
        this.protoParser = protoParser;
        this.protoIndexToFieldMapping = protoIndexToFieldMapping;
    }

    /**
     * returns a map with column name of the target table in database as key and value of the field as the value field in the map.
     *
     * @param bytes byte array to access the fields from
     * @return a map containing mapping between the column name and the actual value for the column.
     */
    public Map<String, Object> getFields(byte[] bytes) {

        DynamicMessage dynamicMessage;
        try {
            dynamicMessage = protoParser.parse(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(e);
        }
        Map<String, Object> columnToValueMap = new HashMap<>();
        updateMapping(dynamicMessage, protoIndexToFieldMapping, columnToValueMap);
        return columnToValueMap;
    }

    private void updateMapping(Message message, Properties protoToDbMapping, Map<String, Object> columnToValueMap) {
        Enumeration<Object> keys = protoToDbMapping.keys();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            JdbcMapper jdbcMapper = new JdbcMapper(key, message, protoToDbMapping).initialize();
            Object column = jdbcMapper.getColumn();
            if (column instanceof String) {
                columnToValueMap = jdbcMapper.add(columnToValueMap);
            } else if (column instanceof Properties) {
                Asserts.check(jdbcMapper.getColumnValue() instanceof Message, "could not handle mapping");
                updateMapping((Message) jdbcMapper.getColumnValue(), (Properties) column, columnToValueMap);
            } else {
                throw new RuntimeException("column can either be properties or string");
            }
        }
    }
}
