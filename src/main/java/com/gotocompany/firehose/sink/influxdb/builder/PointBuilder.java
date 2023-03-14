package com.gotocompany.firehose.sink.influxdb.builder;

import com.gotocompany.firehose.config.InfluxSinkConfig;
import com.gotocompany.firehose.exception.ConfigurationException;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import org.influxdb.dto.Point;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Point builder for influx metrics.
 */
public class PointBuilder {
    public static final String FIELD_NAME_MAPPING_ERROR_MESSAGE = "field index mapping cannot be empty; at least one field value is required";
    private static final long SECONDS_SCALED_TO_MILLI = 1000L;
    private static final long MILLIS_SCALED_TO_NANOS = 1000000L;

    private Point.Builder pointBuilder;
    private Properties tagNameProtoIndexMapping;
    private Properties fieldNameProtoIndexMapping;
    private String measurementName;
    private Integer timeStampIndex;

    /**
     * Instantiates a new Point builder.
     *
     * @param config the config
     */
    public PointBuilder(InfluxSinkConfig config) {
        tagNameProtoIndexMapping = config.getSinkInfluxTagNameProtoIndexMapping();
        fieldNameProtoIndexMapping = config.getSinkInfluxFieldNameProtoIndexMapping();
        this.timeStampIndex = config.getSinkInfluxProtoEventTimestampIndex();
        this.measurementName = config.getSinkInfluxMeasurementName();
    }

    public Point buildPoint(DynamicMessage message) throws InvalidProtocolBufferException {
        this.pointBuilder = Point.measurement(measurementName);
        addTagsToPoint(message, tagNameProtoIndexMapping);
        addFieldsToPoint(message, fieldNameProtoIndexMapping);
        Timestamp timestamp = getTimestamp(message, timeStampIndex);
        pointBuilder.time(getMillisFromTimestamp(timestamp), TimeUnit.MILLISECONDS);
        return pointBuilder.build();
    }

    private Timestamp getTimestamp(Message message, Integer fieldIndex) throws InvalidProtocolBufferException {
        DynamicMessage timestamp = (DynamicMessage) getField(message, fieldIndex);
        return Timestamp.parseFrom(timestamp.toByteArray());
    }

    private void addTagsToPoint(Message message, Properties protoIndexMapping) throws InvalidProtocolBufferException {
        for (Object protoFieldIndex : protoIndexMapping.keySet()) {
            int fieldIndex = Integer.parseInt((String) protoFieldIndex);
            Object tagValue = getField(message, fieldIndex);
            Object tag = protoIndexMapping.get(protoFieldIndex);
            Descriptors.FieldDescriptor fieldDescriptor = getFieldByIndex(message, fieldIndex);
            if (fieldIsOfMessageType(fieldDescriptor, Timestamp.getDescriptor())
                    || fieldIsOfMessageType(fieldDescriptor, Duration.getDescriptor())) {
                pointBuilder.tag((String) tag, getMillisFromTimestamp(getTimestamp(message, fieldIndex)).toString());
            } else if (tag instanceof String) {
                pointBuilder.tag((String) tag, tagValue.toString());
            } else if (tag instanceof Properties) {
                addTagsToPoint((Message) tagValue, (Properties) tag);
            } else {
                throw new RuntimeException("column can either be properties or string");
            }
        }
    }

    private void addFieldsToPoint(Message message, Properties protoIndexMapping) throws InvalidProtocolBufferException {
        if (protoIndexMapping.isEmpty()) {
            throw new ConfigurationException(FIELD_NAME_MAPPING_ERROR_MESSAGE);
        }
        Map<String, Object> fieldNameValueMap = new HashMap<>();
        for (Object protoFieldIndex : protoIndexMapping.keySet()) {
            int fieldIndex = Integer.parseInt((String) protoFieldIndex);
            Object field = protoIndexMapping.get(protoFieldIndex);

            if (field instanceof String) {
                Descriptors.FieldDescriptor fieldDescriptor = getFieldByIndex(message, fieldIndex);
                if (fieldIsOfMessageType(fieldDescriptor, Timestamp.getDescriptor())
                        || fieldIsOfMessageType(fieldDescriptor, Duration.getDescriptor())) {
                    fieldNameValueMap.put((String) field, getMillisFromTimestamp(getTimestamp(message, fieldIndex)));
                } else if (fieldIsOfEnumType(fieldDescriptor)) {
                    fieldNameValueMap.put((String) field, getField(message, fieldIndex).toString());
                } else {
                    fieldNameValueMap.put((String) field, getField(message, fieldIndex));
                }
            } else if (field instanceof Properties) {
                addFieldsToPoint((Message) getField(message, fieldIndex), (Properties) field);
            } else {
                throw new RuntimeException("column can either be properties or string");
            }
        }
        pointBuilder.fields(fieldNameValueMap);
    }

    private boolean fieldIsOfMessageType(Descriptors.FieldDescriptor fieldDescriptor, Descriptors.Descriptor typeDescriptor) {
        return fieldDescriptor.getType().name().equals("MESSAGE")
                && fieldDescriptor.getMessageType().getFullName().equals(typeDescriptor.getFullName()
        );
    }

    private boolean fieldIsOfEnumType(Descriptors.FieldDescriptor fieldDescriptor) {
        return fieldDescriptor.getType().name().equals("ENUM");
    }

    private Object getField(Message message, int protoIndex) {
        return message.getField(getFieldByIndex(message, protoIndex));
    }

    private Descriptors.FieldDescriptor getFieldByIndex(Message message, int protoIndex) {
        return message.getDescriptorForType().findFieldByNumber(protoIndex);
    }

    private Long getMillisFromTimestamp(Timestamp timestamp) {
        Long millisFromSeconds = timestamp.getSeconds() * SECONDS_SCALED_TO_MILLI;
        Long millisFromNanos = timestamp.getNanos() / MILLIS_SCALED_TO_NANOS;
        return millisFromSeconds + millisFromNanos;
    }

}
