package com.gotocompany.firehose.sink.prometheus.builder;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.gotocompany.firehose.sink.prometheus.PromSinkConstants;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class TimeSeriesBuilderUtils {

    public static Set<PrometheusMetric> getMetricsFromMessage(Message message, Properties metricNameProtoIndexMapping) {
        Set<PrometheusMetric> metrics = new HashSet<>();
        metricNameProtoIndexMapping.forEach((metricIndex, metricKey) -> {
            int fieldIndex = Integer.parseInt((String) metricIndex);
            Object metricValue = getField(message, fieldIndex);
            if (metricKey instanceof String) {
                metrics.add(new PrometheusMetric(metricKey.toString(), Double.parseDouble(metricValue.toString())));
            } else if (metricKey instanceof Properties) {
                metrics.addAll(getMetricsFromMessage((Message) metricValue, (Properties) metricKey));
            }
        });
        return metrics;
    }
    /**
     * @param message                    Protobuf message to read values from
     * @param labelNameProtoIndexMapping mapping of index to metric name
     * @param partition                  kafka partition to be set as default label
     * @return Set of Labels
     */

    public static Set<PrometheusLabel> getLabelsFromMessage(Message message, Properties labelNameProtoIndexMapping, int partition) {
        Set<PrometheusLabel> labels = new HashSet<>();
        labels.add(new PrometheusLabel(PromSinkConstants.KAFKA_PARTITION, String.valueOf(partition)));
        labels.addAll(getLabelsFromMessage(message, labelNameProtoIndexMapping));
        return labels;
    }

    private static Set<PrometheusLabel> getLabelsFromMessage(Message message, Properties labelNameProtoIndexMapping) {
        Set<PrometheusLabel> labels = new HashSet<>();
        labelNameProtoIndexMapping.forEach((labelIndex, labelKey) -> {
            int fieldIndex = Integer.parseInt((String) labelIndex);
            Object labelValue = getField(message, fieldIndex);
            if (labelKey instanceof String) {
                labels.add(new PrometheusLabel(labelKey.toString(), getFieldValueString(message, fieldIndex)));
            } else if (labelKey instanceof Properties) {
                labels.addAll(getLabelsFromMessage((Message) labelValue, (Properties) labelKey));
            }
        });
        return labels;
    }

    private static String getFieldValueString(Message message, int fieldIndex) {
        try {
            Descriptors.FieldDescriptor fieldDescriptor = getFieldByIndex(message, fieldIndex);
            if (fieldIsOfMessageType(fieldDescriptor, Timestamp.getDescriptor())
                    || fieldIsOfMessageType(fieldDescriptor, Duration.getDescriptor())) {
                return getMillisFromTimestamp(getTimestamp(message, fieldIndex)).toString();
            } else {
                return getField(message, fieldIndex).toString();
            }
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Timestamp that will be use as metric timestamp.
     * the timestamp from message or from current timestamp base on prometheus config
     *
     * @param message the protobuf message
     * @return the unix timestamp
     * @throws InvalidProtocolBufferException the exception on invalid protobuf
     */
    public static Long getMetricTimestamp(Message message, boolean isEventTimestampEnabled, int timestampIndex) throws InvalidProtocolBufferException {
        return (isEventTimestampEnabled) ? getMillisFromTimestamp(getTimestamp(message, timestampIndex)) : System.currentTimeMillis();
    }

    private static boolean fieldIsOfMessageType(Descriptors.FieldDescriptor fieldDescriptor, Descriptors.Descriptor typeDescriptor) {
        return fieldDescriptor.getType().name().equals("MESSAGE")
                && fieldDescriptor.getMessageType().getFullName().equals(typeDescriptor.getFullName()
        );
    }

    private static Timestamp getTimestamp(Message message, Integer fieldIndex) throws InvalidProtocolBufferException {
        DynamicMessage timestamp = (DynamicMessage) getField(message, fieldIndex);
        return Timestamp.parseFrom(timestamp.toByteArray());
    }

    private static Object getField(Message message, int protoIndex) {
        return message.getField(getFieldByIndex(message, protoIndex));
    }

    private static Descriptors.FieldDescriptor getFieldByIndex(Message message, int protoIndex) {
        return message.getDescriptorForType().findFieldByNumber(protoIndex);
    }

    private static Long getMillisFromTimestamp(Timestamp timestamp) {
        Long millisFromSeconds = timestamp.getSeconds() * PromSinkConstants.SECONDS_SCALED_TO_MILLI;
        Long millisFromNanos = timestamp.getNanos() / PromSinkConstants.MILLIS_SCALED_TO_NANOS;
        return millisFromSeconds + millisFromNanos;
    }
}
