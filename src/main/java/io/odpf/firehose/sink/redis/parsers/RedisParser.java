package io.odpf.firehose.sink.redis.parsers;


import io.odpf.firehose.config.RedisSinkConfig;
import io.odpf.firehose.consumer.Message;
import io.odpf.firehose.sink.redis.dataentry.RedisDataEntry;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.stencil.parser.ProtoParser;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.errors.InvalidConfigurationException;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Convert kafka messages to RedisDataEntry.
 */
@AllArgsConstructor
public abstract class RedisParser {

    private ProtoParser protoParser;
    private RedisSinkConfig redisSinkConfig;

    public abstract List<RedisDataEntry> parse(Message message);

    public List<RedisDataEntry> parse(List<Message> messages) {
        return messages
                .stream()
                .map(this::parse)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    /**
     * Parse esb message to protobuf.
     *
     * @param message parsed message
     * @return Parsed Proto object
     */
    DynamicMessage parseEsbMessage(Message message) {
        DynamicMessage parsedMessage;
        try {
            parsedMessage = protoParser.parse(getPayload(message));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Unable to parse data when reading Key", e);
        }
        return parsedMessage;
    }

    /**
     * Parse template string.
     *
     * @param data     the data
     * @param template the template
     * @return parsed template
     */
    String parseTemplate(DynamicMessage data, String template) {
        if (StringUtils.isEmpty(template)) {
            throw new IllegalArgumentException("Template '" + template + "' is invalid");
        }
        String[] templateStrings = template.split(",");
        if (templateStrings.length == 0) {
            throw new InvalidConfigurationException("Empty key configuration: '" + template + "'");
        }
        templateStrings = Arrays
                .stream(templateStrings)
                .map(String::trim)
                .toArray(String[]::new);
        String templatePattern = templateStrings[0];
        String templateVariables = StringUtils.join(Arrays.copyOfRange(templateStrings, 1, templateStrings.length), ",");
        String renderedTemplate = renderStringTemplate(data, templatePattern, templateVariables);
        return StringUtils.isEmpty(templateVariables)
                ? templatePattern
                : renderedTemplate;
    }

    private String renderStringTemplate(DynamicMessage parsedMessage, String pattern, String patternVariables) {
        if (StringUtils.isEmpty(patternVariables)) {
            return pattern;
        }
        List<String> patternVariableFieldNumbers = Arrays.asList(patternVariables.split(","));
        Object[] patternVariableData = patternVariableFieldNumbers
                .stream()
                .map(fieldNumber -> getDataByFieldNumber(parsedMessage, fieldNumber))
                .toArray();
        return String.format(pattern, patternVariableData);
    }

    /**
     * Gets data by field number.
     *
     * @param parsedMessage the parsed message
     * @param fieldNumber   the field number
     * @return Data object
     */
    Object getDataByFieldNumber(DynamicMessage parsedMessage, String fieldNumber) {
        int fieldNumberInt;
        try {
            fieldNumberInt = Integer.parseInt(fieldNumber);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid Proto Index");
        }
        Descriptors.FieldDescriptor fieldDescriptor = parsedMessage.getDescriptorForType().findFieldByNumber(fieldNumberInt);
        if (fieldDescriptor == null) {
            throw new IllegalArgumentException(String.format("Descriptor not found for index: %s", fieldNumber));
        }
        return parsedMessage.getField(fieldDescriptor);
    }

    /**
     * Get payload bytes.
     *
     * @param message the message
     * @return binary payload
     */
    byte[] getPayload(Message message) {
        if (redisSinkConfig.getKafkaRecordParserMode().equals("key")) {
            return message.getLogKey();
        } else {
            return message.getLogMessage();
        }
    }
}
