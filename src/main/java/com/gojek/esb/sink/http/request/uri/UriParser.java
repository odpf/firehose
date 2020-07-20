package com.gojek.esb.sink.http.request.uri;

import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.consumer.EsbMessage;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.errors.InvalidConfigurationException;

import java.util.Arrays;
import java.util.List;

public class UriParser {
    private ProtoParser protoParser;
    private String parserMode;

    public UriParser(ProtoParser protoParser, String parserMode) {
        this.protoParser = protoParser;
        this.parserMode = parserMode;
    }

    public String parse(EsbMessage esbMessage, String serviceUrl) {
        DynamicMessage parsedMessage = parseEsbMessage(esbMessage);
        return parseServiceUrl(parsedMessage, serviceUrl);

    }

    private DynamicMessage parseEsbMessage(EsbMessage esbMessage) {
        DynamicMessage parsedMessage;
        try {
            parsedMessage = protoParser.parse(getPayload(esbMessage));
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Unable to parse Service URL", e);
        }
        return parsedMessage;
    }

    private String parseServiceUrl(DynamicMessage data, String serviceUrl) {
        if (StringUtils.isEmpty(serviceUrl)) {
            throw new IllegalArgumentException("Service URL '" + serviceUrl + "' is invalid");
        }
        String[] urlStrings = serviceUrl.split(",");
        if (urlStrings.length == 0) {
            throw new InvalidConfigurationException("Empty Service URL configuration: '" + serviceUrl + "'");
        }
        urlStrings = Arrays
                .stream(urlStrings)
                .map(String::trim)
                .toArray(String[]::new);

        String urlPattern = urlStrings[0];
        String urlVariables = StringUtils.join(Arrays.copyOfRange(urlStrings, 1, urlStrings.length), ",");
        String renderedUrl = renderStringUrl(data, urlPattern, urlVariables);
        return StringUtils.isEmpty(urlVariables)
                ? urlPattern
                : renderedUrl;
    }

    private String renderStringUrl(DynamicMessage parsedMessage, String pattern, String patternVariables) {
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

    private Object getDataByFieldNumber(DynamicMessage parsedMessage, String fieldNumber) {
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

    private byte[] getPayload(EsbMessage esbMessage) {
        if (parserMode.equals("key")) {
            return esbMessage.getLogKey();
        } else {
            return esbMessage.getLogMessage();
        }
    }

}
