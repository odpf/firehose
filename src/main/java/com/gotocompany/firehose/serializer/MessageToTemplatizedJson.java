package com.gotocompany.firehose.serializer;


import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.exception.ConfigurationException;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.google.gson.Gson;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.gotocompany.stencil.Parser;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts kafka messages into Templatized json.
 */
public class MessageToTemplatizedJson implements MessageSerializer {
    private static final String TEMPLATE_PATH_REGEX = "\"\\$\\.[^\\s\\\\]*?\"";
    private static final String ALL_FIELDS_FROM_TEMPLATE = "\"$._all_\"";
    private final String httpSinkJsonBodyTemplate;
    private final Gson gson;
    private Parser protoParser;
    private HashSet<String> pathsToReplace;
    private JSONParser jsonParser;
    private FirehoseInstrumentation firehoseInstrumentation;

    public static MessageToTemplatizedJson create(FirehoseInstrumentation firehoseInstrumentation, String httpSinkJsonBodyTemplate, Parser protoParser) {
        MessageToTemplatizedJson messageToTemplatizedJson = new MessageToTemplatizedJson(firehoseInstrumentation, httpSinkJsonBodyTemplate, protoParser);
        if (messageToTemplatizedJson.isInvalidJson()) {
            throw new ConfigurationException("Given HTTPSink JSON body template :"
                    + httpSinkJsonBodyTemplate
                    + ", must be a valid JSON.");
        }
        messageToTemplatizedJson.setPathsFromTemplate();
        return messageToTemplatizedJson;
    }

    public MessageToTemplatizedJson(FirehoseInstrumentation firehoseInstrumentation, String httpSinkJsonBodyTemplate, Parser protoParser) {
        this.httpSinkJsonBodyTemplate = httpSinkJsonBodyTemplate;
        this.protoParser = protoParser;
        this.jsonParser = new JSONParser();
        this.gson = new Gson();
        this.firehoseInstrumentation = firehoseInstrumentation;
    }

    private void setPathsFromTemplate() {
        HashSet<String> paths = new HashSet<>();
        Pattern pattern = Pattern.compile(TEMPLATE_PATH_REGEX);
        Matcher matcher = pattern.matcher(httpSinkJsonBodyTemplate);
        while (matcher.find()) {
            paths.add(matcher.group(0));
        }
        List<String> pathList = new ArrayList<>(paths);
        firehoseInstrumentation.logDebug("\nPaths: {}", pathList);
        this.pathsToReplace = paths;
    }

    /**
     * Create json string from kafka message based on json Template.
     *
     * @param message the message
     * @return the string
     * @throws DeserializerException the deserializer exception
     */
    @Override
    public String serialize(Message message) throws DeserializerException {
        try {
            String jsonMessage;
            String jsonString;
            // only supports messages not keys
            DynamicMessage msg = protoParser.parse(message.getLogMessage());
            jsonMessage = JsonFormat.printer().includingDefaultValueFields().preservingProtoFieldNames().print(msg);
            String finalMessage = httpSinkJsonBodyTemplate;
            for (String path : pathsToReplace) {
                if (path.equals(ALL_FIELDS_FROM_TEMPLATE)) {
                    jsonString = jsonMessage;
                } else {
                    Object element = JsonPath.read(jsonMessage, path.replaceAll("\"", ""));
                    jsonString = gson.toJson(element);
                }
                finalMessage = finalMessage.replace(path, jsonString);
            }
            return finalMessage;
        } catch (InvalidProtocolBufferException | PathNotFoundException e) {
            throw new DeserializerException(e.getMessage());
        }
    }

    private boolean isInvalidJson() {
        try {
            jsonParser.parse(httpSinkJsonBodyTemplate);
        } catch (ParseException e) {
            return true;
        }
        return false;
    }
}
