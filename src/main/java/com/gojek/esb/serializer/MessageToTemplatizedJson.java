package com.gojek.esb.serializer;

import com.gojek.de.stencil.parser.ProtoParser;
import com.gojek.esb.consumer.Message;
import com.gojek.esb.exception.DeserializerException;
import com.gojek.esb.exception.EglcConfigurationException;
import com.gojek.esb.metrics.Instrumentation;
import com.google.gson.Gson;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MessageToTemplatizedJson implements MessageSerializer {
    private static final String TEMPLATE_PATH_REGEX = "\"\\$\\.[^\\s\\\\]*?\"";
    private static final String ALL_FIELDS_FROM_TEMPLATE = "\"$._all_\"";
    private final String httpSinkJsonBodyTemplate;
    private final Gson gson;
    private ProtoParser protoParser;
    private HashSet<String> pathsToReplace;
    private JSONParser jsonParser;
    private Instrumentation instrumentation;

    public static MessageToTemplatizedJson create(Instrumentation instrumentation, String httpSinkJsonBodyTemplate, ProtoParser protoParser) {
        MessageToTemplatizedJson esbMessageToTemplatizedJson = new MessageToTemplatizedJson(instrumentation, httpSinkJsonBodyTemplate, protoParser);
        if (esbMessageToTemplatizedJson.isInvalidJson()) {
            throw new EglcConfigurationException("Given HTTPSink JSON body template :"
                    + httpSinkJsonBodyTemplate
                    + ", must be a valid JSON.");
        }
        esbMessageToTemplatizedJson.setPathsFromTemplate();
        return esbMessageToTemplatizedJson;
    }

    public MessageToTemplatizedJson(Instrumentation instrumentation, String httpSinkJsonBodyTemplate, ProtoParser protoParser) {
        this.httpSinkJsonBodyTemplate = httpSinkJsonBodyTemplate;
        this.protoParser = protoParser;
        this.jsonParser = new JSONParser();
        this.gson = new Gson();
        this.instrumentation = instrumentation;
    }

    private void setPathsFromTemplate() {
        HashSet<String> paths = new HashSet<>();
        Pattern pattern = Pattern.compile(TEMPLATE_PATH_REGEX);
        Matcher matcher = pattern.matcher(httpSinkJsonBodyTemplate);
        while (matcher.find()) {
            paths.add(matcher.group(0));
        }
        List<String> pathList = new ArrayList<>(paths);
        instrumentation.logDebug("\nPaths: {}", pathList);
        this.pathsToReplace = paths;
    }

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
