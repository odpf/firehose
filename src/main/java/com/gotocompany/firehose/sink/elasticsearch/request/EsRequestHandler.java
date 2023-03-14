package com.gotocompany.firehose.sink.elasticsearch.request;

import com.gotocompany.firehose.config.enums.EsSinkMessageType;
import com.gotocompany.firehose.exception.JsonParseException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.serializer.MessageToJson;
import org.elasticsearch.action.DocWriteRequest;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.nio.charset.Charset;

public abstract class EsRequestHandler {
    private final EsSinkMessageType messageType;
    private final MessageToJson jsonSerializer;
    private final JSONParser jsonParser;

    public EsRequestHandler(EsSinkMessageType messageType, MessageToJson jsonSerializer) {
        this.messageType = messageType;
        this.jsonSerializer = jsonSerializer;
        this.jsonParser = new JSONParser();
    }

    public abstract boolean canCreate();

    public abstract DocWriteRequest getRequest(Message message);

    String extractPayload(Message message) {
        if (messageType.equals(EsSinkMessageType.PROTOBUF)) {
            return getFieldFromJSON(jsonSerializer.serialize(message), "logMessage");
        }
        return new String(message.getLogMessage(), Charset.defaultCharset());
    }

    String getFieldFromJSON(String jsonString, String key) {
        try {
            JSONObject parse = (JSONObject) jsonParser.parse(jsonString);
            Object valueAtKey = parse.get(key);
            if (valueAtKey == null) {
                throw new IllegalArgumentException("Key: " + key + " not found in ESB Message");
            }
            return valueAtKey.toString();
        } catch (ParseException e) {
            throw new JsonParseException(e.getMessage(), e.getCause());
        } finally {
            jsonParser.reset();
        }
    }

}
