package com.gotocompany.firehose.sink.mongodb.request;

import com.gotocompany.firehose.config.enums.MongoSinkMessageType;
import com.gotocompany.firehose.exception.JsonParseException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.serializer.MessageToJson;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.nio.charset.Charset;

/**
 * The abstract class Mongo request handler. This class is responsible for
 * deserializing the raw message byte array, extracting the logMessage from
 * the consumed record and creating a WriteModel request for the message
 */
public abstract class MongoRequestHandler {
    private final MongoSinkMessageType messageType;
    private final MessageToJson jsonSerializer;
    private final JSONParser jsonParser;
    private final String kafkaRecordParserMode;

    /**
     * Instantiates a new Mongo request handler.
     *
     * @param messageType    the message type, i.e JSON/Protobuf
     * @param jsonSerializer the json serializer
     * @since 0.1
     */
    public MongoRequestHandler(MongoSinkMessageType messageType, MessageToJson jsonSerializer, String kafkaRecordParserMode) {
        this.messageType = messageType;
        this.jsonSerializer = jsonSerializer;
        this.jsonParser = new JSONParser();
        this.kafkaRecordParserMode = kafkaRecordParserMode;
    }

    /**
     * Check if can create the specified request type or not.
     * This method returns true if the Mongo request type parameter
     * matches the class of request handler object, i.e. -
     * UPDATE_ONLY for UpdateRequestHandler
     * INSERT_OR_UPDATE for UpsertRequestHandler
     *
     * @return true if the specified request type can be created otherwise false
     * @since 0.1
     */
    public abstract boolean canCreate();

    /**
     * This method creates a MongoDB WriteModel request for the provided
     * Message, after serializing and extracting the logMessage from
     * the raw byte array of the Message.
     *
     * @param message the message
     * @return the request
     * @since 0.1
     */
    public abstract WriteModel<Document> getRequest(Message message);

    /**
     * This method returns the JSON string parsed from the input message.
     * If the input message type is Protobuf, then the raw Protobuf byte
     * array of the log message is first serialized to JSON and then
     * the value stored in the logMessage key is returned.
     * If the input message type is JSON, then the raw JSON byte array,
     * is first deserialized and the then the value stored in the
     * logMessage key is returned.
     *
     * @param message the message
     * @return the JSON string parsed from the message
     * @since 0.1
     */
    protected String extractPayload(Message message) {

        if (messageType.equals(MongoSinkMessageType.PROTOBUF)) {
            JSONObject messageJSONObject = getJSONObject(jsonSerializer.serialize(message));
            return getFieldFromJSON(messageJSONObject, kafkaRecordParserMode.equals("key") ? "logKey" : "logMessage");
        }
        return new String(kafkaRecordParserMode.equals("key") ? message.getLogKey() : message.getLogMessage(), Charset.defaultCharset());
    }

    /**
     * This method extracts the value of the required key from the
     * provided JSONObject.
     *
     * @param jsonObject the JSONObject
     * @param key        the key whose value is to be extracted from the JSONObject
     * @return the field from json
     * @throws IllegalArgumentException if the key whose value is requested
     *                                  is null, in the JSONObject
     * @since 0.1
     */
    protected String getFieldFromJSON(JSONObject jsonObject, String key) {
        if (key == null) {
            throw new NullPointerException("Key cannot be null");
        }
        Object valueAtKey = jsonObject.get(key);
        if (valueAtKey == null) {
            throw new IllegalArgumentException("Key: " + key + " not found in ESB Message");
        }
        return valueAtKey.toString();
    }

    /**
     * This method parses the JSON string to the JSON object.
     *
     * @param jsonString the json string
     * @return the json object
     * @throws JsonParseException if the JSON string provided is invalid or
     *                            contains incorrect JSON syntax. The Exception will be initialized
     *                            with the details and cause of the error which caused the exception
     *                            to be thrown.
     * @since 0.1
     */
    protected JSONObject getJSONObject(String jsonString) {
        try {
            return (JSONObject) jsonParser.parse(jsonString);
        } catch (ParseException e) {
            throw new JsonParseException(e.getMessage(), e.getCause());
        } finally {
            jsonParser.reset();
        }
    }
}
