package com.gotocompany.firehose.sink.mongodb.request;

import com.gotocompany.firehose.config.enums.MongoSinkMessageType;
import com.gotocompany.firehose.config.enums.MongoSinkRequestType;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.serializer.MessageToJson;
import com.mongodb.client.model.ReplaceOneModel;
import org.bson.Document;
import org.json.simple.JSONObject;

/**
 * The Mongo update request handler.
 * This class is responsible for creating requests when one
 * or more fields of a MongoDB document need to be updated.
 */
public class MongoUpdateRequestHandler extends MongoRequestHandler {

    private final MongoSinkRequestType mongoSinkRequestType;
    private final String mongoPrimaryKey;

    /**
     * Instantiates a new Mongo update request handler.
     *
     * @param messageType          the message type
     * @param jsonSerializer       the json serializer
     * @param mongoSinkRequestType the mongo sink request type
     * @param mongoPrimaryKey      the mongo primary key
     * @since 0.1
     */
    public MongoUpdateRequestHandler(MongoSinkMessageType messageType, MessageToJson jsonSerializer, MongoSinkRequestType mongoSinkRequestType, String mongoPrimaryKey, String kafkaRecordParserMode) {
        super(messageType, jsonSerializer, kafkaRecordParserMode);
        this.mongoSinkRequestType = mongoSinkRequestType;
        this.mongoPrimaryKey = mongoPrimaryKey;
    }

    @Override
    public boolean canCreate() {
        return mongoSinkRequestType == MongoSinkRequestType.UPDATE_ONLY;
    }

    @Override
    public ReplaceOneModel<Document> getRequest(Message message) {
        String logMessage = extractPayload(message);
        JSONObject logMessageJSONObject = getJSONObject(logMessage);
        String primaryKeyValue;

        primaryKeyValue = getFieldFromJSON(logMessageJSONObject, mongoPrimaryKey);
        Document document = new Document("_id", primaryKeyValue);
        document.putAll(logMessageJSONObject);

        return new ReplaceOneModel<>(new Document("_id", primaryKeyValue), document);
    }
}
