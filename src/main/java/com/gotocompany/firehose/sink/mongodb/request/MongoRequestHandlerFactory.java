package com.gotocompany.firehose.sink.mongodb.request;

import com.gotocompany.firehose.config.MongoSinkConfig;
import com.gotocompany.firehose.config.enums.MongoSinkMessageType;
import com.gotocompany.firehose.config.enums.MongoSinkRequestType;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.serializer.MessageToJson;
import lombok.AllArgsConstructor;

import java.util.ArrayList;

/**
 * The Mongo request handler factory.
 */
@AllArgsConstructor
public class MongoRequestHandlerFactory {

    private final MongoSinkConfig mongoSinkConfig;
    private final FirehoseInstrumentation firehoseInstrumentation;
    private final String mongoPrimaryKey;
    private final MongoSinkMessageType messageType;
    private final MessageToJson jsonSerializer;

    /**
     * Gets request handler. This method returns the MongoDB update/upsert
     * request handler according to the value specified by the environment
     * variable SINK_MONGO_MODE_UPDATE_ONLY_ENABLE.
     * UpdateRequestHandler is returned if the SINK_MONGO_MODE_UPDATE_ONLY_ENABLE
     * is set to UPDATE_ONLY
     * UpsertRequestHandler is returned if the SINK_MONGO_MODE_UPDATE_ONLY_ENABLE
     * is set to INSERT_OR_UPDATE
     *
     * @return the MongoRequestHandler
     * @throws IllegalArgumentException if the sink type parameter specified in
     *                                  MongoSinkConfig, i.e. the value specified by the MongoRequestType
     *                                  is any value other than UPDATE_ONLY and INSERT_OR_UPDATE.
     * @since 0.1
     */
    public MongoRequestHandler getRequestHandler() {

        String kafkaRecordParserMode = mongoSinkConfig.getKafkaRecordParserMode();
        if (!kafkaRecordParserMode.equals("key") && !kafkaRecordParserMode.equals("message")) {
            throw new IllegalArgumentException("KAFKA_RECORD_PARSER_MODE should be key/message");
        }
        MongoSinkRequestType mongoSinkRequestType = mongoSinkConfig.isSinkMongoModeUpdateOnlyEnable() ? MongoSinkRequestType.UPDATE_ONLY : MongoSinkRequestType.UPSERT;
        firehoseInstrumentation.logInfo("Mongo request mode: {}", mongoSinkRequestType);
        if (mongoSinkRequestType == MongoSinkRequestType.UPDATE_ONLY && mongoPrimaryKey == null) {
            throw new IllegalArgumentException("Primary Key cannot be null in Update-Only mode");
        }

        ArrayList<MongoRequestHandler> mongoRequestHandlers = new ArrayList<>();
        mongoRequestHandlers.add(new MongoUpdateRequestHandler(messageType, jsonSerializer, mongoSinkRequestType, mongoPrimaryKey, kafkaRecordParserMode));
        mongoRequestHandlers.add(new MongoUpsertRequestHandler(messageType, jsonSerializer, mongoSinkRequestType, mongoPrimaryKey, kafkaRecordParserMode));
        return mongoRequestHandlers
                .stream()
                .filter(MongoRequestHandler::canCreate)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Mongo Request Type " + mongoSinkRequestType.name() + " not supported"));
    }
}
