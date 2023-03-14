package com.gotocompany.firehose.sink.elasticsearch.request;

import com.gotocompany.firehose.config.EsSinkConfig;
import com.gotocompany.firehose.config.enums.EsSinkMessageType;
import com.gotocompany.firehose.config.enums.EsSinkRequestType;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.serializer.MessageToJson;
import lombok.AllArgsConstructor;

import java.util.ArrayList;

@AllArgsConstructor
public class EsRequestHandlerFactory {

    private EsSinkConfig esSinkConfig;
    private FirehoseInstrumentation firehoseInstrumentation;
    private final String esIdFieldName;
    private final EsSinkMessageType messageType;
    private final MessageToJson jsonSerializer;
    private final String esTypeName;
    private final String esIndexName;
    private final String esRoutingKeyName;

    public EsRequestHandler getRequestHandler() {
        EsSinkRequestType esSinkRequestType = esSinkConfig.isSinkEsModeUpdateOnlyEnable() ? EsSinkRequestType.UPDATE_ONLY : EsSinkRequestType.INSERT_OR_UPDATE;
        firehoseInstrumentation.logInfo("ES request mode: {}", esSinkRequestType);

        ArrayList<EsRequestHandler> esRequestHandlers = new ArrayList<>();
        esRequestHandlers.add(new EsUpdateRequestHandler(messageType, jsonSerializer, esTypeName, esIndexName, esSinkRequestType, esIdFieldName, esRoutingKeyName));
        esRequestHandlers.add(new EsUpsertRequestHandler(messageType, jsonSerializer, esTypeName, esIndexName, esSinkRequestType, esIdFieldName, esRoutingKeyName));

        return esRequestHandlers
                .stream()
                .filter(EsRequestHandler::canCreate)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Es Request Type " + esSinkRequestType.name() + " not supported"));
    }
}
