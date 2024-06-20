package com.gotocompany.firehose.sink.http.factory;

import com.gotocompany.firehose.config.HttpSinkConfig;
import com.gotocompany.firehose.config.enums.HttpSinkDataFormatType;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.serializer.JsonWrappedProtoByte;
import com.gotocompany.firehose.serializer.MessageSerializer;
import com.gotocompany.firehose.serializer.MessageToJson;
import com.gotocompany.firehose.serializer.MessageToTemplatizedJson;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.firehose.serializer.TypecastedJsonSerializer;
import com.gotocompany.stencil.client.StencilClient;
import com.gotocompany.stencil.Parser;
import lombok.AllArgsConstructor;

/**
 * SerializerFactory create json serializer for proto using http sink config.
 */
@AllArgsConstructor
public class SerializerFactory {

    private HttpSinkConfig httpSinkConfig;
    private StencilClient stencilClient;
    private StatsDReporter statsDReporter;

    public MessageSerializer build() {
        FirehoseInstrumentation firehoseInstrumentation = new FirehoseInstrumentation(statsDReporter, SerializerFactory.class);
        if (isProtoSchemaEmpty() || httpSinkConfig.getSinkHttpDataFormat() == HttpSinkDataFormatType.PROTO) {
            firehoseInstrumentation.logDebug("Serializer type: JsonWrappedProtoByte");
            // Fallback to json wrapped proto byte
            return new JsonWrappedProtoByte();
        }

        if (httpSinkConfig.getSinkHttpDataFormat() == HttpSinkDataFormatType.JSON) {
            Parser protoParser = stencilClient.getParser(httpSinkConfig.getInputSchemaProtoClass());
            if (httpSinkConfig.getSinkHttpJsonBodyTemplate().isEmpty()) {
                firehoseInstrumentation.logDebug("Serializer type: EsbMessageToJson", HttpSinkDataFormatType.JSON);
                return getTypecastedJsonSerializer(new MessageToJson(protoParser, false, httpSinkConfig.getSinkHttpSimpleDateFormatEnable()));
            } else {
                firehoseInstrumentation.logDebug("Serializer type: EsbMessageToTemplatizedJson");
                return getTypecastedJsonSerializer(
                        MessageToTemplatizedJson.create(new FirehoseInstrumentation(statsDReporter, MessageToTemplatizedJson.class), httpSinkConfig.getSinkHttpJsonBodyTemplate(), protoParser));
            }
        }

        // Ideally this code will never be executed because getHttpSinkDataFormat() will return proto as default value.
        // This is required to satisfy compilation.

        firehoseInstrumentation.logDebug("Serializer type: JsonWrappedProtoByte");
        return new JsonWrappedProtoByte();
    }

    private MessageSerializer getTypecastedJsonSerializer(MessageSerializer messageSerializer) {
        return new TypecastedJsonSerializer(messageSerializer, httpSinkConfig);
    }

    private boolean isProtoSchemaEmpty() {
        return httpSinkConfig.getInputSchemaProtoClass() == null || httpSinkConfig.getInputSchemaProtoClass().equals("");
    }
}
