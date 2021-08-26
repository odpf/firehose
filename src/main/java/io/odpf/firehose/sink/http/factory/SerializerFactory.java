package io.odpf.firehose.sink.http.factory;



import io.odpf.firehose.config.HttpSinkConfig;
import io.odpf.firehose.config.enums.HttpSinkDataFormatType;
import io.odpf.firehose.metrics.Instrumentation;
import io.odpf.firehose.metrics.StatsDReporter;
import io.odpf.firehose.serializer.MessageSerializer;
import io.odpf.firehose.serializer.MessageToJson;
import io.odpf.firehose.serializer.MessageToTemplatizedJson;
import io.odpf.firehose.serializer.JsonWrappedProtoByte;
import io.odpf.stencil.client.StencilClient;
import io.odpf.stencil.parser.ProtoParser;
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
        Instrumentation instrumentation = new Instrumentation(statsDReporter, SerializerFactory.class);
        if (isProtoSchemaEmpty() || httpSinkConfig.getSinkHttpDataFormat() == HttpSinkDataFormatType.PROTO) {
            instrumentation.logDebug("Serializer type: JsonWrappedProtoByte");
            // Fallback to json wrapped proto byte
            return new JsonWrappedProtoByte();
        }

        if (httpSinkConfig.getSinkHttpDataFormat() == HttpSinkDataFormatType.JSON) {
            ProtoParser protoParser = new ProtoParser(stencilClient, httpSinkConfig.getInputSchemaProtoClass());
            if (httpSinkConfig.getSinkHttpJsonBodyTemplate().isEmpty()) {
                instrumentation.logDebug("Serializer type: EsbMessageToJson", HttpSinkDataFormatType.JSON);
                return new MessageToJson(protoParser, false, true);
            } else {
                instrumentation.logDebug("Serializer type: EsbMessageToTemplatizedJson");
                return MessageToTemplatizedJson.create(new Instrumentation(statsDReporter, MessageToTemplatizedJson.class), httpSinkConfig.getSinkHttpJsonBodyTemplate(), protoParser);
            }
        }

        // Ideally this code will never be executed because getHttpSinkDataFormat() will return proto as default value.
        // This is required to satisfy compilation.

        instrumentation.logDebug("Serializer type: JsonWrappedProtoByte");
        return new JsonWrappedProtoByte();
    }

    private boolean isProtoSchemaEmpty() {
        return httpSinkConfig.getInputSchemaProtoClass() == null || httpSinkConfig.getInputSchemaProtoClass().equals("");
    }
}
