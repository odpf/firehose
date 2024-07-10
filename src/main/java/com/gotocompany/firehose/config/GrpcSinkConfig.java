package com.gotocompany.firehose.config;

import com.gotocompany.firehose.config.converter.GrpcDynamicMetadataConverter;
import com.gotocompany.firehose.config.converter.GrpcMetadataConverter;
import io.grpc.Metadata;
import org.aeonbits.owner.Config;

import java.util.Map;


public interface GrpcSinkConfig extends AppConfig {

    @Config.Key("SINK_GRPC_SERVICE_HOST")
    String getSinkGrpcServiceHost();

    @Config.Key("SINK_GRPC_SERVICE_PORT")
    Integer getSinkGrpcServicePort();

    @Config.Key("SINK_GRPC_METHOD_URL")
    String getSinkGrpcMethodUrl();

    @Config.Key("SINK_GRPC_RESPONSE_SCHEMA_PROTO_CLASS")
    String getSinkGrpcResponseSchemaProtoClass();

    @Config.Key("SINK_GRPC_ARG_KEEPALIVE_TIME_MS")
    @Config.DefaultValue("9223372036854775807")
    Long getSinkGrpcArgKeepaliveTimeMS();

    @Config.Key("SINK_GRPC_ARG_KEEPALIVE_TIMEOUT_MS")
    @DefaultValue("20000")
    Long getSinkGrpcArgKeepaliveTimeoutMS();

    @Config.Key("SINK_GRPC_ARG_DEADLINE_MS")
    Long getSinkGrpcArgDeadlineMS();

    @Config.Key("SINK_GRPC_RESPONSE_RETRY_CEL_EXPRESSION")
    @DefaultValue("")
    String getSinkGrpcResponseRetryCELExpression();

    @Key("SINK_GRPC_METADATA")
    @DefaultValue("")
    @ConverterClass(GrpcMetadataConverter.class)
    Metadata getSinkGrpcMetadata();

    @Key("SINK_GRPC_DYNAMIC_METADATA_TEMPLATE")
    @DefaultValue("{}")
    @ConverterClass(GrpcDynamicMetadataConverter.class)
    Map<String, Object> getSinkGrpcDynamicMetadata();
}
