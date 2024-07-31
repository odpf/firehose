package com.gotocompany.firehose.proto;

import com.gotocompany.firehose.consumer.GenericError;
import com.gotocompany.firehose.consumer.GenericResponse;
import io.grpc.Metadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.HashMap;
import java.util.Map;

public class ProtoToMetadataMapperTest {

    private ProtoToMetadataMapper protoToMetadataMapper;

    @Before
    public void setup() {
        Map<String, String> template = new HashMap<>();
        template.put("$GenericResponse.detail", "$GenericResponse.success");
        template.put("detail", "$GenericResponse.detail.lowerAscii()");
        template.put("someField", "someValue");
        template.put("$GenericResponse.success", "staticValue");
        template.put("staticKey", "$(GenericResponse.errors[0].cause + '-' + GenericResponse.errors[0].code + '-' + string(GenericResponse.code))");
        template.put("entity", "$GenericResponse.errors[0].entity");
        template.put("binding", "$cel.bind(code, GenericResponse.code, code + 100)");
        template.put("math", "$math.greatest(GenericResponse.code, 200)");
        this.protoToMetadataMapper = new ProtoToMetadataMapper(
                GenericResponse.getDescriptor(),
                template
        );
    }

    @Test
    public void shouldBuildDynamicMetadataWithCorrectPlaceholders() {
        GenericResponse payload = GenericResponse.newBuilder()
                .setSuccess(false)
                .setDetail("Detail_Of_Error")
                .setCode(100)
                .addErrors(GenericError.newBuilder()
                        .setCode("404")
                        .setCause("not_found")
                        .build())
                .build();

        Metadata metadata = protoToMetadataMapper.buildGrpcMetadata(payload.toByteArray());

        Assertions.assertTrue(metadata.containsKey(Metadata.Key.of("detail_of_error", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertEquals("false", metadata.get(Metadata.Key.of("detail_of_error", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertTrue(metadata.containsKey(Metadata.Key.of("detail", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertEquals("detail_of_error", metadata.get(Metadata.Key.of("detail", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertTrue(metadata.containsKey(Metadata.Key.of("statickey", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertEquals("not_found-404-100", metadata.get(Metadata.Key.of("statickey", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertTrue(metadata.containsKey(Metadata.Key.of("somefield", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertEquals("someValue", metadata.get(Metadata.Key.of("somefield", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertTrue(metadata.containsKey(Metadata.Key.of("entity", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertEquals("", metadata.get(Metadata.Key.of("entity", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertTrue(metadata.containsKey(Metadata.Key.of("false", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertEquals("staticValue", metadata.get(Metadata.Key.of("false", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertTrue(metadata.containsKey(Metadata.Key.of("binding", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertEquals("200", metadata.get(Metadata.Key.of("binding", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertTrue(metadata.containsKey(Metadata.Key.of("math", Metadata.ASCII_STRING_MARSHALLER)));
        Assertions.assertEquals("200", metadata.get(Metadata.Key.of("math", Metadata.ASCII_STRING_MARSHALLER)));
    }

    @Test
    public void shouldBuildEmptyMetadataWhenConfigurationIsEmpty() {
        this.protoToMetadataMapper = new ProtoToMetadataMapper(
                GenericResponse.getDescriptor(),
                new HashMap<>()
        );

        Metadata metadata = protoToMetadataMapper.buildGrpcMetadata(new byte[0]);

        Assertions.assertTrue(metadata.keys().isEmpty());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowOperationNotSupportedExceptionWhenMappedHeaderValueIsComplexType() {
        Map<String, String> template = new HashMap<>();
        template.put("$GenericResponse.detail", "$GenericResponse.success");
        template.put("staticKey", "$GenericResponse.errors");

        new ProtoToMetadataMapper(GenericResponse.getDescriptor(), template);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenConfigurationContainsUnregisteredExpression() {
        Map<String, String> template = new HashMap<>();
        template.put("$UnregisteredPayload.detail", "$GenericResponse.success");
        template.put("staticKey", "$(GenericResponse.errors[0].cause + GenericResponse.errors[0].code)");

        new ProtoToMetadataMapper(GenericResponse.getDescriptor(), template);
    }

}
