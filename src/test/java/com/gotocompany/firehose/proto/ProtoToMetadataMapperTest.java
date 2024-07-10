package com.gotocompany.firehose.proto;

import com.gotocompany.firehose.consumer.GenericError;
import com.gotocompany.firehose.consumer.GenericResponse;
import io.grpc.Metadata;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ProtoToMetadataMapperTest {

    @Test
    public void test() throws IOException {
        Map<String, Object> template = new HashMap<>();
        template.put("$GenericResponse.detail", "$GenericResponse.success");
        template.put("staticKey", "$(GenericResponse.errors[0].cause + GenericResponse.errors[0].code)");
        template.put("shouldBeBoolean", true);
        ProtoToMetadataMapper protoToMetadataMapper = new ProtoToMetadataMapper(
                GenericResponse.getDescriptor(),
                template
        );
        GenericResponse payload = GenericResponse.newBuilder()
                .setSuccess(false)
                .setDetail("detail_of_error")
                .addErrors(GenericError.newBuilder()
                        .setCode("404")
                        .setCause("Not Found")
                        .setEntity("GTF")
                        .build())
                .build();
        Metadata metadata = protoToMetadataMapper.buildGrpcMetadata(payload);
        System.out.println(metadata);
    }
}
