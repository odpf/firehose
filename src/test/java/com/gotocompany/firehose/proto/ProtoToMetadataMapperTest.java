package com.gotocompany.firehose.proto;

import com.gotocompany.firehose.consumer.GenericError;
import com.gotocompany.firehose.consumer.GenericResponse;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class ProtoToMetadataMapperTest {

    private static final String TEMPLATE = "{\"$GenericResponse.detail\": \"$GenericResponse.success\"}";


    @Test
    public void test() throws IOException {
        ProtoToMetadataMapper protoToMetadataMapper = new ProtoToMetadataMapper(
                GenericResponse.getDescriptor(),
                TEMPLATE
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
        Map<String, Object> metadataValues = protoToMetadataMapper.buildGrpcMetadata(payload);
        System.out.println(metadataValues);
    }
}
