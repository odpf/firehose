package com.gotocompany.firehose.config.converter;

import io.grpc.Metadata;
import org.junit.Test;

import java.util.Arrays;

import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class GrpcMetadataConverterTest {

    @Test
    public void shouldConvertConfigToMetadata() {
        Metadata actualMetadata = new GrpcMetadataConverter().convert(null, "k1:v1,k2:v2, k3:v3");
        assertEquals(Arrays.asList("k1", "k2", "k3").stream().collect(Collectors.toSet()), actualMetadata.keys());
    }


    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowOnIllegalArgument() {
        new GrpcMetadataConverter().convert(null, "k1:v1,k2:v2,k3:v3,k4");
    }
}
