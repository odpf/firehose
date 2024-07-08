package com.gotocompany.firehose.evaluator;

import com.google.protobuf.Message;
import com.gotocompany.firehose.consumer.GenericError;
import com.gotocompany.firehose.consumer.GenericResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class GrpcResponseCELPayloadEvaluatorTest {

    private static final String CEL_EXPRESSION = "GenericResponse.success == false && GenericResponse.errors.exists(e, e.code == \"400\")";
    private PayloadEvaluator<Message> grpcPayloadEvaluator;

    @Before
    public void setup() {
        this.grpcPayloadEvaluator = new GrpcResponseCELPayloadEvaluator(GenericResponse.getDescriptor(), CEL_EXPRESSION);
    }

    @Test
    public void shouldEvaluateResponseToTrueWhenCELExpressionMatchesPayload() {
        GenericResponse genericResponse = GenericResponse.newBuilder()
                .setSuccess(false)
                .setDetail("Detail Message")
                .addErrors(GenericError.newBuilder()
                        .setCode("400")
                        .setEntity("GoFin")
                        .setCause("Unknown")
                        .build())
                .build();

        boolean result = grpcPayloadEvaluator.evaluate(genericResponse);

        Assertions.assertTrue(result);
    }

    @Test
    public void shouldEvaluateResponseToFalseWhenCELExpressionDoesntMatchPayload() {
        GenericResponse genericResponse = GenericResponse.newBuilder()
                .setSuccess(false)
                .setDetail("Detail Message")
                .addErrors(GenericError.newBuilder()
                        .setCode("50000")
                        .setEntity("GoFin")
                        .setCause("Unknown")
                        .build())
                .build();

        boolean result = grpcPayloadEvaluator.evaluate(genericResponse);

        Assertions.assertFalse(result);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenPayloadNotMatchingDescriptor() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> grpcPayloadEvaluator.evaluate(GenericError.newBuilder()
                        .setCause("Unknown")
                        .setCode("500")
                        .setEntity("GoFin")
                        .build()));
    }
}
