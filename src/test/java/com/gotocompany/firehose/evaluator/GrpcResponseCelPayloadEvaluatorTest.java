package com.gotocompany.firehose.evaluator;

import com.google.protobuf.Message;
import com.gotocompany.firehose.consumer.GenericError;
import com.gotocompany.firehose.consumer.GenericResponse;
import com.gotocompany.firehose.consumer.TestMessage;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class GrpcResponseCelPayloadEvaluatorTest {

    private static final String CEL_EXPRESSION = "GenericResponse.success == false && GenericResponse.errors.exists(e, e.code == \"400\")";
    private PayloadEvaluator<Message> grpcPayloadEvaluator;

    @Before
    public void setup() {
        this.grpcPayloadEvaluator = new GrpcResponseCelPayloadEvaluator(GenericResponse.getDescriptor(), CEL_EXPRESSION);
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
    public void shouldEvaluateResponseToTrueWhenCelExpressionMatchesRange() {
        PayloadEvaluator<Message> evaluator = forCELExpression("GenericResponse.errors.exists(e, int(e.code) >= 1000 && int(e.code) <= 2000)");
        GenericResponse genericResponse = GenericResponse.newBuilder()
                .setSuccess(false)
                .setDetail("Detail Message")
                .addErrors(GenericError.newBuilder()
                        .setCode("1500")
                        .setEntity("GoFin")
                        .setCause("Unknown")
                        .build())
                .build();

        boolean result = evaluator.evaluate(genericResponse);

        Assertions.assertTrue(result);
    }

    @Test
    public void shouldEvaluateResponseToFalseWhenCelExpressionMatchesRangeAndNotInSet() {
        PayloadEvaluator<Message> evaluator = forCELExpression("GenericResponse.errors.exists(e, int(e.code) >= 1000 && int(e.code) <= 2000 && !(int(e.code) in [1500, 1600]))");
        GenericResponse genericResponse = GenericResponse.newBuilder()
                .setSuccess(false)
                .setDetail("Detail Message")
                .addErrors(GenericError.newBuilder()
                        .setCode("1500")
                        .setEntity("GoFin")
                        .setCause("Unknown")
                        .build())
                .build();

        boolean result = evaluator.evaluate(genericResponse);

        Assertions.assertFalse(result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenCelValidationFailed() {
        forCELExpression("GenericResponse.nonExistField == true");
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenPayloadIsNotRecognizedByDescriptor() {
        TestMessage unregisteredPayload = TestMessage.newBuilder()
                .setOrderUrl("url")
                .build();

        grpcPayloadEvaluator.evaluate(unregisteredPayload);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIllegalArgumentExceptionWhenCelExpressionContainsUnregisteredMacro() {
        String expressionWithUnregisteredMacro = "GenericResponse.errors.nonStandardMacro(e, e.code == \"400\")";

        forCELExpression(expressionWithUnregisteredMacro);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowOperationNotSupportedExceptionWhenCelExpressionResultIsNotBoolean() {
        forCELExpression("GenericResponse.errors");
    }

    private static PayloadEvaluator<Message> forCELExpression(String celExpression) {
        return new GrpcResponseCelPayloadEvaluator(GenericResponse.getDescriptor(), celExpression);
    }

}
