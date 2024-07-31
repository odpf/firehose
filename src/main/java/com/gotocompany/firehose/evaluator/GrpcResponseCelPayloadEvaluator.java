package com.gotocompany.firehose.evaluator;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.firehose.utils.CelUtils;
import dev.cel.common.types.CelKind;
import dev.cel.compiler.CelCompiler;
import dev.cel.runtime.CelRuntime;
import lombok.extern.slf4j.Slf4j;

/**
 * Implementation of PayloadEvaluator that evaluates gRPC responses using CEL (Common Expression Language).
 */
@Slf4j
public class GrpcResponseCelPayloadEvaluator implements PayloadEvaluator<Message> {

    private final Descriptors.Descriptor descriptor;
    private CelRuntime.Program celProgram;

    /**
     * Constructs a GrpcResponseCelPayloadEvaluator with the specified descriptor and CEL expression.
     *
     * @param descriptor    the descriptor of the gRPC message
     * @param celExpression the CEL expression to evaluate against the message
     */
    public GrpcResponseCelPayloadEvaluator(Descriptors.Descriptor descriptor, String celExpression) {
        this.descriptor = descriptor;
        buildCelEnvironment(celExpression);
    }

    /**
     * Evaluates the given gRPC message payload using the CEL program.
     *
     * @param payload the gRPC message to be evaluated
     * @return true if the payload passes the evaluation, false otherwise
     */
    @Override
    public boolean evaluate(Message payload) {
        if (!descriptor.getFullName().equals(payload.getDescriptorForType().getFullName())) {
            throw new IllegalArgumentException(String.format("Payload %s does not match descriptor %s",
                    payload.getDescriptorForType().getFullName(), descriptor.getFullName()));
        }
        return (boolean) CelUtils.evaluate(this.celProgram, payload);
    }

    /**
     * Builds the CEL environment required to evaluate the CEL expression.
     *
     * @param celExpression the CEL expression to evaluate against the message
     * @throws IllegalArgumentException if the CEL expression is invalid or if the evaluator cannot be constructed
     */
    private void buildCelEnvironment(String celExpression) {
        CelCompiler celCompiler = CelUtils.initializeCelCompiler(this.descriptor);
        CelRuntime celRuntime = CelUtils.initializeCelRuntime();
        this.celProgram = CelUtils.initializeCelProgram(celExpression, celRuntime, celCompiler,
                celType -> celType.kind().equals(CelKind.BOOL));
    }

}
