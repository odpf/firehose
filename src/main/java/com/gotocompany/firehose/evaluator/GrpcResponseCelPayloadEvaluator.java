package com.gotocompany.firehose.evaluator;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.gotocompany.firehose.exception.DeserializerException;
import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationException;
import dev.cel.common.types.StructTypeReference;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelEvaluationException;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

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
     * @param descriptor the descriptor of the gRPC message
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
     * @throws DeserializerException if the evaluation fails
     */
    @Override
    public boolean evaluate(Message payload) {
        if (!descriptor.getFullName().equals(payload.getDescriptorForType().getFullName())) {
            throw new IllegalArgumentException("Payload does not match descriptor");
        }
        try {
            Map<String, Object> args = new HashMap<>();
            args.put(payload.getDescriptorForType().getFullName(), payload);
            return (boolean) celProgram.eval(args);
        } catch (CelEvaluationException e) {
            throw new DeserializerException("Failed to evaluate payload", e);
        }
    }

    /**
     * Builds the CEL environment required to evaluate the CEL expression.
     *
     * @param celExpression the CEL expression to evaluate against the message
     * @throws IllegalArgumentException if the CEL expression is invalid or if the evaluator cannot be constructed
     */
    private void buildCelEnvironment(String celExpression)  {
        try {
            CelCompiler celCompiler = CelCompilerFactory.standardCelCompilerBuilder()
                    .setStandardMacros(CelStandardMacro.EXISTS, CelStandardMacro.EXISTS_ONE, CelStandardMacro.HAS)
                    .addVar(this.descriptor.getFullName(), StructTypeReference.create(this.descriptor.getFullName()))
                    .addMessageTypes(this.descriptor)
                    .build();
            CelRuntime celRuntime = CelRuntimeFactory.standardCelRuntimeBuilder()
                    .build();
            CelAbstractSyntaxTree celAbstractSyntaxTree = celCompiler.compile(celExpression)
                    .getAst();
            this.celProgram = celRuntime.createProgram(celAbstractSyntaxTree);
        } catch (CelValidationException e) {
            throw new IllegalArgumentException("Invalid CEL expression: " + celExpression, e);
        } catch (CelEvaluationException e) {
            throw new IllegalArgumentException("Failed to construct CEL evaluator: " + e.getMessage(), e);
        }
    }

}
