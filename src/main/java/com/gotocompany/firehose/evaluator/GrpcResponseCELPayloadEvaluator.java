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

@Slf4j
public class GrpcResponseCELPayloadEvaluator implements PayloadEvaluator<Message>{

    private CelRuntime.Program celProgram;

    public GrpcResponseCELPayloadEvaluator(Descriptors.Descriptor descriptor, String celExpression) {
        buildCelEnvironment(descriptor, celExpression);
    }


    @Override
    public boolean evaluate(Message payload) {
        try {
            Map<String, Object> args = new HashMap<>();
            args.put(payload.getDescriptorForType().getFullName(), payload);
            return (boolean) celProgram.eval(args);
        } catch (CelEvaluationException e) {
            throw new DeserializerException("Failed to evaluate payload", e);
        }
    }

    private void buildCelEnvironment(Descriptors.Descriptor descriptor, String celExpression)  {
        try {
            CelCompiler celCompiler = CelCompilerFactory.standardCelCompilerBuilder()
                    .setStandardMacros(CelStandardMacro.EXISTS)
                    .addVar(descriptor.getFullName(), StructTypeReference.create(descriptor.getFullName()))
                    .addMessageTypes(descriptor)
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
